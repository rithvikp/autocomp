use aes_gcm::{aead::Aead, aead::KeyInit, Aes128Gcm, Nonce};
use frankenpaxos::automicrobenchmarks_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::rc::Rc;
use std::{collections::HashMap, io::Cursor};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {}

fn decrypt_and_deserialize(msg: BytesMut, cipher: &Aes128Gcm) -> (i64, u32, Rc<Vec<u8>>) {
    let s =
        automicrobenchmarks_proto::ServerInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();
    let iv = Nonce::from_slice(b"unique nonce");
    let mut decrypted_payload = s.payload.clone();
    for _ in 0..100 {
        decrypted_payload = cipher.decrypt(iv, decrypted_payload.as_ref()).unwrap();
    }
    return (s.id, s.ballot, Rc::new(decrypted_payload));
}

fn encrypt_and_serialize(
    id: i64,
    ballot: u32,
    payload: Rc<Vec<u8>>,
    cipher: &Aes128Gcm,
) -> bytes::Bytes {
    let iv = Nonce::from_slice(b"unique nonce");
    let mut encrypted_payload = payload.as_ref().clone();
    for _ in 0..100 {
        encrypted_payload = cipher.encrypt(iv, encrypted_payload.as_slice()).unwrap();
    }

    let out = automicrobenchmarks_proto::ClientInbound {
        request: Some(
            automicrobenchmarks_proto::client_inbound::Request::ClientReply(
                automicrobenchmarks_proto::ClientReply {
                    id,
                    ballot: Some(ballot),
                    payload: Some(encrypted_payload),
                },
            ),
        ),
    };
    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

pub async fn run(_cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let key_bytes = hex::decode("bfeed277024d4700c7edf24127858917").unwrap();
    let cipher1 = Aes128Gcm::new_from_slice(key_bytes.as_slice()).unwrap();
    let cipher2 = Aes128Gcm::new_from_slice(key_bytes.as_slice()).unwrap();

    // Client setup
    let client_recv = ports
        .remove("receive_from$clients$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let client_send = ports
        .remove("send_to$clients$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    // Replica setup
    let to_replica_port = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let peers = to_replica_port.keys.clone();
    let num_replicas = peers.len();
    let to_replica_sink = to_replica_port.into_sink();

    let from_replica_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let df = datalog!(
        r#"
        .input replicas `repeat_iter(peers.clone()) -> map(|p| (p,))`
        .input numReplicas `repeat_iter([(num_replicas,),])`
        
        .async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> map(|x| {let input = x.unwrap(); let v = decrypt_and_deserialize(input.1, &cipher1); (input.0, v.0, v.1, v.2,)})`
        .async clientOut `map(|(node_id, (id, ballot, payload,))| (node_id, encrypt_and_serialize(id, ballot, payload, &cipher2))) -> dest_sink(client_send)` `null::<(i64,u32,Rc<Vec<u8>>)>()`
        .async voteToReplica `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(to_replica_sink)` `null::<(u32,i64,u32,Rc<Vec<u8>>)>()`
        .async voteFromReplica `null::<(u32,u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(from_replica_source) -> map(|v| deserialize_from_bytes::<(u32,u32,i64,u32,Rc<Vec<u8>>)>(v.unwrap().1).unwrap())`
        
        # ballot is guaranteed to either be empty or contain the 1 current ballot
        newBallots(b) :- clientIn(client, id, b, v)
        newBallots(b) :- ballot(b)
        MaxNewBallot(max(b)) :- newBallots(b)
        ballot(b) :+ MaxNewBallot(b)
        
        voteToReplica@addr(client, id, b, v) :~ clientIn(client, id, b, v), replicas(addr)
        # Record ballot at the time clientIn arrived
        writeTimeBallot(client, id, b) :- clientIn(client, id, _, _), ballot(b)
        writeTimeBallot(client, id, 0) :- clientIn(client, id, _, _), !ballot(b)
        writeTimeBallot(client, id, b) :+ writeTimeBallot(client, id, b), !committed(client, id, _, _)
        
        allVotes(l, client, id, b, v) :- voteFromReplica(l, client, id, b, v)
        allVotes(l, client, id, b, v) :+ allVotes(l, client, id, b, v), !committed(client, id, _, _)
        voteCounts(count(l), client, id) :- allVotes(l, client, id, b, v)
        committed(client, id, b, v) :- voteCounts(n, client, id), numReplicas(n), allVotes(l, client, id, b, v) 
        # Attach ballot at the time clientIn arrived to the output
        clientOut@client(id, b, v) :~ committed(client, id, _, v), writeTimeBallot(client, id, b)
        "#
    );

    launch_flow(df).await
}
