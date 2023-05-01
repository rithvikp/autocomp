use aes_gcm::{aead::Aead, aead::KeyInit, Aes128Gcm, Nonce};
use frankenpaxos::automicrobenchmarks_proto;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct CollectorArgs {
    #[clap(long = "collector.num-replicas")]
    collector_num_replicas: Option<u32>,
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

pub async fn run(cfg: CollectorArgs, mut ports: HashMap<String, ServerOrBound>) {
    let key_bytes = hex::decode("bfeed277024d4700c7edf24127858917").unwrap();
    let cipher = Aes128Gcm::new_from_slice(key_bytes.as_slice()).unwrap();

    let from_replica_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let ballot_from_leader_source = ports
        .remove("receive_from$leaders$0")
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

    let num_replicas = cfg.collector_num_replicas.unwrap();

    let df = datalog!(
        r#"
        .input numReplicas `repeat_iter([(num_replicas,),])`

.async clientOut `map(|(node_id, (id, ballot, payload,))| (node_id, encrypt_and_serialize(id, ballot, payload, &cipher))) -> dest_sink(client_send)` `null::<(i64,u32,Rc<Vec<u8>>)>()`
.async voteFromReplica `null::<(u32,u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(from_replica_source) -> map(|v| deserialize_from_bytes::<(u32,u32,i64,u32,Rc<Vec<u8>>)>(v.unwrap().1).unwrap())`
.async ballotFromLeader `null::<(u32,i64,u32)>()` `source_stream(ballot_from_leader_source) -> map(|v| deserialize_from_bytes::<(u32,i64,u32)>(v.unwrap().1).unwrap())`


# Record ballot at the time clientIn arrived
writeTimeBallot(client, id, b) :- ballotFromLeader(client, id, b)
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
