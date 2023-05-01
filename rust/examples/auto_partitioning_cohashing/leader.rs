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
use rand::rngs::ThreadRng;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePublicKey;
use rsa::{Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey};
use std::rc::Rc;
use std::{collections::HashMap, io::Cursor};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {
    #[clap(long = "leader.num-replica-partitions")]
    leader_num_replica_partitions: Option<i64>,

    #[clap(long = "leader.num-replicas")]
    leader_num_replicas: Option<i64>,
}

fn decrypt_and_deserialize(msg: BytesMut) -> (i64, u32, Rc<Vec<u8>>) {
    let s =
        automicrobenchmarks_proto::ServerInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();
    return (s.id, s.ballot, Rc::new(s.payload));
}

fn encrypt_and_serialize(id: i64, payload: Rc<Vec<u8>>) -> bytes::Bytes {
    let out = automicrobenchmarks_proto::ClientInbound {
        request: Some(
            automicrobenchmarks_proto::client_inbound::Request::ClientReply(
                automicrobenchmarks_proto::ClientReply {
                    id,
                    ballot: None,
                    payload: Some(payload.as_ref().clone()),
                },
            ),
        ),
    };
    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

pub async fn run(cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
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
    let to_replica_sink = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let from_replica_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let num_replica_partitions = cfg.leader_num_replica_partitions.unwrap();
    let num_replicas = cfg.leader_num_replicas.unwrap();
    let mut replica_start_ids = Vec::<i64>::new();
    for i in 0..num_replicas {
        replica_start_ids.push(i * num_replica_partitions);
    }

    let df = datalog!(
        r#"
        .input numReplicaPartitions `repeat_iter([(num_replica_partitions,),])`
        .input replicaStartIDs `repeat_iter(replica_start_ids.clone()) -> map(|p| (p,))`
        .input numReplicas `repeat_iter([(num_replicas,),])`
        
        .async voteToReplica `map(|(node_id, v)| (u32::try_from(node_id).unwrap(), serialize_to_bytes(v))) -> dest_sink(to_replica_sink)` `null::<(u32,i64,u32,Rc<Vec<u8>>)>()`
        .async voteFromReplica `null::<(u32,u32,i64,Rc<Vec<u8>>)>()` `source_stream(from_replica_source) -> map(|v| deserialize_from_bytes::<(u32,u32,i64,Rc<Vec<u8>>)>(v.unwrap().1).unwrap())`
        
        .async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> map(|x| {let input = x.unwrap(); let v = decrypt_and_deserialize(input.1); (input.0, v.0, v.2,)})`
        .async clientOut `map(|(node_id, (id, payload,))| (node_id, encrypt_and_serialize(id, payload))) -> dest_sink(client_send)` `null::<(i64,Rc<Vec<u8>>)>()`
        
        
        voteToReplica@(start+(id%n))(client, id, v) :~ clientIn(client, id, v), replicaStartIDs(start), numReplicaPartitions(n)
                
        allVotes(l, client, id, v) :- voteFromReplica(l, client, id, v)
        allVotes(l, client, id, v) :+ allVotes(l, client, id, v), !committed(client, id, _)
        voteCounts(count(l), client, id) :- allVotes(l, client, id, v)
        committed(client, id, v) :- voteCounts(n, client, id), numReplicas(n), allVotes(l, client, id, v)
        clientOut@client(id, v) :~ committed(client, id, v)
        "#
    );

    launch_flow(df).await
}
