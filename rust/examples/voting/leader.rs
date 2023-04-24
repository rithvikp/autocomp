use frankenpaxos::voting_proto;
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
use std::{collections::HashMap, io::Cursor, time::Duration};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {
    #[clap(long, default_value = "1")]
    flush_every_n: usize,
}

// Returns (client_id, request_id, reply)
fn deserialize((client_id, msg): (u32, BytesMut), vote_requests: &prometheus::Counter) -> (u32,i64,Rc<Vec<u8>>) {
    let s = voting_proto::LeaderInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();

    match s.request.unwrap() {
        voting_proto::leader_inbound::Request::ClientRequest(r) => {
            vote_requests.inc();
            let out = voting_proto::ClientReply {
                id: r.id,
                accepted: true,
                command: r.command,
            };
            let mut buf = Vec::new();
            out.encode(&mut buf).unwrap();
            return (client_id, r.id, Rc::new(buf));
        }
        _ => panic!("Unexpected message from the client"),
    }
}

fn serialize(payload: Rc<Vec<u8>>) -> bytes::Bytes {
    // TODO: Remove this copy if possible
    return bytes::Bytes::from(payload.as_ref().to_vec());
}

pub async fn run(cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let vote_requests = prometheus::register_counter!("voting_requests_total", "help").unwrap();

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
    // let to_replica_unbatched_sink = to_replica_port.into_sink();
    // let to_replica_sink = batched_sink(
    //     to_replica_unbatched_sink,
    //     cfg.flush_every_n,
    //     Duration::from_secs(10),
    // );

    let from_replica_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let df = datalog!(
        r#"
.async clientIn `null::<(u32,i64,Rc<Vec<u8>>,)>()` `source_stream(client_recv) -> map(|x| deserialize(x.unwrap(), &vote_requests))`
.async clientOut `map(|(node_id, (v,)):(u32,(Rc<Vec<u8>>,))| (node_id, serialize(v))) -> dest_sink(client_send)` `null::<(Rc<Vec<u8>>,)>()`

.input replicas `repeat_iter(peers.clone()) -> map(|p| (p,))`
.input numReplicas `repeat_iter([(num_replicas,),])`

.async voteToReplica `map(|(node_id, v):(u32,(u32,i64,Rc<Vec<u8>>,))| (node_id, serialize_to_bytes(v))) -> dest_sink(to_replica_sink)` `null::<(u32,i64,Rc<Vec<u8>>,)>()`
.async voteFromReplica `null::<(u32,u32,i64,Rc<Vec<u8>>,)>()` `source_stream(from_replica_source) -> map(|v| deserialize_from_bytes::<(u32,u32,i64,Rc<Vec<u8>>,)>(v.unwrap().1).unwrap())`

voteToReplica@addr(client, id, v) :~ clientIn(client, id, v), replicas(addr)
allVotes(l, client, id, v) :- voteFromReplica(l, client, id, v)
allVotes(l, client, id, v) :+ allVotes(l, client, id, v), !committed(client, id, _)
voteCounts(count(l), client, id) :- allVotes(l, client, id, v)
committed(client, id, v) :- voteCounts(n, client, id), allVotes(l, client, id, v), numReplicas(n)

clientOut@client(v) :~ committed(client, id, v)
    "#
    );

    launch_flow(df).await;
}
