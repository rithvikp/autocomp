use frankenpaxos::voting_proto;
use hydroflow::util::{
    batched_sink,
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::{collections::HashMap, io::Cursor, time::Duration};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {
    #[clap(long, default_value = "1")]
    flush_every_n: usize,
}

fn deserialize(msg: impl AsRef<[u8]>, vote_requests: &prometheus::Counter) -> (i64,) {
    let s = voting_proto::LeaderInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();

    match s.request.unwrap() {
        voting_proto::leader_inbound::Request::ClientRequest(r) => {
            vote_requests.inc();
            return (r.id,);
        }
        _ => panic!("Unexpected message from the client"),
    }
}

fn serialize(v: (i64,)) -> bytes::Bytes {
    let s = frankenpaxos::voting_proto::ClientReply {
        id: v.0,
        accepted: true,
    };
    let mut buf = Vec::new();
    s.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
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
    let to_replica_unbatched_sink = to_replica_port.into_sink();
    let to_replica_sink = batched_sink(
        to_replica_unbatched_sink,
        cfg.flush_every_n,
        Duration::from_secs(10),
    );

    let from_replica_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let df = datalog!(
        r#"
.async clientIn `null::<(i64,)>()` `source_stream(client_recv) -> map(|x| deserialize(x.unwrap().1, &vote_requests))`
.async clientOut `map(|(node_id, id)| (node_id, serialize(id))) -> dest_sink(client_send)` `null::<(u32, i64,)>()`

.output stdout `for_each(|s:(i64,)| println!("committed: {:?}", s))`
.input replicas `repeat_iter(peers.clone()) -> map(|p| (p,))`

.async voteToReplica `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(to_replica_sink)` `null::<(i64,)>()`
.async voteFromReplica `null::<(u32,i64,)>()` `source_stream(from_replica_source) -> map(|v| deserialize_from_bytes::<(u32,i64,)>(v.unwrap().1).unwrap())`

voteToReplica@addr(v) :~ clientIn(v), replicas(addr)
allVotes(l, v) :- voteFromReplica(l, v)
allVotes(l, v) :+ allVotes(l, v), !committed(v)
voteCounts(count(l), v) :- allVotes(l, v)
numReplicas(count(addr)) :- replicas(addr)
committed(v) :- voteCounts(n, v), numReplicas(n)
// stdout(v) :- committed(v)

clientOut@0(v) :~ committed(v)
    "#
    );

    launch_flow(df).await;
}
