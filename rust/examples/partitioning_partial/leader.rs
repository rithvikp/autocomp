use frankenpaxos::automicrobenchmarks_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::tokio_stream::wrappers::IntervalStream;
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
use tokio::time::{interval_at, Duration, Instant};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {}

fn decrypt_and_deserialize(msg: BytesMut) -> (i64, u32, Rc<Vec<u8>>) {
    let s =
        automicrobenchmarks_proto::ServerInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();
    return (s.id, s.ballot, Rc::new(s.payload));
}

fn encrypt_and_serialize(id: i64, ballot: u32, payload: Rc<Vec<u8>>) -> bytes::Bytes {
    let out = automicrobenchmarks_proto::ClientInbound {
        request: Some(
            automicrobenchmarks_proto::client_inbound::Request::ClientReply(
                automicrobenchmarks_proto::ClientReply {
                    id,
                    ballot: Some(ballot),
                    payload: Some(payload.as_ref().clone()),
                },
            ),
        ),
    };
    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

pub async fn run(_cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
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

    let ballot_to_replica_sink = ports
        .remove("send_to$replicas$1")
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

    let frequency = 5;
    let start = Instant::now() + Duration::from_secs(frequency);
    let periodic_source = IntervalStream::new(interval_at(start, Duration::from_secs(frequency)));

    let df = datalog!(
        r#"
        .input replicas `repeat_iter(peers.clone()) -> map(|p| (p,))`
.input numReplicas `repeat_iter([(num_replicas,),])`
.input periodic `source_stream(periodic_source) -> map(|_| ())`
.input startBallot `repeat_iter([(0 as u32,),])`

.async ballotToReplica `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(ballot_to_replica_sink)` `null::<(u32,)>()`
.async voteToReplica `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(to_replica_sink)` `null::<(u32,i64,u32,Rc<Vec<u8>>)>()`
.async voteFromReplica `null::<(u32,u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(from_replica_source) -> map(|v| deserialize_from_bytes::<(u32,u32,i64,u32,Rc<Vec<u8>>)>(v.unwrap().1).unwrap())`

.async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> map(|x| {let input = x.unwrap(); let v = decrypt_and_deserialize(input.1); (input.0, v.0, v.1, v.2,)})`
.async clientOut `map(|(node_id, (id, ballot, payload,))| (node_id, encrypt_and_serialize(id, ballot, payload))) -> dest_sink(client_send)` `null::<(i64,u32,Rc<Vec<u8>>)>()`

# Automatically update epoch.
ballots(b) :- startBallot(b)
maxBallot(max(b)) :- ballots(b)
newBallot(b+1) :- maxBallot(b), periodic()
ballots(b) :+ maxBallot(b), !newBallot(_)
ballots(b) :+ !maxBallot(_), newBallot(b)
ballotToReplica@addr(b) :~ newBallot(b), replicas(addr)

voteToReplica@addr(client, id, b, v) :~ clientIn(client, id, b, v), replicas(addr)
        
allVotes(l, client, id, b, v) :- voteFromReplica(l, client, id, b, v)
allVotes(l, client, id, b, v) :+ allVotes(l, client, id, b, v), !committed(client, id, _, _)
voteCounts(count(l), client, id) :- allVotes(l, client, id, b, v)
committed(client, id, b, v) :- voteCounts(n, client, id), numReplicas(n), allVotes(l, client, id, b, v)
clientOut@client(id, b, v) :~ committed(client, id, b, v)
        "#
    );

    launch_flow(df).await
}
