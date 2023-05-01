use frankenpaxos::voting_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::tokio_stream::wrappers::IntervalStream;
use hydroflow::{
    tokio_util::codec::{Framed, LinesCodec},
    util::{
        cli::{
            launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
            ConnectedTagged, ServerOrBound,
        },
        deserialize_from_bytes, serialize_to_bytes,
    },
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::{collections::HashMap, io::Cursor, path::Path, rc::Rc};
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::time::{interval_at, Duration, Instant};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {
    #[clap(long, default_value = "1")]
    flush_every_n: usize,
}

// Returns (client_id, request_id, reply)
fn deserialize(
    (client_id, msg): (u32, BytesMut),
    client_requests: &prometheus::Counter,
) -> (u32, i64, Rc<Vec<u8>>) {
    let s = voting_proto::LeaderInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();

    match s.request.unwrap() {
        voting_proto::leader_inbound::Request::ClientRequest(r) => {
            client_requests.inc();
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
    let client_requests = prometheus::register_counter!("twopc_requests_total", "help").unwrap();

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
    let vote_to_participant_port = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let peers = vote_to_participant_port.keys.clone();
    let num_replicas = peers.len();
    let peers_formatted = format!("{:?}", peers);
    // println!("peers: {:?}", peers);
    let vote_to_participant_sink = vote_to_participant_port.into_sink();

    let vote_from_participant_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let commit_to_participant_sink = ports
        .remove("send_to$replicas$1")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let ack_from_participant_source = ports
        .remove("receive_from$replicas$1")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    // Logging
    let log_directory = "twopc_out";
    let coordinator_log = "coordinator.txt";
    fs::create_dir_all(log_directory.clone()).await.unwrap();
    let path = Path::new(".").join(log_directory).join(coordinator_log);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .unwrap();
    file.set_len(0).await.unwrap();
    let file_sink = Framed::new(file, LinesCodec::new());

    let frequency = 1;
    let start = Instant::now() + Duration::from_secs(frequency);
    let periodic_source = IntervalStream::new(interval_at(start, Duration::from_secs(frequency)));

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input participants `repeat_iter(peers.clone()) -> map(|p| (p,))`
.input numParticipants `repeat_iter([(num_replicas,),])`

.async clientIn `null::<(u32,i64,Rc<Vec<u8>>,)>()` `source_stream(client_recv) -> map(|x| deserialize(x.unwrap(), &client_requests))`
.async clientOut `map(|(node_id, (v,)):(u32,(Rc<Vec<u8>>,))| (node_id, serialize(v))) -> dest_sink(client_send)` `null::<(Rc<Vec<u8>>,)>()`

.input periodic `source_stream(periodic_source) -> map(|_| ())`

.async voteToParticipant `map(|(node_id, v):(u32,(u32, i64, Rc<Vec<u8>>))| (node_id, serialize_to_bytes(v))) -> dest_sink(vote_to_participant_sink)` `null::<(u32,i64,Rc<Vec<u8>>,)>()`
.async voteFromParticipant `null::<(u32,i64,Rc<Vec<u8>>,u32,)>()` `source_stream(vote_from_participant_source) -> map(|v| deserialize_from_bytes::<(u32, i64, Rc<Vec<u8>>,u32,)>(v.unwrap().1).unwrap())`
.async commitToParticipant `map(|(node_id, v):(u32,(u32,i64,Rc<Vec<u8>>))| (node_id, serialize_to_bytes(v))) -> dest_sink(commit_to_participant_sink)` `null::<(u32,i64,Rc<Vec<u8>>,)>()`
.async ackFromParticipant `null::<(u32,i64,Rc<Vec<u8>>,u32,)>()` `source_stream(ack_from_participant_source) -> map(|v| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>,u32,)>(v.unwrap().1).unwrap())`

.output logCommit `map(|(client, id, p):(u32,i64,Rc<Vec<u8>>)| format!("client {:?}, id: {:?}, p: {:?}, participants: {:?}", client, id, p, peers_formatted)) -> dest_sink(file_sink)`
######################## end relation definitions

# Phase 1a
voteToParticipant@addr(client, id, p) :~ participants(addr), clientIn(client, id, p)

# Phase 1b, Phase 2a
AllVotes(client, id, payload, src) :+ AllVotes(client, id, payload, src), !committed(client, id, _)
AllVotes(client, id, payload, src) :- voteFromParticipant(client, id, payload, src)

NumYesVotes(client, id, count(src)) :- AllVotes(client, id, payload, src)
committed(client, id, payload) :- NumYesVotes(client, id, num), AllVotes(client, id, payload, src), numParticipants(num) 
logCommit(client, id, payload) :- committed(client, id, payload)
logCommitComplete(client, id, payload) :+ committed(client, id, payload)
commitToParticipant@addr(client, id, payload) :~ logCommitComplete(client, id, payload), participants(addr)

# Phase 2b
AllAcks(client, id, payload, src) :+ AllAcks(client, id, payload, src), !completed(client, id, _)
AllAcks(client, id, payload, src) :- ackFromParticipant(client, id, payload, src)

NumAcks(client, id, count(src)) :- AllAcks(client, id, payload, src)
completed(client, id, payload) :- NumAcks(client, id, num), AllAcks(client, id, payload, src), numParticipants(num)
logCommit(client, id, payload) :- completed(client, id, payload)
clientOut@client(v) :~ completed(client, id, v)
    "#
    );

    launch_flow(df).await;
}
