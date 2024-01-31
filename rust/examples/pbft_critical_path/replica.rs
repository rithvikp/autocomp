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
pub struct ReplicaArgs {
    #[clap(long = "replica.index")]
    index: Option<u32>,
    #[clap(long = "replica.f")]
    f: Option<u32>,
}

// TODO: Create a digest of the command
fn deserialize(msg: BytesMut) -> Option<(Rc<Vec<u8>>,)> {
    if msg.len() == 0 {
        return None;
    }
    let s = multipaxos_proto::LeaderInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();

    match s.request.unwrap() {
        multipaxos_proto::leader_inbound::Request::ClientRequest(r) => {
            let out = multipaxos_proto::CommandBatchOrNoop {
                value: Some(
                    multipaxos_proto::command_batch_or_noop::Value::CommandBatch(
                        multipaxos_proto::CommandBatch {
                            command: vec![r.command],
                        },
                    ),
                ),
            };
            let mut buf = Vec::new();
            out.encode(&mut buf).unwrap();
            return Some((Rc::new(buf),));
        }
        _ => panic!("Unexpected message from the client"),
    }
}

// TODO sign output to client?
fn serialize(payload: Rc<Vec<u8>>, slot: u32) -> bytes::Bytes {
    let command =
        multipaxos_proto::CommandBatchOrNoop::decode(&mut Cursor::new(payload.as_ref())).unwrap();

    let out = multipaxos_proto::ReplicaInbound {
        request: Some(multipaxos_proto::replica_inbound::Request::Chosen(
            multipaxos_proto::Chosen {
                slot: i32::try_from(slot).unwrap() - 1, // Dedalus starts at slot 1
                command_batch_or_noop: command,
            },
        )),
    };

    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

// Note: This key is just used for testing and it's ok if it's public on GitHub.
let mac_key = b"75a651b30273ba7ccc9d314aab6d6544";
// TODO: We technically need to create an authenticator of MACs (one sig for each replica)

// Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <o = command operation>, sig(c) = signature of o).
fn create_pre_prepare(slot: u32, digest: Rc<Vec<u8>>, command: Rc<Vec<u8>>, sigc: Rc<Vec<u8>>) -> bytes::Bytes {
    // TODO: let sigp = sign(n, d)
}

// TODO: Unwrap and verify pre-prepare
fn unwrap_pre_prepare(bytes::Bytes) -> Optional<bytes::Bytes> {

}

// TODO: 
fn create_prepare() -> bytes::Bytes {

}

// TODO
fn unwrap_prepare() -> Optional<bytes::Bytes> {

}

// TODO
fn create_commit() -> bytes::Bytes {

}

// TODO
fn unwrap_commit() -> Optional<bytes::Bytes> {

}

// Need to provide: clients, replicas, and smr (corresponding state machine replica)
pub async fn run(cfg: ReplicaArgs, mut ports: HashMap<String, ServerOrBound>) {
    let client_requests = prometheus::register_counter!("pbft_requests_total", "help").unwrap();

    // Client setup
    let client_recv = ports
        .remove("receive_from$clients$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    // Replica setup
    let pre_prepare_to_replica_port = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let replicas = pre_prepare_to_replica_port.keys.clone();
    let pre_prepare_to_replica_sink = pre_prepare_to_replica_port.into_sink();

    let pre_prepare_from_replica_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let prepare_to_replica_sink = ports
        .remove("send_to$replicas$1")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let prepare_from_replica_source = ports
        .remove("receive_from$replicas$1")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let commit_to_smr_port = ports
        .remove("send_to$smr$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let smr = commit_to_smr_port.keys.clone();
    let commit_to_smr_sink = commit_to_smr_port.into_sink();

    let commit_from_replica_source = ports
        .remove("receive_from$replicas$2")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let my_id = cfg.index.unwrap();
    let f = cfg.f.unwrap();

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input leaderId `repeat_iter([(0,),])`
.input replicas `repeat_iter(replicas.clone()) -> map(|p| (p,))`
.input smr `repeat_iter(smr.clone()) -> map(|p| (p,))`
.input quorum `repeat_iter([(2*f+1,),])`
.input fullQuorum `repeat_iter([(3*f+1,),])`

# IDB

# Request (<o = command operation, t = timestamp, c = client>, sig(c) = signature of (o,t,c)).
# Simplified request (<o = command operation>, sig(c) = signature of o, d = digest of o).
.async clientIn `null::<(Rc<Vec<u8>>,)>()` `source_stream(client_recv) -> filter_map(|x: Result<(u32, BytesMut,), _>| (deserialize(x.unwrap().1)))`

.output clientStdout `for_each(|(_,slot):(Rc<Vec<u8>>,u32)| println!("committed {:?}", slot))`

# Reply (v = view, t = timestamp, c = client, i = id of self, r = result of execution, sig(i) = signature of (v,t,c,i,r)).
# Simplified reply (o = command operation, n = slot). The difference in message content is because we're sending this to the state machine, not the client that sent the request.
.async clientOut `map(|(node_id, (payload, slot,))| (node_id, serialize(payload, slot))) -> dest_sink(replica_send)` `null::<(Rc<Vec<u8>>, u32,)>()`

.input startSlot `repeat_iter([(0 as u32,),])`

# Pre-prepare (v = view, n = slot, d = hash digest of m, sig(p) = signature of (v,n,d), m = <o = command operation, t = timestamp, c = client>, sig(c) = signature of (o,t,c)).
# Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <o = command operation>, sig(c) = signature of o).
.async prePrepareOut `map(|(node_id, (slot, digest, command, sigc))| (node_id, create_pre_prepare(slot, digest, command, sigc))) -> dest_sink(pre_prepare_to_replica_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`
.async prePrepareIn `null::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>)>()` `source_stream(pre_prepare_from_replica_source) -> filter_map(|x: Result<(u32, BytesMut, BytesMut, BytesMut, BytesMut), _>| (unwrap_pre_prepare(deserialize(x.unwrap().1))))`

# Prepare (v = view, n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (v,n,d,i)).
# Simplified prepare (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
.async prepareOut `map(|(node_id, (slot, digest, i))| (node_id, create_prepare(slot, digest, i))) -> dest_sink(prepare_to_replica_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`
.async prepareIn `null::<(u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)>()` `source_stream(prepare_from_replica_source) -> filter_map(|x: Result<(u32, BytesMut, u32, BytesMut), _>| (unwrap_prepare(deserialize(x.unwrap().1))))`

# Commit (v = view, n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (v,n,d,i)).
# Simplified commit (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
.async commitOut `map(|(node_id, (slot, digest, i))| (node_id, create_commit(slot, digest, i))) -> dest_sink(commit_to_replica_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`
.async commitIn `null::<(u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)>()` `source_stream(commit_from_replica_source) -> filter_map(|x: Result<(u32, BytesMut, u32, BytesMut), _>| (unwrap_commit(deserialize(x.unwrap().1))))`

######################## end relation definitions

.persist prePrepareLog
.persist perpareLog
.persist commitSent
.persist commitLog
.persist replySent

IsLeader() :- leaderId(i), id(i)
slots(s) :- startSlot(s)
nextSlot(max(s)) :- slots(s)

######################## pre-prepare
IndexedPayloads(command, sigc, digest, index()) :~ clientIn(command, sigc, digest), IsLeader()
prePrepareOut@r((slot+offset), digest, command, sigc) :~ IndexedPayloads(command, sigc, digest, offset), nextSlot(slot), replicas(r)

# Increment slot
NumPayloads(max(offset) + 1) :- IndexedPayloads(_, _, _, offset)
slots(s + num) :+ nextSlot(s), NumPayloads(num)
######################## end pre-prepare

######################## prepare
prePrepareLog(slot, digest, sigp, command, sigc) :- prePrepareIn(slot, digest, sigp, command, sigc)
prepareOut@r(slot, digest, i) :~ prePrepareIn(slot, digest, sigp, command, sigc), replicas(r), id(i)
######################## end prepare

######################## commit
prepareLog(slot, digest, sigp, i) :- prepareIn(slot, digest, i, sigp)
numPrepares(slot, digest, count(i)) :- prepareLog(slot, digest, _, i), !commitSent(slot, digest)
shouldSendCommit(slot, digest) :- numPrepares(slot, digest, count), quorum(q), (c >= q), !commitSent(slot, digest)
commitOut@r(slot, digest, i) :~ shouldSendCommit(slot, digest), replicas(r), id(i)

# Store own commit locally
commitLog(slot, digest, i) :- shouldSendCommit(slot, digest)
commitSent(slot, digest) :+ shouldSendCommit(slot, digest)
######################## end commit

######################## reply
commitLog(slot, digest, i) :- commitIn(slot, digest, i, sigi) # Note: sigi is not stored, because the commit log is not used during view-change so we don't need to prove to others that this is the message we got.
numCommits(slot, digest, count(i)) :- commitLog(slot, digest, i), !replySent(slot, digest)
shouldSendReply(slot, digest, command) :- numCommits(slot, digest, count), quorum(q), (c >= q), !replySent(slot, digest)
clientOut@s(command, slot) :~ shouldSendReply(slot, _, command), smr(s)

replySent(slot, digest) :+ shouldSendReply(slot, digest, _)
######################## end reply
    "#
    );

    launch_flow(df).await;
}
