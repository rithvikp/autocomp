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
use sha2::{Sha256, Digest};
use hmac::{Hmac, Mac};

#[derive(clap::Args, Debug)]
pub struct PBFTReplicaArgs {
    #[clap(long = "pbft_replica.index")]
    index: Option<u32>,
    #[clap(long = "pbft_replica.f")]
    f: Option<u32>,
}

// Output: Command, sigc, digest
fn deserialize(msg: BytesMut) -> Option<(Rc<Vec<u8>>,Rc<Vec<u8>>,Rc<Vec<u8>>)> {
    if msg.len() == 0 {
        return None;
    }
    let s = multipaxos_proto::LeaderInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();

    match s.request.unwrap() {
        multipaxos_proto::leader_inbound::Request::ClientRequest(r) => {
            let command = vec![r.command];
            let sigc = vec![r.signature];
            let digest = vec![r.digest];

            // Verify the digest is from the command
            let mut hasher = Sha256::new();
            hasher.update(command);
            let result = hasher.finalize();
            if result != digest {
                panic!("Client digest {:?} didn't match expected digest {:?}", digest, result)
                // TODO: Change to return None after debugging, so Byzantine clients can't crash the PBFT replica
            }

            // Verify the signature is signed over the digest
            let client_key = b"clientPrimaryKey";
            let mut mac = Hmac::<Sha256>::new_from_slice(client_key).unwrap();
            mac.update(digest.as_slice());
            if mac.verify(sigc).is_err() {
                panic!("Client signature {:?} was incorrect", sigc)
                // TODO: Change to return None after debugging, so Byzantine clients can't crash the PBFT replica
            }

            Some((Rc::new(command), Rc::new(sigc), Rc::new(digest),))
        }
        _ => panic!("Unexpected message from the client"), // TODO: Change to return None after debugging, so Byzantine clients can't crash the PBFT replica
    }
}

// TODO sign output to SMR?
fn serialize(payload: Rc<Vec<u8>>, slot: u32) -> bytes::Bytes {
    let out = multipaxos_proto::ReplicaInbound {
        request: Some(multipaxos_proto::replica_inbound::Request::Chosen(
            multipaxos_proto::Chosen {
                slot: i32::try_from(slot).unwrap() - 1, // Dedalus starts at slot 1
                command_batch_or_noop: multipaxos_proto::CommandBatchOrNoop {
                    value: Some(
                        multipaxos_proto::command_batch_or_noop::Value::CommandBatch(
                            multipaxos_proto::CommandBatch {
                                command: payload.as_ref().clone(),
                            },
                        ),
                    ),
                }
            },
        )),
    };

    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

fn get_mac(replica_1_id: u32, replica_2_id: u32) -> Hmac<Sha256> {
    // Key scheme: If replica 1 is writing to replica 2, then the key is b"12". Lowest id first, so replica 2 writing to replica 1 uses the same key.
    let mut key;
    if replica_1_id < replica_2_id {
        key = replica_1_id * 10 + replica_2_id;
    } else {
        key = replica_2_id * 10 + replica_1_id;
    }
    Hmac::<Sha256>::new(key.to_be_bytes()).unwrap()
}

fn sign(digest: Rc<Vec<u8>>, replica_1_id: u32, replica_2_id: u32) -> bytes::Bytes {
    // Key scheme: If replica 1 is writing to replica 2, then the key is b"12". Lowest id first, so replica 2 writing to replica 1 uses the same key.
    let mut key;
    if replica_1_id < replica_2_id {
        key = replica_1_id * 10 + replica_2_id;
    } else {
        key = replica_2_id * 10 + replica_1_id;
    }
    // Note: we will consistently use big endian (be) between nodes
    let mut mac = Hmac::<Sha256>::new(key.to_be_bytes()).unwrap();
    mac.update(digest);
    mac.finalize().into_bytes()
}

// Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <o = command operation>, sig(c) = signature of o).
fn create_pre_prepare(slot: u32, digest: Rc<Vec<u8>>, command: Rc<Vec<u8>>, sigc: Rc<Vec<u8>>, receiver: i32) -> (u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>) {
    // sender = 0 because only the leader (id = 0) sends prePrepares
    let mut mac = get_mac(0, receiver);
    mac.update(slot);
    mac.update(digest.as_slice());
    let sigp = mac.finalize().into_bytes();
    (slot, digest, sigp, command, sigc)
}

fn unwrap_pre_prepare(msg: (u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>), receiver: i32) -> Optional<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>)> {
    let (slot, digest, sigp, command, sigc) = msg;
    let mut mac = get_mac(0, receiver);
    mac.update(slot);
    mac.update(digest.as_slice());
    if mac.verify(sigp).is_err() {
        panic!("PrePrepare leader signature {:?} was incorrect", sigp)
        // TODO: Change to return None after debugging, so a Byzantine primary can't crash the replica
    }
    // TODO: Verify the command signature sigc. Needs client to send authenticator instead of single sig?
    Some((slot, digest, sigp, command, sigc))
}

// Simplified prepare (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
fn create_prepare(slot: u32, digest: Rc<Vec<u8>>, sender: i32, receiver: i32) -> bytes::Bytes {
    let mut mac = get_mac(sender, receiver);
    mac.update(slot);
    mac.update(digest.as_slice());
    mac.update(sender);
    let sigi = mac.finalize().into_bytes();
    (slot, digest, sender, sigi)
}

fn unwrap_prepare(msg: (u32, Rc<Vec<u8>>, i32, Rc<Vec<u8>>), receiver: i32) -> Optional<bytes::Bytes> {
    let (slot, digest, sender, sigi) = msg;
    let mut mac = get_mac(sender, receiver);
    mac.update(slot);
    mac.update(digest.as_slice());
    mac.update(sender);
    if mac.verify(sigi).is_err() {
        panic!("Prepare signature {:?} from {:?} was incorrect", sigi, sender)
        // TODO: Change to return None after debugging, so another Byzantine replica can't crash this replica
    }
    Some((slot, digest, sender, sigi))
}

// Simplified commit (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
fn create_commit(sender: i32, receiver: i32) -> bytes::Bytes {
    let mut mac = get_mac(sender, receiver);
    mac.update(slot);
    mac.update(digest);
    mac.update(sender);
    let sigi = mac.finalize().into_bytes();
    (slot, digest, sender, sigi)
}

fn unwrap_commit(msg: (u32, Rc<Vec<u8>>, i32, Rc<Vec<u8>>), receiver: i32) -> Optional<bytes::Bytes> {
    let (slot, digest, sender, sigi) = msg;
    let mut mac = get_mac(sender, receiver);
    mac.update(slot);
    mac.update(digest.as_slice());
    mac.update(sender);
    if mac.verify(sigi).is_err() {
        panic!("Commit signature {:?} from {:?} was incorrect", sigi, sender)
        // TODO: Change to return None after debugging, so another Byzantine replica can't crash this replica
    }
    Some((slot, digest, sender, sigi))
}

// Need to provide: clients, replicas, and smr (corresponding state machine replica)
pub async fn run(cfg: PBFTReplicaArgs, mut ports: HashMap<String, ServerOrBound>) {
    let client_requests = prometheus::register_counter!("pbft_requests_total", "help").unwrap();

    // Client setup
    let client_recv = ports
        .remove("receive_from$clients$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    // Replica setup
    let pre_prepare_to_pbft_replica_port = ports
        .remove("send_to$pbft_replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let pbft_replicas = pre_prepare_to_pbft_replica_port.keys.clone();
    let pre_prepare_to_pbft_replica_sink = pre_prepare_to_pbft_replica_port.into_sink();

    let pre_prepare_from_pbft_replica_source = ports
        .remove("receive_from$pbft_replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let prepare_to_pbft_replica_sink = ports
        .remove("send_to$pbft_replicas$1")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let prepare_from_pbft_replica_source = ports
        .remove("receive_from$pbft_replicas$1")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let commit_to_pbft_replica_sink = ports
        .remove("send_to$pbft_replicas$2")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let commit_from_pbft_replica_source = ports
        .remove("receive_from$pbft_replicas$2")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let replica_port = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let replicas = replica_port.keys.clone();
    let replica_send = replica_port.into_sink();

    let my_id = cfg.index.unwrap();
    let f = cfg.f.unwrap();

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input leaderId `repeat_iter([(0,),])`
.input pbftReplicas `repeat_iter(pbft_replicas.clone()) -> map(|p| (p,))`
.input replica `repeat_iter(replica.clone()) -> map(|p| (p,))`
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
.async prePrepareOut `map(|(node_id, (slot, digest, command, sigc))| (node_id, create_pre_prepare(slot, digest, command, sigc, node_id))) -> dest_sink(pre_prepare_to_pbft_replica_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`
.async prePrepareIn `null::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>)>()` `source_stream(pre_prepare_from_pbft_replica_source) -> filter_map(|x: Result<(u32, BytesMut, BytesMut, BytesMut, BytesMut), _>| (unwrap_pre_prepare(deserialize(x.unwrap().1), my_id)))`

# Prepare (v = view, n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (v,n,d,i)).
# Simplified prepare (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
.async prepareOut `map(|(node_id, (slot, digest, i))| (node_id, create_prepare(slot, digest, i, node_id))) -> dest_sink(prepare_to_pbft_replica_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`
.async prepareIn `null::<(u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)>()` `source_stream(prepare_from_pbft_replica_source) -> filter_map(|x: Result<(u32, BytesMut, u32, BytesMut), _>| (unwrap_prepare(deserialize(x.unwrap().1), my_id)))`

# Commit (v = view, n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (v,n,d,i)).
# Simplified commit (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
.async commitOut `map(|(node_id, (slot, digest, i))| (node_id, create_commit(slot, digest, i, node_id))) -> dest_sink(commit_to_pbft_replica_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`
.async commitIn `null::<(u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)>()` `source_stream(commit_from_pbft_replica_source) -> filter_map(|x: Result<(u32, BytesMut, u32, BytesMut), _>| (unwrap_commit(deserialize(x.unwrap().1), my_id)))`

######################## end relation definitions

.persist prePrepareLog
.persist perpareLog
.persist commitLog

IsLeader() :- leaderId(i), id(i)
slots(s) :- startSlot(s)
nextSlot(max(s)) :- slots(s)

######################## pre-prepare
IndexedPayloads(command, sigc, digest, index()) :~ clientIn(command, sigc, digest), IsLeader()
prePrepareOut@r((slot+offset), digest, command, sigc) :~ IndexedPayloads(command, sigc, digest, offset), nextSlot(slot), pbftReplicas(r)

# Increment slot
NumPayloads(max(offset) + 1) :- IndexedPayloads(_, _, _, offset)
slots(s + num) :+ nextSlot(s), NumPayloads(num)
######################## end pre-prepare

######################## prepare
prePrepareLog(slot, digest, sigp, command, sigc) :- prePrepareIn(slot, digest, sigp, command, sigc)
prepareOut@r(slot, digest, i) :~ prePrepareIn(slot, digest, sigp, command, sigc), pbftReplicas(r), id(i)
######################## end prepare

######################## commit
prepareLog(slot, digest, sigp, i) :- prepareIn(slot, digest, i, sigp)
pendingPrepares(slot, digest, sigp, i) :- prepareIn(slot, digest, i, sigp)
numPrepares(slot, digest, count(i)) :- pendingPrepares(slot, digest, _, i), !commitSent(slot, digest)
shouldSendCommit(slot, digest) :- numPrepares(slot, digest, c), quorum(q), (c >= q), !commitSent(slot, digest)
commitOut@r(slot, digest, i) :~ shouldSendCommit(slot, digest), pbftReplicas(r), id(i)

# Store own commit locally
commitLog(slot, digest, i) :- shouldSendCommit(slot, digest)
# Persist prepares until we get it from all 3f+1 replicas
fullPrepare(slot, digest) :- numPrepares(slot, digest, c), fullQuorum(c)
pendingPrepares(slot, digest, sigp, i) :+ pendingPrepares(slot, digest, sigp, i), !fullPrepare(slot, digest)
commitSent(slot, digest) :+ shouldSendCommit(slot, digest), !fullPrepare(slot, digest)
commitSent(slot, digest) :+ commitSent(slot, digest), !fullPrepare(slot, digest)
######################## end commit

######################## reply
commitLog(slot, digest, i) :- commitIn(slot, digest, i, sigi) # Note: sigi is not stored, because the commit log is not used during view-change so we don't need to prove to others that this is the message we got.
pendingCommits(slot, digest, i) :- commitIn(slot, digest, i, sigi)
numCommits(slot, digest, count(i)) :- pendingCommits(slot, digest, i), !replySent(slot, digest)
shouldSendReply(slot, digest, command) :- numCommits(slot, digest, c), quorum(q), (c >= q), !replySent(slot, digest)
clientOut@r(command, slot) :~ shouldSendReply(slot, _, command), replica(r)

fullCommit(slot, digest) :- numCommits(slot, digest, c), fullQuorum(c)
pendingCommits(slot, digest, i) :+ pendingCommits(slot, digest, i), !fullCommit(slot, digest)
replySent(slot, digest) :+ shouldSendReply(slot, digest, _), !fullCommit(slot, digest)
replySent(slot, digest) :+ replySent(slot, digest), !fullCommit(slot, digest)
######################## end reply
    "#
    );

    launch_flow(df).await;
}
