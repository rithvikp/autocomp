use frankenpaxos::multipaxos_proto;
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
use std::{collections::HashMap, io::Cursor, rc::Rc};
use sha2::{Sha256, Digest};
use hmac::{Hmac, Mac};
use chrono::Local;

#[derive(clap::Args, Debug)]
pub struct CommitterArgs {
    #[clap(long = "committer.index")]
    committer_index: Option<u32>,
    #[clap(long = "committer.leader-index")]
    committer_leader_index: Option<u32>,
    #[clap(long = "committer.f")]
    committer_f: Option<u32>,
    #[clap(long = "committer.num-committer-partitions")]
    committer_num_committer_partitions: Option<u32>,
}

fn get_replica_key(my_id: u32) -> &'static [u8] {
    match my_id {
        0 => b"replica0",
        1 => b"replica1",
        2 => b"replica2",
        3 => b"replica3",
        _ => panic!("Invalid pbft_replica index {}", my_id),
    }
}

// Will sign slot + digest with the replica key, the put the signature in signature0
fn serialize(digest: Rc<Vec<u8>>, command_id_buf: Rc<Vec<u8>>, command: Rc<Vec<u8>>, slot: u32, leader_id: u32, client_requests: &prometheus::Counter) -> bytes::Bytes {
    client_requests.inc();
    let slot_i32 = i32::try_from(slot).unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(get_replica_key(leader_id)).unwrap();
    mac.update(&slot_i32.to_be_bytes());
    mac.update(digest.as_slice());
    let sigp = mac.finalize().into_bytes();

    let command_id = multipaxos_proto::CommandId::decode(&mut Cursor::new(command_id_buf.as_ref())).unwrap();

    let out = multipaxos_proto::ReplicaInbound {
        request: Some(multipaxos_proto::replica_inbound::Request::Chosen(
            multipaxos_proto::Chosen {
                slot: slot_i32,
                command_batch_or_noop: multipaxos_proto::CommandBatchOrNoop {
                    value: Some(
                        multipaxos_proto::command_batch_or_noop::Value::CommandBatch(
                            multipaxos_proto::CommandBatch {
                                command: vec![multipaxos_proto::Command {
                                    command_id: command_id,
                                    command: command.to_vec(),
                                    signature0: Some(sigp.to_vec()),
                                    signature1: None,
                                    signature2: None,
                                    signature3: None,
                                    digest: None,
                                }],
                            },
                        ),
                    ),
                }
            },
        )),
    };
    // let date = Local::now();
    // println!("{} Sending to replica {:?} command {:?} slot {:?}", date.format("[%b %d %H:%M:%S%.9f]"), leader_id, command[0], slot);

    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

fn get_mac(replica_1_id: u32, replica_2_id: u32) -> Hmac<Sha256> {
    // Key scheme: If replica 1 is writing to replica 2, then the key is b"12". Lowest id first, so replica 2 writing to replica 1 uses the same key.
    let key;
    if replica_1_id < replica_2_id {
        key = replica_1_id * 10 + replica_2_id;
    } else {
        key = replica_2_id * 10 + replica_1_id;
    }
    Hmac::<Sha256>::new_from_slice(&key.to_be_bytes()).unwrap()
}

// Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <o = command operation>, sig(c) = signature of o).
fn unwrap_decoupled_pre_prepare(msg: (u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>), my_id: u32, leader_id: u32, num_committer_partitions: u32) -> Option<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>,)> {
    let (slot, digest, command_id, command, sigp) = msg;
    // Both sender & receiver = leader_id, since this is a message sent between 2 decoupled components
    let mut mac = get_mac(leader_id, leader_id);
    mac.update(&slot.to_be_bytes());
    mac.update(digest.as_slice());
    if mac.verify_slice(sigp.as_slice()).is_err() {
        panic!("Decoupled PrePrepare leader signature {:?} was incorrect", sigp)
    }
    // let date = Local::now();
    // println!("{} Received prePrepare slot {:?} digest {:?}", date.format("[%b %d %H:%M:%S%.9f]"), slot, digest[0]);
    
    // Message verification (partitioning invariant is preserved)
    if (slot % num_committer_partitions) != (my_id % num_committer_partitions) {
        println!("Commit slot {:?} was incorrectly sent to partition {:?}", slot, my_id);
        return None
    }

    Some((slot, digest, command_id, command,))
}

fn unwrap_commit(msg: (u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>), receiver: u32, num_committer_partitions: u32) -> Option<(u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)> {
    let (slot, digest, sender, sigi) = msg;
    // println!("Unwrapping commit: (Slot: {:?}, {:?}, Sender: {:?}, {:?})", slot, digest[0], sender, sigi[0]);
    let mut mac = get_mac(sender, receiver);
    mac.update(&slot.to_be_bytes());
    mac.update(digest.as_slice());
    mac.update(&sender.to_be_bytes());
    if mac.verify_slice(sigi.as_slice()).is_err() {
        panic!("Commit signature {:?} from {:?} was incorrect", sigi, sender)
        // TODO: Change to return None after debugging, so another Byzantine replica can't crash this replica
    }
    // let date = Local::now();
    // println!("{} Received commit slot {:?} digest {:?} from sender {:?}", date.format("[%b %d %H:%M:%S%.9f]"), slot, digest[0], sender);
    // println!("Commit verified: {:?}", sigi[0]);

    // Message verification (partitioning invariant is preserved)
    if (slot % num_committer_partitions) != (receiver % num_committer_partitions) {
        println!("Commit slot {:?} was incorrectly sent to partition {:?}", slot, receiver);
        return None
    }

    Some((slot, digest, sender, sigi))
}

// Need to provide: clients, replicas, and smr (corresponding state machine replica)
pub async fn run(cfg: CommitterArgs, mut ports: HashMap<String, ServerOrBound>) {
    let client_requests = prometheus::register_counter!("autopbft_requests_total", "help").unwrap();
    let my_id = cfg.committer_index.unwrap();
    println!("Committer {:?} started", my_id);

    let replica_send = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let commit_from_preparer_source = ports
        .remove("receive_from$preparers$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let preprepare_from_prepreparer_source = ports
        .remove("receive_from$prepreparers$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let leader_id = cfg.committer_leader_index.unwrap();
    let f = cfg.committer_f.unwrap();
    let num_committer_partitions = cfg.committer_num_committer_partitions.unwrap();

    println!("Committer {:?} ready", my_id);

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input replica `repeat_iter([(leader_id,),])` # Note: Assumes same number of replicas as pbft_replicas, indexed from 0
.input quorum `repeat_iter([(2*f+1,),])`
.input fullQuorum `repeat_iter([(3*f+1,),])`

# IDB

# Reply (v = view, t = timestamp, c = client, i = id of self, r = result of execution, sig(i) = signature of (v,t,c,i,r)).
# Simplified reply (o = command operation, n = slot). The difference in message content is because we're sending this to the state machine, not the client that sent the request.
.async clientOut `map(|(node_id, (digest, command_id, payload, slot,))| (node_id, serialize(digest, command_id, payload, slot, leader_id, &client_requests))) -> dest_sink(replica_send)` `null::<(Rc<Vec<u8>>, u32,)>()`

.async prePrepareIn `null::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>,)>()` `source_stream(preprepare_from_prepreparer_source) -> filter_map(|x| (unwrap_decoupled_pre_prepare(deserialize_from_bytes::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>,)>(x.unwrap().1).unwrap(), my_id, leader_id, num_committer_partitions)))`

# Commit (v = view, n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (v,n,d,i)).
# Simplified commit (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
.async commitIn `null::<(u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)>()` `source_stream(commit_from_preparer_source) -> filter_map(|x| (unwrap_commit(deserialize_from_bytes::<(u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)>(x.unwrap().1).unwrap(), my_id, num_committer_partitions)))`

######################## end relation definitions

.persist commitLog

######################## prepare
pendingPrePrepares(slot, digest, commandID, command) :- prePrepareIn(slot, digest, commandID, command)
pendingPrePrepares(slot, digest, commandID, command) :+ pendingPrePrepares(slot, digest, commandID, command), !fullCommit(slot, digest)
######################## end prepare

######################## reply
commitLog(slot, digest, i) :- commitIn(slot, digest, i, sigi) # Note: sigi is not stored, because the commit log is not used during view-change so we don't need to prove to others that this is the message we got.
pendingCommits(slot, digest, i) :- commitIn(slot, digest, i, sigi)
numCommits(slot, digest, count(i)) :- pendingCommits(slot, digest, i)
fullCommit(slot, digest) :- numCommits(slot, digest, c), fullQuorum(c)
clientOut@r(digest, commandID, command, slot) :~ fullCommit(slot, digest), pendingPrePrepares(slot, digest, commandID, command), replica(r)

sentClientOut(slot, digest) :- fullCommit(slot, digest), pendingPrePrepares(slot, digest, _, _)
pendingCommits(slot, digest, i) :+ pendingCommits(slot, digest, i), !sentClientOut(slot, digest)
######################## end reply
    "#
    );

    launch_flow(df).await;
}
