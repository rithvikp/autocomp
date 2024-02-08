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

#[derive(clap::Args, Debug)]
pub struct PreparerArgs {
    #[clap(long = "preparer.index")]
    preparer_index: Option<u32>,
    #[clap(long = "preparer.f")]
    preparer_f: Option<u32>,
    #[clap(long = "preparer.num-preparer-partitions")]
    preparer_num_preparer_partitions: Option<u32>,
    #[clap(long = "preparer.num-committer-partitions")]
    preparer_num_committer_partitions: Option<u32>,
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

fn unwrap_prepare(msg: (u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>), receiver: u32, num_preparer_partitions: u32) -> Option<(u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)> {
    let (slot, digest, sender, sigi) = msg;
    // println!("Unwrapping prepare: (Slot: {:?}, {:?}, Sender: {:?}, {:?})", slot, digest[0], sender, sigi[0]);
    let mut mac = get_mac(sender, receiver);
    mac.update(&slot.to_be_bytes());
    mac.update(digest.as_slice());
    mac.update(&sender.to_be_bytes());
    if mac.verify_slice(sigi.as_slice()).is_err() {
        panic!("Prepare signature {:?} from {:?} was incorrect", sigi, sender)
        // TODO: Change to return None after debugging, so another Byzantine replica can't crash this replica
    }
    // println!("Prepare verified: {:?}", sigi[0]);

    // Message verification (partitioning invariant is preserved)
    if (slot % num_preparer_partitions) != (receiver % num_preparer_partitions) {
        println!("Prepare slot {:?} was incorrectly sent to partition {:?}", slot, receiver);
        return None
    }

    Some((slot, digest, sender, sigi))
}

// Simplified commit (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
fn create_commit(slot: u32, digest: Rc<Vec<u8>>, sender: u32, receiver: u32) -> (u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>) {
    // println!("Creating commit: (Slot: {:?}, {:?}, Sender: {:?}, Receiver: {:?})", slot, digest[0], sender, receiver);
    let mut mac = get_mac(sender, receiver);
    mac.update(&slot.to_be_bytes());
    mac.update(digest.as_slice());
    mac.update(&sender.to_be_bytes());
    let sigi = mac.finalize().into_bytes();
    // println!("Signed commit: {:?}", sigi[0]);
    (slot, digest, sender, Rc::new(sigi.to_vec()))
}

// Need to provide: clients, replicas, and smr (corresponding state machine replica)
pub async fn run(cfg: PreparerArgs, mut ports: HashMap<String, ServerOrBound>) {
    let client_requests = prometheus::register_counter!("autopbft_requests_total", "help").unwrap();
    let my_id = cfg.preparer_index.unwrap();
    // println!("Preparer {:?} started", my_id);

    let commit_to_committer_sink = ports
        .remove("send_to$committers$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let prepare_from_prepreparer_source = ports
        .remove("receive_from$prepreparers$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let f = cfg.preparer_f.unwrap();

    let num_preparer_partitions = cfg.preparer_num_preparer_partitions.unwrap();
    let num_committer_partitions = cfg.preparer_num_committer_partitions.unwrap();
    let committer_start_ids: Vec<u32> = (0u32..((3*f+1) * num_committer_partitions))
        .step_by(num_committer_partitions.try_into().unwrap())
        .collect();

    // println!("Preparer {:?} ready", my_id);

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input committerStartIDs `repeat_iter(committer_start_ids.clone()) -> map(|p| (p,))` # Assume = 0,m,2m,...,(n-1)*m, for n committers and m partitions
.input numCommitterPartitions `repeat_iter([(num_committer_partitions,),])` 
.input fullQuorum `repeat_iter([(3*f+1,),])`

# IDB

# Prepare (v = view, n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (v,n,d,i)).
# Simplified prepare (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
.async prepareIn `null::<(u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)>()` `source_stream(prepare_from_prepreparer_source) -> filter_map(|x| (unwrap_prepare(deserialize_from_bytes::<(u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)>(x.unwrap().1).unwrap(), my_id, num_preparer_partitions)))`

# Commit (v = view, n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (v,n,d,i)).
# Simplified commit (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
.async commitOut `map(|(node_id, (slot, digest, i))| (node_id, serialize_to_bytes(create_commit(slot, digest, i, node_id)))) -> dest_sink(commit_to_committer_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`

.input prepareLog `null::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, u32,)>()`
.input numPrepares `null::<(u32, Rc<Vec<u8>>, u32,)>()`

######################## end relation definitions

.persist prepareLog

######################## commit
prepareLog(slot, digest, sigp, i) :- prepareIn(slot, digest, i, sigp)
pendingPrepares(slot, digest, sigp, i) :- prepareIn(slot, digest, i, sigp)
numPrepares(slot, digest, count(i)) :- pendingPrepares(slot, digest, _, i)
# Waiting for 3f+1, then deleting & broadcasting, is much more performant than waiting for 2f+1, broadcasting, then checking to delete
# For some reason using just fullPrepare gives an error.
fullPrepare1(slot, digest) :- numPrepares(slot, digest, c), fullQuorum(c)
fullPrepare2(slot, digest) :- numPrepares(slot, digest, c), fullQuorum(c)
commitOut@(r+(slot%n))(slot, digest, i) :~ fullPrepare1(slot, digest), id(i), committerStartIDs(r), numCommitterPartitions(n)

pendingPrepares(slot, digest, sigp, i) :+ pendingPrepares(slot, digest, sigp, i), !fullPrepare2(slot, digest)
######################## end commit
    "#
    );

    launch_flow(df).await;
}
