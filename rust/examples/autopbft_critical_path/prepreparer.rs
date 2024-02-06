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
pub struct PrepreparerArgs {
    #[clap(long = "prepreparer.index")]
    prepreparer_index: Option<u32>,
    #[clap(long = "prepreparer.leader-index")]
    prepreparer_leader_index: Option<u32>,
    #[clap(long = "prepreparer.f")]
    prepreparer_f: Option<u32>,
    #[clap(long = "prepreparer.num-preparer-partitions")]
    prepreparer_num_preparer_partitions: Option<u32>,
    #[clap(long = "prepreparer.num-committer-partitions")]
    prepreparer_num_committer_partitions: Option<u32>,
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

fn unwrap_pre_prepare(msg: (u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>), receiver: u32) -> Option<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>)> {
    let (slot, digest, sigp, command, sigc) = msg;
    // println!("Unwrapping prePrepare: (Slot: {:?}, {:?}, {:?}, {:?}, Receiver: {:?})", slot, digest[0], sigp[0], command[0], receiver);
    let mut mac = get_mac(0, receiver);
    mac.update(&slot.to_be_bytes());
    mac.update(digest.as_slice());
    if mac.verify_slice(sigp.as_slice()).is_err() {
        panic!("PrePrepare leader signature {:?} was incorrect", sigp)
        // TODO: Change to return None after debugging, so a Byzantine primary can't crash the replica
    }
    // println!("PrePrepare verified: {:?}", sigp[0]);
    // TODO: Verify the command signature sigc. Needs client to send authenticator instead of single sig?
    Some((slot, digest, sigp, command, sigc))
}

// Simplified prepare (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
fn create_prepare(slot: u32, digest: Rc<Vec<u8>>, sender: u32, receiver: u32) -> (u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>) {
    // println!("Creating prepare: (Slot: {:?}, {:?}, Sender: {:?}, Receiver: {:?})", slot, digest[0], sender, receiver);
    let mut mac = get_mac(sender, receiver);
    mac.update(&slot.to_be_bytes());
    mac.update(digest.as_slice());
    mac.update(&sender.to_be_bytes());
    let sigi = mac.finalize().into_bytes();
    // println!("Signed prepare: {:?}", sigi[0]);
    (slot, digest, sender, Rc::new(sigi.to_vec()))
}

// Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <o = command operation>, sig(c) = signature of o).
fn create_decoupled_pre_prepare(slot: u32, digest: Rc<Vec<u8>>, command: Rc<Vec<u8>>, leader_id: u32) -> (u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>) {
    // Both sender & receiver = leader_id, since this is a message sent between 2 decoupled components
    let mut mac = get_mac(leader_id, leader_id);
    mac.update(&slot.to_be_bytes());
    mac.update(digest.as_slice());
    let sigp = mac.finalize().into_bytes();
    // println!("Signed prePrepare: {:?}", sigp[0]);
    (slot, digest, command, Rc::new(sigp.to_vec()))
}

// Need to provide: clients, replicas, and smr (corresponding state machine replica)
pub async fn run(cfg: PrepreparerArgs, mut ports: HashMap<String, ServerOrBound>) {
    let my_id = cfg.prepreparer_index.unwrap();
    println!("Prepreparer {:?} started", my_id);

    let preprepare_to_committer_sink = ports
        .remove("send_to$committers$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let prepare_to_preparer_sink = ports
        .remove("send_to$preparers$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

       let pre_prepare_from_leader_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let leader_id = cfg.prepreparer_leader_index.unwrap();
    let f = cfg.prepreparer_f.unwrap();

    let num_preparer_partitions = cfg.prepreparer_num_preparer_partitions.unwrap();
    let preparer_start_ids: Vec<u32> = (0u32..u32::try_from(3*f+1).unwrap())
        .step_by(num_preparer_partitions.try_into().unwrap())
        .collect();
    // Carefully calculate our OWN committer's ID (sending a PrePrepare along for monotonic decoupling)
    let num_committer_partitions = cfg.prepreparer_num_committer_partitions.unwrap();
    let committers_start_id = leader_id * num_committer_partitions;

    println!("Prepreparer {:?} ready", my_id);

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input preparerStartIDs `repeat_iter(preparer_start_ids.clone()) -> map(|p| (p,))` # Assume = 0,m,2m,...,(n-1)*m, for n preparers and m partitions
.input numPreparerPartitions `repeat_iter([(num_preparer_partitions,),])` 
.input committersStartID `repeat_iter([(committers_start_id,),])`
.input numCommitterPartitions `repeat_iter([(num_committer_partitions,),])`

# IDB

# Pre-prepare (v = view, n = slot, d = hash digest of m, sig(p) = signature of (v,n,d), m = <o = command operation, t = timestamp, c = client>, sig(c) = signature of (o,t,c)).
# Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <o = command operation>, sig(c) = signature of o).
.async prePrepareIn `null::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>)>()` `source_stream(pre_prepare_from_leader_source) -> filter_map(|x| (unwrap_pre_prepare(deserialize_from_bytes::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>)>(x.unwrap().1).unwrap(), my_id)))`

# Prepare (v = view, n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (v,n,d,i)).
# Simplified prepare (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
.async prepareOut `map(|(node_id, (slot, digest, i))| (node_id, serialize_to_bytes(create_prepare(slot, digest, i, node_id)))) -> dest_sink(prepare_to_preparer_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`

# Decoupled pre-prepare to committer (n, d, m, sig)
.async prePrepareCommitterOut `map(|(node_id, (slot, digest, command))| (node_id, serialize_to_bytes(create_decoupled_pre_prepare(slot, digest, command, leader_id)))) -> dest_sink(preprepare_to_committer_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`

.input prePrepareLog `null::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>)>()`

######################## end relation definitions

.persist prePrepareLog

######################## prepare
prePrepareLog(slot, digest, sigp, command, sigc) :- prePrepareIn(slot, digest, sigp, command, sigc)
prepareOut@(r+(slot%n))(slot, digest, i) :~ prePrepareIn(slot, digest, sigp, command, sigc), id(i), preparerStartIDs(r), numPreparerPartitions(n)
prePrepareCommitterOut@(r+(slot%n))(slot, digest, command) :~ prePrepareIn(slot, digest, _, command, _), committersStartID(r), numCommitterPartitions(n)
######################## end prepare
    "#
    );

    launch_flow(df).await;
}
