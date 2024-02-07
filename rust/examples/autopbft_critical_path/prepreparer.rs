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
    #[clap(long = "prepreparer.num-prepreparer-partitions")]
    prepreparer_num_prepreparer_partitions: Option<u32>,
    #[clap(long = "prepreparer.num-preparer-partitions")]
    prepreparer_num_preparer_partitions: Option<u32>,
    #[clap(long = "prepreparer.num-committer-partitions")]
    prepreparer_num_committer_partitions: Option<u32>,
}

fn get_client_key(my_id: u32) -> &'static [u8] {
    match my_id {
        0 => b"client0",
        1 => b"client1",
        2 => b"client2",
        3 => b"client3",
        _ => panic!("Invalid pbft_replica index {}", my_id),
    }
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

fn unwrap_pre_prepare(msg: (u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>), receiver: u32, leader_id: u32, num_prepreparer_partitions: u32) -> Option<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>)> {
    let (slot, digest, sigp, command_id, command, sigc) = msg;
    // println!("Unwrapping prePrepare: (Slot: {:?}, {:?}, {:?}, {:?}, Receiver: {:?})", slot, digest[0], sigp[0], command[0], receiver);

    // Verify the pre-prepare signature. Note: sender = 0 because only the leader (id = 0) sends pre-prepares
    let mut preprepare_mac = get_mac(0, receiver);
    preprepare_mac.update(&slot.to_be_bytes());
    preprepare_mac.update(digest.as_slice());
    if preprepare_mac.verify_slice(sigp.as_slice()).is_err() {
        panic!("PrePrepare leader signature {:?} was incorrect", sigp)
        // TODO: Change to return None after debugging, so a Byzantine primary can't crash the replica
    }
    // println!("PrePrepare verified: {:?}", sigp[0]);

    // Verify the digest is from the command
    let mut hasher = Sha256::new();
    hasher.update(command.as_slice());
    let result = hasher.finalize().to_vec();
    if result != digest.as_slice() {
        panic!("Client digest {:?} didn't match expected digest {:?}", digest, result)
        // TODO: Change to return None after debugging, so Byzantine clients can't crash the PBFT replica
    }
    // println!("Digest {:?} matched, checking sig", digest[0]);

    // Verify the signature is signed over the digest
    let mut client_mac = Hmac::<Sha256>::new_from_slice(get_client_key(leader_id)).unwrap();
    client_mac.update(digest.as_slice());
    if client_mac.verify_slice(sigc[leader_id as usize].as_slice()).is_err() {
        panic!("Client signature {:?} was incorrect", sigc[leader_id as usize])
        // TODO: Change to return None after debugging, so Byzantine clients can't crash the PBFT replica
    }
    // println!("Sig {:?} matched", sigc[receiver as usize]);

    // Message verification (partitioning invariant is preserved)
    if (slot % num_prepreparer_partitions) != (receiver % num_prepreparer_partitions) {
        println!("PrePrepare slot {:?} was incorrectly sent to partition {:?}", slot, receiver);
        return None
    }

    Some((slot, digest, sigp, command_id, command, sigc))
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

// Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <command_id, o = command operation>, sig(c) = signature of o).
fn create_decoupled_pre_prepare(slot: u32, digest: Rc<Vec<u8>>, command_id: Rc<Vec<u8>>, command: Rc<Vec<u8>>, leader_id: u32) -> (u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>) {
    // Both sender & receiver = leader_id, since this is a message sent between 2 decoupled components
    let mut mac = get_mac(leader_id, leader_id);
    mac.update(&slot.to_be_bytes());
    mac.update(digest.as_slice());
    let sigp = mac.finalize().into_bytes();
    // println!("Signed prePrepare: {:?}", sigp[0]);
    (slot, digest, command_id, command, Rc::new(sigp.to_vec()))
}

// Need to provide: clients, replicas, and smr (corresponding state machine replica)
pub async fn run(cfg: PrepreparerArgs, mut ports: HashMap<String, ServerOrBound>) {
    let client_requests = prometheus::register_counter!("autopbft_requests_total", "help").unwrap();
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

    let num_prepreparer_partitions = cfg.prepreparer_num_prepreparer_partitions.unwrap();
    let num_preparer_partitions = cfg.prepreparer_num_preparer_partitions.unwrap();
    let preparer_start_ids: Vec<u32> = (0u32..((3*f+1) * num_preparer_partitions))
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
# Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <command_id, o = command operation>, sig(c) = signature of o).
.async prePrepareIn `null::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>)>()` `source_stream(pre_prepare_from_leader_source) -> filter_map(|x| (unwrap_pre_prepare(deserialize_from_bytes::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>)>(x.unwrap().1).unwrap(), my_id, leader_id, num_prepreparer_partitions)))`

# Prepare (v = view, n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (v,n,d,i)).
# Simplified prepare (n = slot, d = hash digest of m, i = id of self, sig(i) = signature of (n,d,i)).
.async prepareOut `map(|(node_id, (slot, digest, i))| (node_id, serialize_to_bytes(create_prepare(slot, digest, i, node_id)))) -> dest_sink(prepare_to_preparer_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`

# Decoupled pre-prepare to committer (n, d, m, sig)
.async prePrepareCommitterOut `map(|(node_id, (slot, digest, command_id, command))| (node_id, serialize_to_bytes(create_decoupled_pre_prepare(slot, digest, command_id, command, leader_id)))) -> dest_sink(preprepare_to_committer_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`

.input prePrepareLog `null::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>)>()`

######################## end relation definitions

.persist prePrepareLog

######################## prepare
prePrepareLog(slot, digest, sigp, commandID, command, sigc) :- prePrepareIn(slot, digest, sigp, commandID, command, sigc)
prepareOut@(r+(slot%n))(slot, digest, i) :~ prePrepareIn(slot, digest, sigp, commandID, command, sigc), id(i), preparerStartIDs(r), numPreparerPartitions(n)
prePrepareCommitterOut@(r+(slot%n))(slot, digest, commandID, command) :~ prePrepareIn(slot, digest, _, commandID, command, _), committersStartID(r), numCommitterPartitions(n)
######################## end prepare
    "#
    );

    launch_flow(df).await;
}
