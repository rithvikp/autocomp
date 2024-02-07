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
pub struct LeaderArgs {
    #[clap(long = "leader.index")]
    leader_index: Option<u32>,
    #[clap(long = "leader.f")]
    leader_f: Option<u32>,
    #[clap(long = "leader.num-prepreparer-partitions")]
    leader_num_prepreparer_partitions: Option<u32>,
}

// Output: Command, sigc, digest
fn deserialize(msg: BytesMut, client_requests: &prometheus::Counter) -> Option<(Rc<Vec<u8>>,Rc<Vec<u8>>,Rc<Vec<Vec<u8>>>,Rc<Vec<u8>>)> {
    if msg.len() == 0 {
        return None;
    }
    let s = multipaxos_proto::LeaderInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();
    client_requests.inc();
    // println!("Primary received {:?}", s);

    match s.request.unwrap() {
        multipaxos_proto::leader_inbound::Request::ClientRequest(r) => {
            let command_id = r.command.command_id.clone();
            let command = r.command.command.clone();
            // TODO: Needs to safely handle None so Byzantine clients can't crash the PBFT replica
            let signatures = vec![
                r.command.signature0.clone().unwrap(),
                r.command.signature1.clone().unwrap(),
                r.command.signature2.clone().unwrap(),
                r.command.signature3.clone().unwrap(),
            ];
            let digest = r.command.digest.clone().unwrap();
            // println!("Command {:?}, sigc {:?}, digest {:?}", command[0], sigc[0], digest[0]);
            let mut command_id_buf = Vec::new();
            command_id.encode(&mut command_id_buf).unwrap();

            Some((Rc::new(command_id_buf), Rc::new(command), Rc::new(signatures), Rc::new(digest),))
        }
        _ => panic!("Unexpected message from the client"), // TODO: Change to return None after debugging, so Byzantine clients can't crash the PBFT replica
    }
}

// TODO: Technically, this signing scheme doesn't follow our rewrites (since we're introducing more keys since there are more replica IDs now). It shouldn't impact performance though.
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
fn create_pre_prepare(slot: u32, digest: Rc<Vec<u8>>, command_id: Rc<Vec<u8>>, command: Rc<Vec<u8>>, sigc: Rc<Vec<Vec<u8>>>, receiver: u32) -> (u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>) {
    // println!("Creating prePrepare: (Slot: {:?}, {:?}, {:?}, {:?}, Receiver: {:?})", slot, digest[0], command[0], sigc[0], receiver);
    // sender = 0 because only the leader (id = 0) sends prePrepares
    let mut mac = get_mac(0, receiver);
    mac.update(&slot.to_be_bytes());
    mac.update(digest.as_slice());
    let sigp = mac.finalize().into_bytes();
    // println!("Signed prePrepare: {:?}", sigp[0]);
    (slot, digest, Rc::new(sigp.to_vec()), command_id, command, sigc)
}

// Need to provide: clients, replicas, and smr (corresponding state machine replica)
pub async fn run(cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let client_requests = prometheus::register_counter!("autopbft_requests_total", "help").unwrap();

    let my_id = cfg.leader_index.unwrap();
    println!("Leader {:?} started, waiting for clients", my_id);

    let pre_prepare_to_prepreparer_port = ports
        .remove("send_to$prepreparers$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let client_recv = ports
        .remove("receive_from$clients$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let prepreparers = pre_prepare_to_prepreparer_port.keys.clone();
    let pre_prepare_to_prepreparer_sink = pre_prepare_to_prepreparer_port.into_sink();

    let f = cfg.leader_f.unwrap();
    let num_prepreparer_partitions = cfg.leader_num_prepreparer_partitions.unwrap();
    let prepreparer_start_ids: Vec<u32> = (0u32..((3*f+1) * num_prepreparer_partitions))
        .step_by(num_prepreparer_partitions.try_into().unwrap())
        .collect();

    println!("Leader {:?} ready", my_id);

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input leaderId `repeat_iter([(0,),])`
.input prepreparerStartIDs `repeat_iter(prepreparer_start_ids.clone()) -> map(|p| (p,))` # Assume = 0,m,2m,...,(n-1)*m, for n prepreparers and m partitions
.input numPrepreparerPartitions `repeat_iter([(num_prepreparer_partitions,),])` 

# IDB

# Request (<o = command operation, t = timestamp, c = client>, sig(c) = signature of (o,t,c)).
# Simplified request (<o = command operation>, sig(c) = signature of o, d = digest of o).
.async clientIn `null::<(Rc<Vec<u8>>,)>()` `source_stream(client_recv) -> filter_map(|x: Result<(u32, BytesMut,), _>| (deserialize(x.unwrap().1, &client_requests)))`

.input startSlot `repeat_iter([(0 as u32,),])`
.input nextSlot `null::<(u32,)>()`
.input SlottedPayloads `null::<(Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>, Rc<Vec<u8>>, u32,)>()`

# Pre-prepare (v = view, n = slot, d = hash digest of m, sig(p) = signature of (v,n,d), m = <o = command operation, t = timestamp, c = client>, sig(c) = signature of (o,t,c)).
# Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <command_id, o = command operation>, sig(c) = signature of o).
.async prePrepareOut `map(|(node_id, (slot, digest, command_id, command, sigc))| (node_id, serialize_to_bytes(create_pre_prepare(slot, digest, command_id, command, sigc, node_id)))) -> dest_sink(pre_prepare_to_prepreparer_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`

######################## end relation definitions

IsLeader() :- leaderId(i), id(i)
slots(s) :- startSlot(s)
nextSlot(max(s)) :- slots(s)

######################## pre-prepare
IndexedPayloads(commandID, command, sigc, digest, index()) :- clientIn(commandID, command, sigc, digest), IsLeader()
SlottedPayloads(commandID, command, sigc, digest, (slot + offset)) :- IndexedPayloads(commandID, command, sigc, digest, offset), nextSlot(slot)
prePrepareOut@(r+(slot%n))(slot, digest, commandID, command, sigc) :~ SlottedPayloads(commandID, command, sigc, digest, slot), prepreparerStartIDs(r), numPrepreparerPartitions(n)

# Increment slot
NumPayloads(max(offset)) :- IndexedPayloads(_, _, _, _, offset)
slots(s) :+ nextSlot(s), !NumPayloads(n)
slots(s + num + 1) :+ nextSlot(s), NumPayloads(num)
######################## end pre-prepare
    "#
    );

    launch_flow(df).await;
}
