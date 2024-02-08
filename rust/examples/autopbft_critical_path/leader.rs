use frankenpaxos::multipaxos_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::{collections::HashMap, io::Cursor, rc::Rc};
use sha2::Sha256;
use hmac::{Hmac, Mac};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {
    #[clap(long = "leader.index")]
    leader_index: Option<u32>,
    #[clap(long = "leader.f")]
    leader_f: Option<u32>,
    #[clap(long = "leader.num-proxy-leader-partitions")]
    leader_num_proxy_leader_partitions: Option<u32>,
    #[clap(long = "leader.num-prepreparer-partitions")]
    leader_num_prepreparer_partitions: Option<u32>,
}

// Output: Command, sigc, digest
fn deserialize(msg: BytesMut, client_requests: &prometheus::Counter) -> Option<(Rc<Vec<u8>>,Rc<Vec<u8>>,Rc<Vec<Vec<u8>>>,Rc<Vec<u8>>)> {
    if msg.len() == 0 {
        return None;
    }
    let s_option = multipaxos_proto::LeaderInbound::decode(&mut Cursor::new(msg.as_ref()));
    if s_option.is_err() {
        // For some reason, on large deployments, we sometimes get errors like:
        // thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: DecodeError { description: "delimited length exceeded", stack: [("Command", "command_id"), ("CommandBatch", "command"), ("ClientRequestBatch", "batch"), ("LeaderInbound", "request")] }'
        // and
        // thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: DecodeError { description: "invalid wire type value: 6", stack: [("Command", "command_id"), ("CommandBatch", "command"), ("ClientRequestBatch", "batch"), ("LeaderInbound", "request")] }'
        // They don't seem to prevent the protocol from executing correctly, so we'll just ignore it.
        // println!("Leader received an invalid message from the client. Error: {:?}", s_option.err().unwrap());
        return None;
    }
    let s = s_option.unwrap();
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
fn create_pre_prepare(slot: u32, digest: Rc<Vec<u8>>, command_id: Rc<Vec<u8>>, command: Rc<Vec<u8>>, sigc: Rc<Vec<Vec<u8>>>, f: u32) -> (u32, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>) {
    // println!("Creating prePrepare: (Slot: {:?}, {:?}, {:?}, {:?}, Receiver: {:?})", slot, digest[0], command[0], sigc[0], receiver);
    // sender = 0 because only the leader (id = 0) sends prePrepares
    // leader has to send a MAC authenticator since the proxy leader will need to talk to all prepreparers
    let mut sigps = Vec::new();
    for i in 0..(3*f+1) {
        let mut mac = get_mac(0, i);
        mac.update(&slot.to_be_bytes());
        mac.update(digest.as_slice());
        sigps.push(mac.finalize().into_bytes().to_vec());
    }
    // println!("Signed prePrepare: {:?}", sigp[0]);
    (slot, digest, Rc::new(sigps), command_id, command, sigc)
}

// Need to provide: clients, replicas, and smr (corresponding state machine replica)
pub async fn run(cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let client_requests = prometheus::register_counter!("autopbft_requests_total", "help").unwrap();

    let my_id = cfg.leader_index.unwrap();
    // println!("Leader {:?} started, waiting for clients", my_id);

    let pre_prepare_to_proxy_leader_sink = ports
        .remove("send_to$proxy_leaders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let client_recv = ports
        .remove("receive_from$clients$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();
    
    let f = cfg.leader_f.unwrap();
    let num_proxy_leader_partitions = cfg.leader_num_proxy_leader_partitions.unwrap();
    let proxy_leader_start_id = my_id * num_proxy_leader_partitions;

    // println!("Leader {:?} ready", my_id);

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input leaderId `repeat_iter([(0,),])`
.input proxyLeaderStartID `repeat_iter([(proxy_leader_start_id,),])` # Assume = 0,m,2m,...,(n-1)*m, for n proxy leaders and m partitions
.input numProxyLeaderPartitions `repeat_iter([(num_proxy_leader_partitions,),])` 

# IDB

# Request (<o = command operation, t = timestamp, c = client>, sig(c) = signature of (o,t,c)).
# Simplified request (<o = command operation>, sig(c) = signature of o, d = digest of o).
.async clientIn `null::<(Rc<Vec<u8>>,)>()` `source_stream(client_recv) -> filter_map(|x: Result<(u32, BytesMut,), _>| (deserialize(x.unwrap().1, &client_requests)))`

.input startSlot `repeat_iter([(0 as u32,),])`
.input nextSlot `null::<(u32,)>()`
.input SlottedPayloads `null::<(Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>, Rc<Vec<u8>>, u32,)>()`

# Pre-prepare (v = view, n = slot, d = hash digest of m, sig(p) = signature of (v,n,d), m = <o = command operation, t = timestamp, c = client>, sig(c) = signature of (o,t,c)).
# Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <command_id, o = command operation>, sig(c) = signature of o).
.async prePrepareOut `map(|(node_id, (slot, digest, command_id, command, sigc))| (node_id, serialize_to_bytes(create_pre_prepare(slot, digest, command_id, command, sigc, f)))) -> dest_sink(pre_prepare_to_proxy_leader_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`

######################## end relation definitions

IsLeader() :- leaderId(i), id(i)
slots(s) :- startSlot(s)
nextSlot(max(s)) :- slots(s)

######################## pre-prepare
IndexedPayloads(commandID, command, sigc, digest, index()) :- clientIn(commandID, command, sigc, digest), IsLeader()
SlottedPayloads(commandID, command, sigc, digest, (slot + offset)) :- IndexedPayloads(commandID, command, sigc, digest, offset), nextSlot(slot)
prePrepareOut@(r+(slot%n))(slot, digest, commandID, command, sigc) :~ SlottedPayloads(commandID, command, sigc, digest, slot), proxyLeaderStartID(r), numProxyLeaderPartitions(n)

# Increment slot
NumPayloads(max(offset)) :- IndexedPayloads(_, _, _, _, offset)
slots(s) :+ nextSlot(s), !NumPayloads(n)
slots(s + num + 1) :+ nextSlot(s), NumPayloads(num)
######################## end pre-prepare
    "#
    );

    launch_flow(df).await;
}
