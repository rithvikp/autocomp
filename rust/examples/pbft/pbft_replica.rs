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

fn get_view_primary(view: u32) -> u32 {
    0 // NOTE: need to change once view changes implemented
}

#[derive(clap::Args, Debug)]
pub struct PBFTReplicaArgs {
    #[clap(long = "pbft_replica.index")]
    index: Option<u32>,
    #[clap(long = "pbft_replica.f")]
    f: Option<u32>,
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

fn get_replica_key(my_id: u32) -> &'static [u8] {
    match my_id {
        0 => b"replica0",
        1 => b"replica1",
        2 => b"replica2",
        3 => b"replica3",
        _ => panic!("Invalid pbft_replica index {}", my_id),
    }
}

// Output: command_id, Command, clientTimestamp, client identifier (pseudonym), client sig, digest
fn deserialize(msg: BytesMut) -> Option<(Rc<Vec<u8>>,Rc<Vec<u8>>,u32,Rc<Vec<u8>>,Rc<Vec<Vec<u8>>>,Rc<Vec<u8>>)> {
    if msg.len() == 0 {
        return None;
    }
    let s = multipaxos_proto::LeaderInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();
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

            Some((Rc::new(command_id_buf), Rc::new(command), command_id.clientId, Rc::new(command_id.clientPseudonym), Rc::new(signatures), Rc::new(digest),))
        }
        _ => panic!("Unexpected message from the client"), // TODO: Change to return None after debugging, so Byzantine clients can't crash the PBFT replica
    }
}

// Will sign seq num + digest with the replica key, then put the signature in signature0
fn serialize(digest: Rc<Vec<u8>>, command_id_buf: Rc<Vec<u8>>, command: Rc<Vec<u8>>, seq_num: u32, my_id: u32) -> bytes::Bytes {
    let seq_num_i32 = i32::try_from(seq_num).unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(get_replica_key(my_id)).unwrap();
    mac.update(&seq_num_i32.to_be_bytes());
    mac.update(digest.as_slice());
    let sigp = mac.finalize().into_bytes();

    let command_id = multipaxos_proto::CommandId::decode(&mut Cursor::new(command_id_buf.as_ref())).unwrap();

    let out = multipaxos_proto::ReplicaInbound {
        request: Some(multipaxos_proto::replica_inbound::Request::Chosen(
            multipaxos_proto::Chosen {
                seq_num: seq_num_i32,
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
    // println!("Sending to replica payload {:?} seq_num {:?}", payload[0], seq_num);

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

// piggybackedPreprepare (v = view, n = seq num, d = hash digest of m, sig = primary signature of (v, n, d), o = command operation, clientTimestamp = client timestamp, clientLocation = client, clientSig = client signature of (o, clientTimestamp, clientLocation))
fn create_piggybacked_preprepare(view: u32, seq_num: u32, digest: Rc<Vec<u8>>, command_id: Rc<Vec<u8>>, command: Rc<Vec<u8>>, client_timestamp: u32, client_location: u32, sigc: Rc<Vec<Vec<u8>>>, receiver: u32) -> (u32, u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, u32, u32, Rc<Vec<Vec<u8>>>) {
    // ASK?: is the index 0 for digest/command/sigc tied to the previous assumption that only replica 0 is the primary?
    println!("Creating piggybacked preprepare: ((v, n, d): ({:?}, {:?}, {:?}), (o, c_t, c): ({:?}, {:?}, {:?}), Receiver: {:?})", view, seq_num, digest[0], command[0], client_timestamp, client_location, sigc[0], receiver);
    let primary = get_view_primary(view);
    let mut mac = get_mac(primary, receiver);
    mac.update(&view.to_be_bytes());
    mac.update(&seq_num.to_be_bytes());
    mac.update(digest.as_slice());
    let sigp = mac.finalize().into_bytes();
    println!("Signed prePrepare: {:?}", sigp[0]);
    (view, seq_num, digest, Rc::new(sigp.to_vec()), command_id, command, client_timestamp, client_location, sigc)
}

fn unwrap_piggybacked_preprepare(msg: (u32, u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, u32, u32, Rc<Vec<Vec<u8>>>), receiver: u32) -> Option<(u32, u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, u32, u32, Rc<Vec<Vec<u8>>>)> {
    let (view, seq_num, digest, sig, command_id, command, client_timestamp, client_location, client_sig) = msg;
    println!("Unwrapping piggybacked preprepare: ((v, n, d): ({:?}, {:?}, {:?}), sig: {:?}, (o_id, o, c_t, c): ({:?}, {:?}, {:?}, {:?}), Receiver: {:?})", view, seq_num, digest[0], sig[0], command_id, command[0], client_timestamp, client_location, client_sig[0], receiver);

    // Verify the pre-prepare signature. Note: sender = 0 because only the leader (id = 0) sends pre-prepares
    let primary = get_view_primary(view);
    let mut preprepare_mac = get_mac(primary, receiver);
    preprepare_mac.update(&view.to_be_bytes());
    preprepare_mac.update(&seq_num.to_be_bytes());
    preprepare_mac.update(digest.as_slice());
    if preprepare_mac.verify_slice(sigp.as_slice()).is_err() {
        panic!("piggybacked preprepare leader signature {:?} was incorrect", sigp)
        // TODO: Change to return None after debugging, so a Byzantine primary can't crash the replica
    }
    println!("piggybacked preprepare verified: {:?}", sigp[0]);

    // Verify the digest is from the command
    let mut hasher = Sha256::new();
    hasher.update(command.as_slice());
    let result = hasher.finalize().to_vec();
    if result != digest.as_slice() {
        panic!("Client digest {:?} didn't match expected digest {:?}", digest, result)
        // TODO: Change to return None after debugging, so Byzantine clients can't crash the PBFT replica
    }
    println!("Digest {:?} matched, checking sig", digest[0]);

    // Verify the signature is signed over the digest
    let mut client_mac = Hmac::<Sha256>::new_from_slice(get_client_key(receiver)).unwrap();
    client_mac.update(digest.as_slice());
    client_mac.update(&client_timestamp.to_be_bytes());
    client_mac.update(&client_location.to_be_bytes());
    if client_mac.verify_slice(client_sig[receiver as usize].as_slice()).is_err() {
        panic!("Client signature {:?} was incorrect", client_sig[receiver as usize])
        // TODO: Change to return None after debugging, so Byzantine clients can't crash the PBFT replica
    }
    println!("Sig {:?} matched", client_sig[receiver as usize]);
    Some((view, seq_num, digest, sig, command_id, command, client_timestamp, client_location, client_sig))
}

// preprepare (v = view, n = seq num, d = hash digest of m, l = sender, sig = sender signature of (v, n, d, l)
fn create_prepare(view: u32, seq_num: u32, digest: Rc<Vec<u8>>, sender: u32, receiver: u32) -> (u32, u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>) {
    println!("Creating prepare: ((v, n, d, l): ({:?}, {:?}, {:?}), Sender: {:?}, Receiver: {:?})", view, seq_num, digest[0], sender, sender, receiver);
    let mut mac = get_mac(sender, receiver);
    mac.update(&view.to_be_bytes());
    mac.update(&seq_num.to_be_bytes());
    mac.update(digest.as_slice());
    mac.update(&sender.to_be_bytes());
    let sig = mac.finalize().into_bytes();
    println!("Signed prepare: {:?}", sig[0]);
    (view, seq_num, digest, sender, Rc::new(sig.to_vec()))
}

fn unwrap_prepare(msg: (u32, u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>), receiver: u32) -> Option<(u32, u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)> {
    let (view, seq_num, digest, sender, sig) = msg;
    println!("Unwrapping prepare: ((v, n, d, l): ({:?}, {:?}, {:?}, {:?}), sig: {:?}, Sender: {:?})", view, seq_num, digest[0], sender, sig[0], sender);
    let mut mac = get_mac(sender, receiver);
    mac.update(&view.to_be_bytes());
    mac.update(&seq_num.to_be_bytes());
    mac.update(digest.as_slice());
    mac.update(&sender.to_be_bytes());
    if mac.verify_slice(sig.as_slice()).is_err() {
        panic!("Prepare signature {:?} from {:?} was incorrect", sig, sender)
        // TODO: Change to return None after debugging, so another Byzantine replica can't crash this replica
    }
    println!("Prepare verified: {:?}", sig[0]);
    Some((view, seq_num, digest, sender, sig))
}

// preprepare (v = view, n = seq num, d = hash digest of m, l = sender, sig = sender signature of (v, n, d, l)
fn create_commit(view: u32, seq_num: u32, digest: Rc<Vec<u8>>, sender: u32, receiver: u32) -> (u32, u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>) {
    println!("Creating commit: ((v, n, d, l): ({:?}, {:?}, {:?}, {:?}), Sender: {:?}, Receiver: {:?})", view, seq_num, digest[0], sender, sender, receiver);
    let mut mac = get_mac(sender, receiver);
    mac.update(&view.to_be_bytes());
    mac.update(&seq_num.to_be_bytes());
    mac.update(digest.as_slice());
    mac.update(&sender.to_be_bytes());
    let sig = mac.finalize().into_bytes();
    println!("Signed commit: {:?}", sig[0]);
    (view, seq_num, digest, sender, Rc::new(sig.to_vec()))
}

fn unwrap_commit(msg: (u32, u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>), receiver: u32) -> Option<(u32, u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)> {
    let (view, seq_num, digest, sender, sig) = msg;
    println!("Unwrapping commit: ((v, n, d, l): ({:?}, {:?}, {:?}, {:?}), {:?}, sig: {:?}, Sender: {:?})", view, seq_num, digest[0], sender, sig[0], sender);
    let mut mac = get_mac(sender, receiver);
    mac.update(&view.to_be_bytes());
    mac.update(&seq_num.to_be_bytes());
    mac.update(digest.as_slice());
    mac.update(&sender.to_be_bytes());
    if mac.verify_slice(sig.as_slice()).is_err() {
        panic!("Commit signature {:?} from {:?} was incorrect", sig, sender)
        // TODO: Change to return None after debugging, so another Byzantine replica can't crash this replica
    }
    println!("Commit verified: {:?}", sig[0]);
    Some((view, seq_num, digest, sender, sig))
}

// Need to provide: clients, replicas, and smr (corresponding state machine replica)
pub async fn run(cfg: PBFTReplicaArgs, mut ports: HashMap<String, ServerOrBound>) {
    // Client setup
    let client_recv = ports
        .remove("receive_from$clients$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    // Replica setup
    let piggybacked_preprepare_to_pbft_replica_port = ports
        .remove("send_to$pbft_replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let pbft_replicas = piggybacked_preprepare_to_pbft_replica_port.keys.clone();
    let piggybacked_preprepare_to_pbft_replica_sink = piggybacked_preprepare_to_pbft_replica_port.into_sink();

    let piggybacked_preprepare_from_pbft_replica_source = ports
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

    let replica_send = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let my_id = cfg.index.unwrap();
    let f = cfg.f.unwrap();
    let k = cfg.k.unwrap();
    let num_replicas = cfg.num_pbft_replicas.unwrap();

    let df = datalog!(
        r#"
########## EDBs

# EDBs provided by the system
.input ZERO `repeat_iter([(0 as u32,),])`
.input replicas `repeat_iter(pbft_replicas.clone()) -> map(|p| (p,))`
.input STATE_MACHINE `repeat_iter([(my_id,),])` # the replica's own state machine, assumes same number of state machines as replicas (indexed from 0)

# EDBs created for this protocol
.input SAFETY_QUORUM `repeat_iter([(2*f+1,),])`
.input WATERMARK_WIDTH `repeat_iter([(k as u32,),])`
.input NUM_REPLICAS `repeat_iter([(num_replicas,),])`
.input INDEXED_REPLICAS `repeat_iter(pbft_replicas.clone()).enumerate().map(|(index, p)| (p, index))`

########## end EDBs



########## IDBs

# request (commandId = command id, o = command operation, clientTimestamp = timestamp, c = client, sig = client signature of o, d = digest of o)
.async requestVerifiedIn `` `source_stream(client_recv) -> filter_map(|x: Result<(u32, BytesMut,), _>| (deserialize(x.unwrap().1)))`

# piggybackedPreprepare (v = view, n = seq num, d = hash digest of m, sig = primary signature of (v, n, d), o = command operation, clientTimestamp = client timestamp, clientLocation = client, clientSig = client signature of (o, clientTimestamp, clientLocation))
.async piggybackedPreprepareVerifiedIn `` `source_stream(piggybacked_preprepare_from_pbft_replica_source) -> filter_map(|x| (unwrap_piggybacked_preprepare(deserialize_from_bytes::<(u32, u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, u32, u32, Rc<Vec<Vec<u8>>>)>(x.unwrap().1).unwrap(), my_id)))`
.async piggybackedPreprepareOut `map(|(node_id, (view, seq_num, digest, sig, command, client_timestamp, client_location, client_sig))| (node_id, serialize_to_bytes(create_piggybacked_preprepare(view, seq_num, digest, sig, command, client_timestamp, client_location, client_sig, node_id)))) -> dest_sink(piggybacked_preprepare_to_pbft_replica_sink)` ``

# prepare (v = view, n = seq num, d = hash digest of m, l' = id of sender, sig = l' signature of (v, n, d, l'))
.async prepareVerifiedIn `` `source_stream(prepare_from_pbft_replica_source) -> filter_map(|x| (unwrap_prepare(deserialize_from_bytes::<(u32, u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)>(x.unwrap().1).unwrap(), my_id)))`
.async prepareOut `map(|(node_id, (view, seq_num, digest, sender))| (node_id, serialize_to_bytes(create_prepare(view, seq_num, digest, sender, node_id)))) -> dest_sink(prepare_to_pbft_replica_sink)` ``

# commit (v = view, n = seq num, d = hash digest of m, l' = id of sender, sig = l' signature of (v, n, d, l'))
.async commitVerifiedIn `` `source_stream(commit_from_pbft_replica_source) -> filter_map(|x| (unwrap_commit(deserialize_from_bytes::<(u32, u32, Rc<Vec<u8>>, u32, Rc<Vec<u8>>)>(x.unwrap().1).unwrap(), my_id)))`
.async commitOut `map(|(node_id, (view, seq_num, digest, sender))| (node_id, serialize_to_bytes(create_commit(view, seq_num, digest, sender, node_id)))) -> dest_sink(commit_to_pbft_replica_sink)` ``

# executeCommand (d = digest of o, commandId = command id, o = command operation, n = seq num)
.async executeCommandOut `map(|(node_id, (digest, command_id, command, seq_num))| (node_id, serialize_to_bytes(serialize(digest, command_id, command, seq_num, my_id)))) -> dest_sink(replica_send)` ``

########## end IDBs



########## temporarly placeholders

currentView(v) :- ZERO(v)
viewPrimary(v, l') :- normalizedView(v, x), INDEXED_REPLICAS(l', x)
normalizedView(v, v % x) :- currentView(v), NUM_REPLICAS(x)
lowWatermark(h) :- ZERO(h)
highWatermark(h + k) :- lowWatermark(h), WATERMARK_WIDTH(k)

########## end temporarly placeholders



########## client requests

requestLog(commandId, o, clientTimestamp, l', sig, d) :+ requestVerifiedIn(commandId, o, clientTimestamp, l', sig, d), !attemptingViewChange(_)
requestLog(commandId, o, clientTimestamp, l', sig, d) :- piggybackedPreprepareLog(_, _, _, commandId, o, clientTimestamp, l', sig, d) # taken from a piggybacked preprepare
# conditional persist for garbage collection based on highest clientTimestamp replied to
requestLog(commandId, o, clientTimestamp, l', sig, d) :+ requestLog(commandId, o, clientTimestamp, l', sig, d), highestClientTimestampReplyByClient(l', maxTimestamp), clientTimestamp >= maxTimestamp # garbage collect requests with a strictly lower timestamp

########## end client requests



########## preprepare + prepare

# send preprepares for any request in the log that has no preprepare in the current view and isn't already committed
outboundPiggybackedPreprepareBatch(d) :- requestLog(commandId, o, clientTimestamp, clientLocation, clientSig, d), highestClientTimestampReplyByClient(clientLocation, maxTimestamp), clientTimestamp > maxTimestamp, currentView(v), viewPrimary(v, l), !preprepareExists(v, d), !committedLocal(d, _, _)

# ordering batches of concurrent requests to be sent as preprepares
indexedOutboundPiggybackedPreprepare(d, index()) :- outboundPiggybackedPreprepareBatch(d)

outboundPiggybackedPreprepareBatchSize(max(i) + 1) :- indexedOutboundPiggybackedPreprepare(d, i)

takenSeqNum(n) :- lowWatermark(n)
takenSeqNum(n' + x - 1) :+ nextOpenSeqNum(n'), outboundPiggybackedPreprepareBatch(_), outboundPiggybackedPreprepareBatchSize(x)
takenSeqNum(n' - 1) :+ nextOpenSeqNum(n'), !outboundPiggybackedPreprepareBatch(_)

# next open seq num increments for each outbound preprepare, defaulting to low watermark
nextOpenSeqNum(max(n) + 1) :- takenSeqNum(n)

piggybackedPreprepareOutbox(v, n + i, d, commandId, o, clientTimestamp, clientLocation, clientSig) :- requestLog(commandId, o, clientTimestamp, clientLocation, clientSig, d), outboundPiggybackedPreprepareBatch(d), indexedOutboundPiggybackedPreprepare(d, i), currentView(v), nextOpenSeqNum(n)

preprepareExists(v, d) :- preprepareLog(v, _, d, _), currentView(v)
preprepareExists(v, d) :+ prepareSent(v, _, d, _), currentView(v)
preprepareExists(v, d) :+ preprepareExists(v, d), currentView(v)

piggybackedPreprepareSent(v, n, d, commandId, o, clientTimestamp, clientLocation, clientSig) :+ piggybackedPreprepareOutbox(v, n, d, commandId, o, clientTimestamp, clientLocation, clientSig)
piggybackedPreprepareSent(v, n, d, commandId, o, clientTimestamp, clientLocation, clientSig) :+ piggybackedPreprepareSent(v, n, d, commandId, o, clientTimestamp, clientLocation, clientSig)
piggybackedPreprepareOut@l'(v, n, d, commandId, o, clientTimestamp, clientLocation) :~ piggybackedPreprepareOutbox(v, n, d, commandId, o, clientTimestamp, clientLocation, clientSig), !piggybackedPreprepareSent(v, n, d, commandId, o, clientTimestamp, clientLocation, clientSig), replicas(l')

# chooses one digest in case multiple concurrent preprepares are received
# gives priority to preprepares taken from the O set, which are added to the preprepareLog in the same timestep as accepting the new-view message
# preprepares taken in not from new-view messages have to wait a timestep to get added
onlyOneValidPiggybackedPreprepareByEmptyViewAndSeqNum(v, n, choose(d)) :+ piggybackedPreprepareVerifiedIn(v, n, d, sig, commandId, o, clientTimestamp, clientLocation, clientSig), !attemptingViewChange(_), viewPrimary(v, l'), currentView(v), !preprepareLog(v, n, _, _), lowWatermark(h), h < n, highWatermark(H), n < H

# persisted into the next t' as long as (v, n) is empty during t
piggybackedPrepreparePurgatoryLog(v, n, d, sig, commandId, o, clientTimestamp, clientLocation, clientSig, l') :+ piggybackedPreprepareVerifiedIn(v, n, d, sig, commandId, o, clientTimestamp, clientLocation, clientSig), !preprepareLog(v, n, _, _)

# logic to accept preprepare messages
piggybackedPreprepareLog(v, n, d, sig, commandId, o, clientTimestamp, clientLocation, clientSig) :- piggybackedPrepreparePurgatoryLog(v, n, d, sig, commandId, o, clientTimestamp, clientLocation, clientSig, l'), onlyOneValidPiggybackedPreprepareByViewAndSeqNum(v, n, d) # choose only one
# conditional persist for garbage collection based on low watermark
piggybackedPreprepareLog(v, n, d, sig, commandId, o, clientTimestamp, clientLocation, clientSig) :+ piggybackedPreprepareLog(v, n, d, sig, commandId, o, clientTimestamp, clientLocation, clientSig), lowWatermark(h), h < n

preprepareLog(v, n, d, sig) :- piggybackedPreprepareLog(v, n, d, sig, _, _, _, _)
# conditional persist for garbage collection based on low watermark
preprepareLog(v, n, d, sig) :+ preprepareLog(v, n, d, sig), lowWatermark(h), h < n

# send prepares after accepting a preprepare
prepareOutbox(v, n, d, l) :- preprepareLog(v, n, d, _)
prepareSent(v, n, d, l') :+ prepareOutbox(v, n, d, l)
prepareSent(v, n, d, l') :+ prepareSent(v, n, d, l)
prepareOut@l'(v, n, d, l) :~ prepareOutbox(v, n, d, l), !prepareSent(v, n, d, l), replicas(l')

# logic to accept prepare messages
prepareLog(v, n, d, l', sig) :+ prepareVerifiedIn(v, n, d, l', sig), !attemptingViewChange(_), currentView(v), lowWatermark(h), h < n, highWatermark(H), n < H
# conditional persist for garbage collection based on low watermark
prepareLog(v, n, d, l', sig) :+ prepareLog(v, n, d, l', sig), lowWatermark(h), h < n

########## end preprepare + prepare



########## message commit

prepareCertSize(v, n, d, count(l')) :- prepareLog(v, n, d, l', _)

# has a preprepare and 2f + 1 matching prepares
digestPrepared(d, v, n) :- preprepareLog(v, n, d, _), prepareCertSize(v, n, d, certSize), SAFETY_QUORUM(x), certSize >= x
# additionally has the original request logged
prepared(d, v, n) :- requestLog(commandId, o, clientTimestamp, c, clientSig, d), digestPrepared(d, v, n)

# send commits after a message satisfies prepared predicate
commitOutbox(v, n, d, l) :- prepared(d, v, n)
commitSent(v, n, d, l) :+ commitOutbox(v, n, d, l)
commitSent(v, n, d, l) :+ commitSent(v, n, d, l)
commitOut@l'(v, n, d, l) :~ commitOutbox(v, n, d, l), !commitSent(v, n, d, l), replicas(l')

# logic to accept commit messages
commitLog(v, n, d, l', sig) :+ commitVerifiedIn(v, n, d, l', sig), !attemptingViewChange(_), currentView(v), lowWatermark(h), h < n, highWatermark(H), n < H
# conditional persist for garbage collection based on low watermark
commitLog(v, n, d, l', sig) :+ commitLog(v, n, d, l', sig), lowWatermark(h), h < n

########## end message commit



########## command execution

commitCertSize(v, n, d, count(l')) :- commitLog(v, n, d, l', _)

# satisfies prepared and also has 2f + 1 commit messages
committedLocal(d, v, n) :- prepared(d, v, n), commitCertSize(v, n, d, certSize), SAFETY_QUORUM(x), certSize >= x

# execute command after it satisfies committedLocal
# assume that the state machine can execute requests sequentially based on the seq nums provided
executeCommandOutbox(d, commandId, o, n) :- committedLocal(d, _, n), requestLog(commandId, o, _, _, _, d)
executeCommandSent(d, commandId, o, n) :+ executeCommandOutbox(d, commandId, o, n)
executeCommandSent(d, commandId, o, n) :+ executeCommandSent(d, commandId, o, n)
executeCommandOut@l'(d, commandId, o, n) :~ executeCommandOutbox(d, commandId, o, n), !executeCommandSent(d, commandId, o, n), STATE_MACHINE(l')

highestClientTimestampReplyByClient(l', max(clientTimestamp)) :- executeCommandOutbox(d, commandId, _, _), requestLog(commandId, _, clientTimestamp, l', _, d)

########## end command execution
    "#
    );

    launch_flow(df).await;
}
