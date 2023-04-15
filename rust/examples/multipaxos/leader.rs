use frankenpaxos::multipaxos_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::tokio_stream::wrappers::IntervalStream;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::{collections::HashMap, convert::TryFrom, io::Cursor};
use tokio::time::{interval_at, Duration, Instant};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {
    #[clap(long = "leader.flush-every-n", default_value = "1")]
    flush_every_n: usize,
    #[clap(long = "leader.index")]
    index: Option<u32>,
    #[clap(long = "leader.f")]
    f: Option<u32>,

    #[clap(long = "leader.p1a-timeout")]
    p1a_timeout: Option<u32>,
    #[clap(long = "leader.i-am-leader-resend-timeout")]
    i_am_leader_resend_timeout: Option<u32>,
    #[clap(long = "leader.i-am-leader-check-timeout")]
    i_am_leader_check_timeout: Option<u32>,
}

fn serialize_noop() -> (Vec<u8>,) {
    let s = multipaxos_proto::CommandBatchOrNoop {
        value: Some(multipaxos_proto::command_batch_or_noop::Value::Noop(
            multipaxos_proto::Noop {},
        )),
    };
    let mut buf = Vec::new();
    s.encode(&mut buf).unwrap();
    return (buf,);
}

fn deserialize(msg: BytesMut) -> Option<(Vec<u8>,)> {
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
            return Some((buf,));
        }
        _ => panic!("Unexpected message from the client"),
    }
}

fn serialize(payload: Vec<u8>, slot: u32) -> bytes::Bytes {
    let command = multipaxos_proto::CommandBatchOrNoop::decode(&mut Cursor::new(payload)).unwrap();

    let out = multipaxos_proto::ReplicaInbound {
        request: Some(multipaxos_proto::replica_inbound::Request::Chosen(
            multipaxos_proto::Chosen {
                slot: i32::try_from(slot).unwrap()-1, // Dedalus starts at slot 1
                command_batch_or_noop: command,
            },
        )),
    };
    // println!("serialize: {:?}", out.request.as_ref().unwrap());
    // let s = frankenpaxos::multipaxos_proto::ClientReply {
    //     id: v.0,
    //     accepted: true,
    // };
    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

pub async fn run(cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    // Client setup
    let client_recv = ports
        .remove("receive_from$clients$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    // Replica setup
    let replica_port = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let replicas = replica_port.keys.clone();
    println!("replicas: {:?}", replicas);
    let replica_send = replica_port.into_sink();

    // General setup
    let p1a_port = ports
        .remove("send_to$acceptors$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let acceptors = p1a_port.keys.clone();
    let p1a_sink = p1a_port.into_sink();

    let p1b_source = ports
        .remove("receive_from$acceptors$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let p1b_log_source = ports
        .remove("receive_from$acceptors$1")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let p2a_sink = ports
        .remove("send_to$acceptors$1")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let p2b_source = ports
        .remove("receive_from$acceptors$2")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let i_am_leader_port = ports
        .remove("send_to$leaders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let proposers = i_am_leader_port.keys.clone();
    println!("proposers: {:?}", proposers);

    let i_am_leader_sink = i_am_leader_port.into_sink();

    let i_am_leader_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let my_id = cfg.index.unwrap();
    let f = cfg.f.unwrap();
    let p1a_timeout = periodic(cfg.p1a_timeout.unwrap());
    let i_am_leader_resend_timeout = periodic(cfg.i_am_leader_resend_timeout.unwrap());
    let i_am_leader_check_timeout = periodic(cfg.i_am_leader_check_timeout.unwrap());

    // let p2a_sink = batched_sink(p2a_unbatched_sink, flush_every_n, Duration::from_secs(10));

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input acceptors `repeat_iter(acceptors.clone()) -> map(|p| (p,))`
.input proposers `repeat_iter(proposers.clone()) -> map(|p| (p,))`
.input replicas `repeat_iter(replicas.clone()) -> map(|p| (p,))`
.input quorum `repeat_iter([(f+1,),])`
.input fullQuorum `repeat_iter([(2*f+1,),])`
.input noop `repeat_iter([serialize_noop(),])` // FIXME: Serialize a noop

# Debug
.output p1aOut `for_each(|(a,pid,id,num):(u32,u32,u32,u32,)| println!("proposer {:?} sent p1a to {:?}: [{:?},{:?},{:?}]", pid, a, pid, id, num))`
.output p1bOut `for_each(|(pid,a,log_size,id,num,max_id,max_num):(u32,u32,u32,u32,u32,u32,u32,)| println!("proposer {:?} received p1b: [{:?},{:?},{:?},{:?},{:?},{:?}]", pid, a, log_size, id, num, max_id, max_num))`
.output p1bLogOut `for_each(|(pid,a,payload,slot,payload_id,payload_num,id,num):(u32,u32,Vec<u8>,u32,u32,u32,u32,u32,)| println!("proposer {:?} received p1bLog: [{:?},{:?},{:?},{:?},{:?},{:?},{:?}]", pid, a, payload, slot, payload_id, payload_num, id, num))`
.output p2aOut `for_each(|(a,pid,payload,slot,id,num):(u32,u32,Vec<u8>,u32,u32,u32,)| println!("proposer {:?} sent p2a to {:?}: [{:?},{:?},{:?},{:?},{:?}]", pid, a, pid, payload, slot, id, num))`
.output p2bOut `for_each(|(pid,a,payload,slot,id,num,max_id,max_num):(u32,u32,Vec<u8>,u32,u32,u32,u32,u32,)| println!("proposer {:?} received p2b: [{:?},{:?},{:?},{:?},{:?},{:?},{:?}]", pid, a, payload, slot, id, num, max_id, max_num))`
.output iAmLeaderSendOut `for_each(|(dest,pid,num):(u32,u32,u32,)| println!("proposer {:?} sent iAmLeader to {:?}: [{:?},{:?}]", pid, dest, pid, num))`
.output iAmLeaderReceiveOut `for_each(|(my_id,pid,num):(u32,u32,u32,)| println!("proposer {:?} received iAmLeader: [{:?},{:?}]", my_id, pid, num))`
.output throughputOut `for_each(|(num,):(u32,)| println!("{:?}", num))`
.output throughputOut `for_each(|(num,):(u32,)| eprintln!("total_throughput,{:?}", num))`
.output nextSlotOut `for_each(|(num,):(u32,)| eprintln!("total_sent,{:?}", num))`
.output acceptorThroughputOut `for_each(|(acceptor_id,num,):(u32,u32,)| println!("From acceptor {:?}: {:?}", acceptor_id, num))`
.output acceptorThroughputOut `for_each(|(acceptor_id,num,):(u32,u32,)| eprintln!("throughput,{:?},{:?}", acceptor_id, num))`
.output LeaderExpiredOut `for_each(|(pid,):(u32,)| println!("proposer {:?} leader expired", pid))`

# IDB
// .input clientIn `repeat_iter_external(vec![()]) -> map(|_| (context.current_tick() as u32,))`
.async clientIn `null::<(Vec<u8>,)>()` `source_stream(client_recv) -> filter_map(|x| (deserialize(x.unwrap().1)))`
// .input clientIn `repeat_iter_external(vec![()]) -> flat_map(|_| (0..3).map(|d| ((context.current_tick() * 3 + d) as u32,)))`
.output clientStdout `for_each(|(payload,slot):(Vec<u8>,u32)| println!("committed {:?}", slot))`
.async clientOut `map(|(node_id, (payload, slot,))| (node_id, serialize(payload, slot))) -> dest_sink(replica_send)` `null::<(Vec<u8>, u32,)>()`

.input startBallot `repeat_iter([(0 as u32,),])`
.input startSlot `repeat_iter([(0 as u32,),])`

# p1a: proposerID, ballotID, ballotNum
.async p1a `map(|(node_id, v):(u32,(u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p1a_sink)` `null::<(u32,u32,u32)>()`
# p1b: acceptorID, logSize, ballotID, ballotNum, maxBallotID, maxBallotNum
.async p1bU `null::<(u32,u32,u32,u32,u32,u32,)>()` `source_stream(p1b_source) -> map(|v: Result<(u32,BytesMut,), _>| deserialize_from_bytes::<(u32,u32,u32,u32,u32,u32,)>(v.unwrap().1).unwrap())`
# p1bLog: acceptorID, payload, slot, payloadBallotID, payloadBallotNum, ballotID, ballotNum
.async p1bLogU `null::<(u32,Vec<u8>,u32,u32,u32,u32,u32,)>()` `source_stream(p1b_log_source) -> map(|v: Result<(u32,BytesMut,), _>| deserialize_from_bytes::<(u32,Vec<u8>,u32,u32,u32,u32,u32,)>(v.unwrap().1).unwrap())`
# p2a: proposerID, payload, slot, ballotID, ballotNum
.async p2a `map(|(node_id, v):(u32,(u32,Vec<u8>,u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p2a_sink)` `null::<(u32,Vec<u8>,u32,u32,u32)>()`
# p2b: acceptorID, payload, slot, ballotID, ballotNum, maxBallotID, maxBallotNum
.async p2bU `null::<(u32,Vec<u8>,u32,u32,u32,u32,u32)>()` `source_stream(p2b_source) -> map(|v| deserialize_from_bytes::<(u32,Vec<u8>,u32,u32,u32,u32,u32,)>(v.unwrap().1).unwrap())`

.input p1aTimeout `source_stream(p1a_timeout) -> map(|_| () )` # periodic timer to send p1a, so proposers each send at random times to avoid contention
.input iAmLeaderResendTimeout `source_stream(i_am_leader_resend_timeout) -> map(|_| () )` # periodic timer to resend iAmLeader
.input iAmLeaderCheckTimeout `source_stream(i_am_leader_check_timeout) -> map(|_| () )` # periodic timer to check if the leader has sent a heartbeat
// .input currTime `repeat_iter(vec![()]) -> map(|_| (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as u32,))` # wall-clock time
# iAmLeader: ballotID, ballotNum. Note: this is both a source and a sink
.async iAmLeaderU `map(|(node_id, v):(u32,(u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(i_am_leader_sink)` `source_stream(i_am_leader_source) -> map(|v: Result<(u32,BytesMut), _>| deserialize_from_bytes::<(u32,u32,)>(v.unwrap().1).unwrap())`
######################## end relation definitions

# inputs that are persisted must have an alias. Format: inputU = unpersisted input.
p1b(a, l, i, n, mi, mn) :- p1bU(a, l, i, n, mi, mn)
p1b(a, l, i, n, mi, mn) :+ p1b(a, l, i, n, mi, mn)
// .persist p1b
p1bLog(a, p, s, pi, pn, i, n) :- p1bLogU(a, p, s, pi, pn, i, n)
p1bLog(a, p, s, pi, pn, i, n) :+ p1bLog(a, p, s, pi, pn, i, n) # drop all p1bLogs if slot s is committed
// .persist p1bLog
p2b(a, p, s, i, n, mi, mn) :- p2bU(a, p, s, i, n, mi, mn)
p2b(a, p, s, i, n, mi, mn) :+ p2b(a, p, s, i, n, mi, mn), !allCommit(s) # drop all p2bs if slot s is committed
receivedBallots(i, n) :+ receivedBallots(i, n)
// .persist receivedBallots
iAmLeader(i, n) :- iAmLeaderU(i, n)
iAmLeader(i, n) :+ iAmLeader(i, n), !iAmLeaderCheckTimeout() # clear iAmLeader periodically (like LRU clocks)
payloads(p) :- clientIn(p)
payloads(p) :+ payloads(p), !ChosenPayload(p)

# Initialize
ballot(zero) :- startBallot(zero)

# Debug
p1aOut(a, i, i, num) :- id(i), NewBallot(num), p1aTimeout(), LeaderExpired(), acceptors(a)
p1aOut(a, i, i, num) :- id(i), ballot(num), !NewBallot(newNum), p1aTimeout(), LeaderExpired(), acceptors(a)
p1bOut(pid, a, logSize, id, num, maxID, maxNum) :- id(pid), p1bU(a, logSize, id, num, maxID, maxNum)
p1bLogOut(pid, a, payload, slot, payloadBallotID, payloadBallotNum, id, num) :- id(pid), p1bLogU(a, payload, slot, payloadBallotID, payloadBallotNum, id, num)
p2aOut(a, i, payload, slot, i, num) :- ResentLog(payload, slot), id(i), ballot(num), acceptors(a)
p2aOut(a, i, no, slot, i, num) :- FilledHoles(no, slot), id(i), ballot(num), acceptors(a) # Weird bug where if this line is commented out, id has an error?
p2aOut(a, i, payload, slot, i, num) :- ChosenPayload(payload), nextSlot(slot), id(i), ballot(num), acceptors(a)
// p2bOut(pid, a, payload, slot, id, num, maxID, maxNum) :- id(pid), p2bU(a, payload, slot, id, num, maxID, maxNum)
// iAmLeaderSendOut(pid, i, num) :- id(i), ballot(num), IsLeader(), proposers(pid), iAmLeaderResendTimeout(), !id(pid)
// iAmLeaderReceiveOut(pid, i, num) :- id(pid), iAmLeaderU(i, num)
// LeaderExpiredOut(pid) :- id(pid), LeaderExpired()
// throughputOut(num) :- totalCommitted(num), p1aTimeout(), IsLeader()
nextSlotOut(num) :- nextSlot(num), p1aTimeout(), IsLeader()
// acceptorThroughputOut(acceptorID, num) :- totalAcceptorSentP2bs(acceptorID, num), p1aTimeout(), IsLeader()

######################## stable leader election
RelevantP1bs(acceptorID, logSize) :- p1b(acceptorID, logSize, i, num, maxID, maxNum), id(i), ballot(num)
receivedBallots(id, num) :- iAmLeader(id, num)
receivedBallots(maxBallotID, maxBallotNum) :- p1b(acceptorID, logSize, i, num, maxBallotID, maxBallotNum)
receivedBallots(maxBallotID, maxBallotNum) :- p2b(acceptorID, payload, slot, ballotID, ballotNum, maxBallotID, maxBallotNum)
MaxReceivedBallotNum(max(num)) :- receivedBallots(id, num)
MaxReceivedBallot(max(id), num) :- MaxReceivedBallotNum(num), receivedBallots(id, num)
HasLargestBallot() :- MaxReceivedBallot(maxId, maxNum), id(i), ballot(num), (num > maxNum)
HasLargestBallot() :- MaxReceivedBallot(maxId, maxNum), id(i), ballot(num), (num == maxNum), (i >= maxId)

# send heartbeat if we're the leader.
iAmLeaderU@pid(i, num) :~ id(i), ballot(num), IsLeader(), proposers(pid), iAmLeaderResendTimeout(), !id(pid) # don't send to self
LeaderExpired() :- iAmLeaderCheckTimeout(), !iAmLeader(i, n), !IsLeader()

# Resend p1a if we waited a random amount of time (timeout) AND leader heartbeat timed out. Send NewBallot if it was just triggered (ballot is updated in t+1), otherwise send ballot.
p1a@a(i, i, num) :~ id(i), NewBallot(num), p1aTimeout(), LeaderExpired(), acceptors(a)
p1a@a(i, i, num) :~ id(i), ballot(num), !NewBallot(newNum), p1aTimeout(), LeaderExpired(), acceptors(a)

# ballot = max + 1. If anothe proposer sends iAmLeader, that contains its ballot, which updates our ballot (to be even higher), so we are no longer the leader (RelevantP1bs no longer relevant)
NewBallot(maxNum + 1) :- MaxReceivedBallot(maxId, maxNum), id(i), ballot(num), (maxNum >= num), (maxId != i)
ballot(num) :+ NewBallot(num)
ballot(num) :+ ballot(num), !NewBallot(newNum)
######################## end stable leader election

######################## reconcile p1b log with local log
RelevantP1bLogs(acceptorID, payload, slot, payloadBallotID, payloadBallotNum) :- p1bLog(acceptorID, payload, slot, payloadBallotID, payloadBallotNum, i, num), id(i), ballot(num)

# cannot send new p2as until all p1b acceptor logs are PROCESSED; otherwise might miss pre-existing entry
P1bLogFromAcceptor(acceptorID, count(slot)) :- RelevantP1bLogs(acceptorID, payload, slot, payloadBallotID, payloadBallotNum)
P1bAcceptorLogReceived(acceptorID) :- P1bLogFromAcceptor(acceptorID, logSize), RelevantP1bs(acceptorID, logSize)
P1bAcceptorLogReceived(acceptorID) :- RelevantP1bs(acceptorID, logSize), (logSize == 0)
P1bNumAcceptorsLogReceived(count(acceptorID)) :- P1bAcceptorLogReceived(acceptorID)
IsLeader() :- P1bNumAcceptorsLogReceived(c), quorum(size), (c >= size), HasLargestBallot()

P1bMatchingEntry(payload, slot, count(acceptorID), payloadBallotID, payloadBallotNum) :-  RelevantP1bLogs(acceptorID, payload, slot, payloadBallotID, payloadBallotNum)
# what was committed = store in local log. Note: Don't need to worry about overwriting; it's impossible to have f+1 matching for the same slot and another payload with a higher ballot; therefore this slot must already have the same payload (maybe with a lower ballot)
CommittedLog(payload, slot) :- P1bMatchingEntry(payload, slot, c, payloadBallotID, payloadBallotNum), quorum(size), (c >= size)

# what was not committed = find max ballot, store in local log, resend
P1bLargestEntryBallotNum(slot, max(payloadBallotNum)) :- RelevantP1bLogs(acceptorID, payload, slot, payloadBallotID, payloadBallotNum)
P1bLargestEntryBallot(slot, max(payloadBallotID), payloadBallotNum) :- P1bLargestEntryBallotNum(slot, payloadBallotNum), RelevantP1bLogs(acceptorID, payload, slot, payloadBallotID, payloadBallotNum)
# makes sure that p2as cannot be sent yet; otherwise resent slots might conflict. Once p2as can be sent, a new p1b log might tell us to propose a payload for the same slot we propose (in parallel) for p2a, which violates an invariant.
ResentLog(payload, slot) :- P1bLargestEntryBallot(slot, payloadBallotID, payloadBallotNum), P1bMatchingEntry(payload, slot, c, payloadBallotID, payloadBallotNum), !CommittedLog(otherPayload, slot), IsLeader(), !nextSlot(s)
p2a@a(i, payload, slot, i, num) :~ ResentLog(payload, slot), id(i), ballot(num), acceptors(a)

# hole filling: if a slot is not in ResentEntries or proposedLog but it's smaller than max, then propose noop. Provides invariant that all holes are filled (proposed) by next timestep and we can just assign slots as current slot+1
ProposedSlots(slot) :- startSlot(slot)
ProposedSlots(slot) :- CommittedLog(payload, slot)
ProposedSlots(slot) :- ResentLog(payload, slot)
MaxProposedSlot(max(slot)) :- ProposedSlots(slot)
PrevSlots(s) :- MaxProposedSlot(maxSlot), less_than(s, maxSlot)
FilledHoles(no, s) :- noop(no), !ProposedSlots(s), PrevSlots(s), IsLeader(), !nextSlot(s2)
p2a@a(i, no, slot, i, num) :~ FilledHoles(no, slot), id(i), ballot(num), acceptors(a)

# To assign values sequential slots after reconciling p1bs, start at max+1
nextSlot(s+1) :+ IsLeader(), MaxProposedSlot(s), !nextSlot(s2)
######################## end reconcile p1b log with local log

######################## send p2as
# assign a slot
ChosenPayload(choose(payload)) :- clientIn(payload), nextSlot(s), IsLeader() # drop all payloads that we can't handle in this tick by not persisting clientIn
p2a@a(i, payload, slot, i, num) :~ ChosenPayload(payload), nextSlot(slot), id(i), ballot(num), acceptors(a)
# Increment the slot if a payload was chosen
nextSlot(s+1) :+ ChosenPayload(payload), nextSlot(s)
# Don't increment the slot if no payload was chosen, but we are still the leader
nextSlot(s) :+ !ChosenPayload(payload), nextSlot(s), IsLeader()
######################## end send p2as

######################## process p2bs
CountMatchingP2bs(payload, slot, count(acceptorID), i, num) :- p2b(acceptorID, payload, slot, i, num, payloadBallotID, payloadBallotNum)
commit(payload, slot) :- CountMatchingP2bs(payload, slot, c, i, num), quorum(size), (c >= size)
allCommit(slot) :- CountMatchingP2bs(payload, slot, c, i, num), fullQuorum(c)
// clientStdout(payload, slot) :- commit(payload, slot)
MaxCommits(max(slot)) :- commit(payload, slot)
clientOut@r(payload, slot) :~ commit(payload, slot), replicas(r)

totalCommitted(new) :+ !totalCommitted(prev), MaxCommits(new)
totalCommitted(prev) :+ totalCommitted(prev), !MaxCommits(new)
totalCommitted(new) :+ totalCommitted(prev), MaxCommits(new), (prev < new)
totalCommitted(prev) :+ totalCommitted(prev), MaxCommits(new), (prev >= new)

// acceptorSentP2bs(acceptorID, count(slot)) :- p2bU(acceptorID, payload, slot, i, num, payloadBallotID, payloadBallotNum)
// totalAcceptorSentP2bs(acceptorID, new) :+ !totalAcceptorSentP2bs(acceptorID, prev), acceptorSentP2bs(acceptorID, new)
// totalAcceptorSentP2bs(acceptorID, prev) :+ totalAcceptorSentP2bs(acceptorID, prev), !acceptorSentP2bs(acceptorID, new)
// totalAcceptorSentP2bs(acceptorID, (prev + new)) :+ totalAcceptorSentP2bs(acceptorID, prev), acceptorSentP2bs(acceptorID, new)
######################## end process p2bs
    "#
    );

    launch_flow(df).await
}

fn periodic(timeout: u32) -> IntervalStream {
    let start = Instant::now() + Duration::from_secs(timeout.into());
    IntervalStream::new(interval_at(start, Duration::from_secs(timeout.into())))
}
