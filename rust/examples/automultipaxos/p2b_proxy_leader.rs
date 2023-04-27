use frankenpaxos::multipaxos_proto;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::rc::Rc;
use std::{collections::HashMap, convert::TryFrom, io::Cursor};

#[derive(clap::Args, Debug)]
pub struct P2bProxyLeaderArgs {
    #[clap(long = "p2b-proxy-leader.index")]
    p2b_proxy_leader_index: Option<u32>,

    #[clap(long = "p2b-proxy-leader.leader-index")]
    p2b_proxy_leader_leader_index: Option<u32>,

    #[clap(long = "p2b-proxy-leader.f")]
    f: Option<u32>,

    #[clap(long = "p2b-proxy-leader.num-acceptor-partitions")]
    p2b_proxy_leader_num_acceptor_partitions: Option<u32>,

    // Total number of acceptor nodes (across all partitions)
    #[clap(long = "p2b-proxy-leader.num-acceptors")]
    p2b_proxy_leader_num_acceptors: Option<u32>,
}

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

pub async fn run(cfg: P2bProxyLeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let requests = prometheus::register_counter!("automultipaxos_p2b_proxy_leader_incoming_p2b", "help").unwrap();
    let responses = prometheus::register_counter!("automultipaxos_p2b_proxy_leader_outgoing_p2b", "help").unwrap();

    let p2b_source = ports
        .remove("receive_from$acceptors$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let p2b_to_proposer_sink = ports
        .remove("send_to$leaders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let inputs_sink = ports
        .remove("send_to$leaders$1")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let my_id = cfg.p2b_proxy_leader_index.unwrap();
    let num_acceptor_partitions = cfg.p2b_proxy_leader_num_acceptor_partitions.unwrap();
    let acceptor_start_ids: Vec<u32> = (0u32..cfg.p2b_proxy_leader_num_acceptors.unwrap())
        .step_by(num_acceptor_partitions.try_into().unwrap())
        .collect();
    let proposer = cfg.p2b_proxy_leader_leader_index.unwrap();
    let f = cfg.f.unwrap();

    // Replica setup
    let replica_port = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let replicas = replica_port.keys.clone();
    let replica_send = replica_port.into_sink();

    let df = datalog!(
        r#"
        ######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input quorum `repeat_iter([(f+1,),])`
.input fullQuorum `repeat_iter([(2*f+1,),])`
.input acceptorStartIDs `repeat_iter(acceptor_start_ids.clone()) -> map(|p| (p,))` # Assume = 0,m,2m,...,(n-1)*m, for n acceptors and m partitions
.input numAcceptorGroups `repeat_iter([(num_acceptor_partitions,),])`
.input proposer `repeat_iter([(proposer,),])` # The proposer this proxy leader was decoupled from
.input tick `repeat_iter(vec![()]) -> map(|_| (context.current_tick() as u32,))`
.input replicas `repeat_iter(replicas.clone()) -> map(|p| (p,))`

.async clientOut `map(|(node_id, (payload, slot,))| (node_id, serialize(payload, slot))) -> dest_sink(replica_send)` `null::<(Rc<Vec<u8>>, u32,)>()`

# Debug
.output p2bOut `for_each(|(i,a,payload,slot,id,num,max_id,max_num):(u32,u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32,)| println!("p2bProxyLeader {:?} received p2b from acceptor: [{:?},{:?},{:?},{:?},{:?},{:?},{:?}]]", i, a, payload, slot, id, num, max_id, max_num))`
.output p2bToProposerOut `for_each(|(i,pid,max_id,max_num,t1):(u32,u32,u32,u32,u32,)| println!("p2bProxyLeader {:?} sent p2b to proposer {:?}: [{:?},{:?},{:?}]]", i, pid, max_id, max_num, t1))`
.output p2bToProposerOut `for_each(|(i,pid,n,t1,prev_t):(u32,u32,u32,u32,u32,)| println!("p2bProxyLeader {:?} sent inputs to proposer {:?}: [{:?},{:?},{:?}]]", i, pid, n, t1, prev_t))`

.output allCommit `for_each(|(_payload, _slot): (Rc<Vec<u8>>,u32)| {responses.inc(); responses.inc(); responses.inc()})`
.output p2bU `for_each(|(_,_,_,_,_,_,_): (u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32)| requests.inc())`


# p2b: acceptorID, payload, slot, ballotID, ballotNum, maxBallotID, maxBallotNum
.async p2bU `null::<(u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32)>()` `source_stream(p2b_source) -> map(|v| deserialize_from_bytes::<(u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32,)>(v.unwrap().1).unwrap())`
# p2bToProposer: maxBallotID, maxBallotNum, t1
.async p2bToProposer `map(|(node_id, v):(u32,(u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p2b_to_proposer_sink)` `null::<(u32,u32,u32)>()` 
# inputs: n, t1, prevT
.async inputs `map(|(node_id, v):(u32,(u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(inputs_sink)` `null::<(u32,u32,u32)>()` 
######################## end relation definitions


p2b(a, p, s, i, n, mi, mn) :- p2bU(a, p, s, i, n, mi, mn)
p2b(a, p, s, i, n, mi, mn) :+ p2b(a, p, s, i, n, mi, mn), !allCommit(_, s) # drop all p2bs if slot s is committed


# Debug
// p2bOut(i, a, payload, slot, id, num, maxID, maxNum) :- id(i), p2bU(a, payload, slot, id, num, maxID, maxNum)
// p2bToProposerOut(i, pid, mi, mn, t1) :- p2bNewBallot(mi, mn), tick(t1), proposer(pid), id(i)
// inputsOut(i, pid, n, t1, prevT) :- batchSize(n), tick(t1), batchTimes(prevT), proposer(pid), id(i)
// inputsOut(i, pid, n, t1, 0) :- batchSize(n), tick(t1), !batchTimes(prevT), proposer(pid), id(i)


######################## p2bs with asymmetric decoupling
p2bUniqueBallot(mi, mn) :+ p2bU(a, p, s, i, n, mi, mn)
p2bUniqueBallot(mi, mn) :+ p2bUniqueBallot(mi, mn)
p2bNewBallot(mi, mn) :- p2bU(a, p, s, i, n, mi, mn), !p2bUniqueBallot(mi, mn)
p2bToProposer@pid(mi, mn, t1) :~ p2bNewBallot(mi, mn), tick(t1), proposer(pid)
p2bUCount(count(*)) :- p2bNewBallot(mi, mn)
batchSize(n) :- p2bUCount(n)
batchTimes(t1) :+ batchSize(n), tick(t1) # Since there's only 1 r to be batched, (n != 0) is implied
batchTimes(t1) :+ !batchSize(n), batchTimes(t1) # Persist if no batch
inputs@pid(n, t1, prevT) :~ batchSize(n), tick(t1), batchTimes(prevT), proposer(pid)
inputs@pid(n, t1, 0) :~ batchSize(n), tick(t1), !batchTimes(prevT), proposer(pid)
######################## end p2bs with asymmetric decoupling


CountMatchingP2bs(payload, slot, count(acceptorID), i, num) :- p2b(acceptorID, payload, slot, i, num, payloadBallotID, payloadBallotNum)
commit(payload, slot) :- CountMatchingP2bs(payload, slot, c, i, num), quorum(size), (c >= size)
allCommit(payload, slot) :- CountMatchingP2bs(payload, slot, c, i, num), fullQuorum(c)
clientOut@r(payload, slot) :~ allCommit(payload, slot), replicas(r)
"#
    );

    launch_flow(df).await
}
