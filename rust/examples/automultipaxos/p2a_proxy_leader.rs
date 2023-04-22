use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use std::rc::Rc;
use std::{collections::HashMap, convert::TryFrom};

#[derive(clap::Args, Debug)]
pub struct P2aProxyLeaderArgs {
    #[clap(long = "p2a-proxy-leader.index")]
    p2a_proxy_leader_index: Option<u32>,

    #[clap(long = "p2a-proxy-leader.num-acceptor-partitions")]
    p2a_proxy_leader_num_acceptor_partitions: Option<u32>,
}

pub async fn run(cfg: P2aProxyLeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let p2a_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let p2a_port = ports
        .remove("send_to$acceptors$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let acceptors = p2a_port.keys.clone();
    let p2a_sink = p2a_port.into_sink();

    let my_id = cfg.p2a_proxy_leader_index.unwrap();
    let num_acceptor_partitions = cfg.p2a_proxy_leader_num_acceptor_partitions.unwrap();
    let acceptor_start_ids: Vec<u32> = (0u32..u32::try_from(acceptors.len()).unwrap())
        .step_by(num_acceptor_partitions.try_into().unwrap())
        .collect();

    let df = datalog!(
        r#"
        ######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input acceptorStartIDs `repeat_iter(acceptor_start_ids.clone()) -> map(|p| (p,))` # Assume = 0,m,2m,...,(n-1)*m, for n acceptors and m partitions
.input numAcceptorGroups `repeat_iter([(num_acceptor_partitions,),])` 

# Debug
.output p2aOut `for_each(|(i,pid,payload,slot,id,num):(u32,u32,Rc<Vec<u8>>,u32,u32,u32,)| println!("p2aProxyLeader {:?} received p2a from proposer: [{:?},{:?},{:?},{:?},{:?}]", i, pid, payload, slot, id, num))`
.output p2aBroadcastOut `for_each(|(i,a,pid,payload,slot,id,num):(u32,u32,u32,Rc<Vec<u8>>,u32,u32,u32)| println!("p2aProxyLeader {:?} sent p2a to acceptor {:?}: [{:?},{:?},{:?},{:?},{:?}]", i, a, pid, payload, slot, id, num))`

# p2a: proposerID, payload, slot, ballotID, ballotNum
.async p2aIn `null::<(u32,Rc<Vec<u8>>,u32,u32,u32,)>()` `source_stream(p2a_source) -> map(|v| deserialize_from_bytes::<(u32,Rc<Vec<u8>>,u32,u32,u32,)>(v.unwrap().1).unwrap())`
.async p2aBroadcast `map(|(node_id, v):(u32,(u32,Rc<Vec<u8>>,u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p2a_sink)` `null::<(u32,Rc<Vec<u8>>,u32,u32,u32)>()` 
# p2b: acceptorID, payload, slot, ballotID, ballotNum, maxBallotID, maxBallotNum
######################## end relation definitions

# Debug
// p2aOut(i, pid, payload, slot, id, num) :- p2aIn(pid, payload, slot, id, num), id(i)
// p2aBroadcastOut(i, aid+(slot%n), pid, payload, slot, id, num) :- p2aIn(pid, payload, slot, id, num), numAcceptorGroups(n), acceptorStartIDs(aid), id(i)
p2aBroadcast@(aid+(slot%n))(pid, payload, slot, id, num) :~ p2aIn(pid, payload, slot, id, num), numAcceptorGroups(n), acceptorStartIDs(aid)
"#
    );

    launch_flow(df).await
}
