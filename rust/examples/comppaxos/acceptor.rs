use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct AcceptorArgs {
    #[clap(long = "acceptor.row")]
    acceptor_row: Option<u32>,

    #[clap(long = "acceptor.column")]
    acceptor_column: Option<u32>,

    #[clap(long = "acceptor.num-proxy-leaders")]
    acceptor_num_proxy_leaders: Option<u32>,
}

pub async fn run(cfg: AcceptorArgs, mut ports: HashMap<String, ServerOrBound>) {
    let p1a_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let p1b_sink = ports
        .remove("send_to$leaders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let p1b_log_sink = ports
        .remove("send_to$leaders$1")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let nack_sink = ports
        .remove("send_to$leaders$2")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let p2a_source = ports
        .remove("receive_from$proxy_leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let p2b_ports = ports
        .remove("send_to$proxy_leaders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;
    let p2b_sink = p2b_ports.into_sink();

    let acceptor_row = cfg.acceptor_row.unwrap();
    let acceptor_column = cfg.acceptor_column.unwrap();
    let num_proxy_leaders = cfg.acceptor_num_proxy_leaders.unwrap();

    let df = datalog!(
        r#"
    ######################## relation definitions
# EDB
.input acceptorRow `repeat_iter([(acceptor_row,),])`
.input acceptorColumn `repeat_iter([(acceptor_column,),])`
.input numProxyLeaders `repeat_iter([(num_proxy_leaders,),])`

# p1a: proposerID, ballotID, ballotNum
.async p1a `null::<(u32,u32,u32,)>()` `source_stream(p1a_source) -> map(|v| deserialize_from_bytes::<(u32,u32,u32,)>(v.unwrap().1).unwrap())`
# p1b: acceptorRow, acceptorColumn, logSize, ballotID, ballotNum, maxBallotID, maxBallotNum
.async p1b `map(|(node_id, v):(u32,(u32,u32,u32,u32,u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p1b_sink)` `null::<(u32,u32,u32,u32,u32,u32,u32)>()`
# p1bLog: acceptorRow, acceptorColumn, payload, slot, payloadBallotID, payloadBallotNum, ballotID, ballotNum
.async p1bLog `map(|(node_id, v):(u32,(u32,u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p1b_log_sink)` `null::<(u32,u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32)>()`
# p2a: proposerID, payload, slot, ballotID, ballotNum
.async p2a `null::<(u32,Rc<Vec<u8>>,u32,u32,u32,)>()` `source_stream(p2a_source) -> map(|v| deserialize_from_bytes::<(u32,Rc<Vec<u8>>,u32,u32,u32,)>(v.unwrap().1).unwrap())`
# p2b: acceptorRow, acceptorColumn, payload, slot, ballotID, ballotNum, maxBallotID, maxBallotNum
.async p2b `map(|(node_id, v):(u32,(u32,u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p2b_sink)` `null::<(u32,u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32)>()`
# nack: ballotID, ballotNum
.async nack `map(|(node_id, v):(u32,(u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(nack_sink)` `null::<(u32,u32)>()`
######################## end relation definitions


// .persist ballots
ballots(id, num) :+ ballots(id, num)
.persist log


######################## reply to p1a 
ballots(id, num) :- p1a(pid, id, num)
MaxBallotNum(max(num)) :- ballots(id, num) 
MaxBallot(max(id), num) :- MaxBallotNum(num), ballots(id, num)
LogSize(count(slot)) :- log(p, slot, ballotID, ballotNum), p1a(_, _, _)
p1b@pid(ar, ac, size, ballotID, ballotNum, maxBallotID, maxBallotNum) :~ p1a(pid, ballotID, ballotNum), acceptorRow(ar), acceptorColumn(ac), LogSize(size), MaxBallot(maxBallotID, maxBallotNum)
p1b@pid(ar, ac, 0, ballotID, ballotNum, maxBallotID, maxBallotNum) :~ p1a(pid, ballotID, ballotNum), acceptorRow(ar), acceptorColumn(ac), !LogSize(size), MaxBallot(maxBallotID, maxBallotNum)

LogEntryMaxBallotNum(slot, max(ballotNum)) :- log(p, slot, ballotID, ballotNum), p1a(_, _, _)
LogEntryMaxBallot(slot, max(ballotID), ballotNum) :- LogEntryMaxBallotNum(slot, ballotNum), log(p, slot, ballotID, ballotNum), p1a(_, _, _)

# send back entire log 
p1bLog@pid(ar, ac, payload, slot, payloadBallotID, payloadBallotNum, ballotID, ballotNum) :~ p1a(pid, ballotID, ballotNum), acceptorRow(ar), acceptorColumn(ac), log(payload, slot, payloadBallotID, payloadBallotNum), LogEntryMaxBallot(slot, payloadBallotID, payloadBallotNum)
######################## end reply to p1a 



######################## reply to p2a
log(payload, slot, ballotID, ballotNum) :- p2a(pid, payload, slot, ballotID, ballotNum), MaxBallot(ballotID, ballotNum)
p2b@(slot%n)(ar, ac, payload, slot, ballotID, ballotNum, maxBallotID, maxBallotNum) :~ p2a(pid, payload, slot, ballotID, ballotNum), acceptorRow(ar), acceptorColumn(ac), MaxBallot(maxBallotID, maxBallotNum), numProxyLeaders(n)
nack@pid(ballotID, ballotNum) :~ p2a(pid, payload, slot, ballotID, ballotNum), MaxBallot(maxBallotID, maxBallotNum), (ballotNum < maxBallotNum)
nack@pid(ballotID, ballotNum) :~ p2a(pid, payload, slot, ballotID, ballotNum), MaxBallot(maxBallotID, maxBallotNum), (ballotNum == maxBallotNum), (ballotID < maxBallotID)
######################## end reply to p2a
    "#
    );

    launch_flow(df).await
}
