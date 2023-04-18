use hydroflow::bytes::BytesMut;
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
    #[clap(long = "acceptor.index")]
    acceptor_index: Option<u32>,
}

pub async fn run(cfg: AcceptorArgs, mut ports: HashMap<String, ServerOrBound>) {
    let p1a_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let p1b = ports
        .remove("send_to$leaders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let p1b_sink = p1b.into_sink();

    let p1b_log_sink = ports
        .remove("send_to$leaders$1")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let p2a_source = ports
        .remove("receive_from$leaders$1")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let p2b_sink = ports
        .remove("send_to$leaders$2")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let my_id: Vec<u32> = vec![cfg.acceptor_index.unwrap()];

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter(my_id.clone()) -> map(|p| (p,))`

# Debug
.output p1aOut `for_each(|(a,pid,id,num):(u32,u32,u32,u32,)| println!("acceptor {:?} received p1a: [{:?},{:?},{:?}]", a, pid, id, num))`
.output p1bOut `for_each(|(pid,a,log_size,id,num,max_id,max_num):(u32,u32,u32,u32,u32,u32,u32,)| println!("acceptor {:?} sent p1b to {:?}: [{:?},{:?},{:?},{:?},{:?},{:?}]", a, pid, a, log_size, id, num, max_id, max_num))`
.output p1bLogOut `for_each(|(pid,a,payload,slot,payload_id,payload_num,id,num):(u32,u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32,)| println!("acceptor {:?} sent p1bLog to {:?}: [{:?},{:?},{:?},{:?},{:?},{:?},{:?}]", a, pid, a, payload, slot, payload_id, payload_num, id, num))`
.output p2aOut `for_each(|(a,pid,payload,slot,id,num):(u32,u32,Rc<Vec<u8>>,u32,u32,u32,)| println!("acceptor {:?} received p2a: [{:?},{:?},{:?},{:?},{:?}]", a, pid, payload, slot, id, num))`
.output p2bOut `for_each(|(pid,a,payload,slot,id,num,max_id,max_num):(u32,u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32,)| println!("acceptor {:?} sent p2b to {:?}: [{:?},{:?},{:?},{:?},{:?},{:?},{:?}]]", a, pid, a, payload, slot, id, num, max_id, max_num))`
// .output MaxBallot `for_each(|(id,num):(u32,u32)| println!("ballot: [{:?},{:?}]", id, num))`
// .output id `for_each(|(id):(u32,)| println!("id: [{:?}]", id))`

# p1a: proposerID, ballotID, ballotNum
.async p1a `null::<(u32,u32,u32,)>()` `source_stream(p1a_source) -> map(|v: Result<(u32,BytesMut), _>| deserialize_from_bytes::<(u32,u32,u32,)>(v.unwrap().1).unwrap())`
# p1b: acceptorID, logSize, ballotID, ballotNum, maxBallotID, maxBallotNum
.async p1b `map(|(node_id, v):(u32,(u32,u32,u32,u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p1b_sink)` `null::<(u32,u32,u32,u32,u32,u32)>()`
# p1bLog: acceptorID, payload, slot, payloadBallotID, payloadBallotNum, ballotID, ballotNum
.async p1bLog `map(|(node_id, v):(u32,(u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p1b_log_sink)` `null::<(u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32)>()`
# p2a: proposerID, payload, slot, ballotID, ballotNum
.async p2a `null::<(u32,Rc<Vec<u8>>,u32,u32,u32,)>()` `source_stream(p2a_source) -> map(|v: Result<(u32, BytesMut), _>| deserialize_from_bytes::<(u32,Rc<Vec<u8>>,u32,u32,u32,)>(v.unwrap().1).unwrap())`
# p2b: acceptorID, payload, slot, ballotID, ballotNum, maxBallotID, maxBallotNum
.async p2b `map(|(node_id, v):(u32,(u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p2b_sink)` `null::<(u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32)>()`
######################## end relation definitions


ballots(id, num) :+ ballots(id, num)
.persist log
// .persist ballots
// log(payload, slot, payloadBallotID, payloadBallotNum) :+ log(payload, slot, payloadBallotID, payloadBallotNum)


# Debug
// p1aOut(a, pid, id, num) :- p1a(pid, id, num), id(a)
// p1bOut(pid, i, size, ballotID, ballotNum, maxBallotID, maxBallotNum) :- p1a(pid, ballotID, ballotNum), id(i), LogSize(size), MaxBallot(maxBallotID, maxBallotNum)
// p1bOut(pid, i, 0, ballotID, ballotNum, maxBallotID, maxBallotNum) :- p1a(pid, ballotID, ballotNum), id(i), !LogSize(size), MaxBallot(maxBallotID, maxBallotNum)
// p1bLogOut(pid, i, payload, slot, payloadBallotID, payloadBallotNum, ballotID, ballotNum) :- p1a(pid, ballotID, ballotNum), id(i), log(payload, slot, payloadBallotID, payloadBallotNum), LogEntryMaxBallot(slot, payloadBallotID, payloadBallotNum)
// p2aOut(a, pid, payload, slot, id, num) :- p2a(pid, payload, slot, id, num), id(a)
// p2bOut(pid, i, payload, slot, ballotID, ballotNum, maxBallotID, maxBallotNum) :- p2a(pid, payload, slot, ballotID, ballotNum), id(i), MaxBallot(maxBallotID, maxBallotNum)

######################## reply to p1a 
ballots(id, num) :- p1a(pid, id, num)
MaxBallotNum(max(num)) :- ballots(id, num) 
MaxBallot(max(id), num) :- MaxBallotNum(num), ballots(id, num)
LogSize(count(slot)) :- p1a(_,_,_), log(p, slot, ballotID, ballotNum)
p1b@pid(i, size, ballotID, ballotNum, maxBallotID, maxBallotNum) :~ p1a(pid, ballotID, ballotNum), LogSize(size), MaxBallot(maxBallotID, maxBallotNum), id(i)
p1b@pid(i, 0, ballotID, ballotNum, maxBallotID, maxBallotNum) :~ p1a(pid, ballotID, ballotNum), !LogSize(size), MaxBallot(maxBallotID, maxBallotNum), id(i)

LogEntryMaxBallotNum(slot, max(ballotNum)) :- p1a(_,_,_), log(p, slot, ballotID, ballotNum)
LogEntryMaxBallot(slot, max(ballotID), ballotNum) :- p1a(_,_,_), LogEntryMaxBallotNum(slot, ballotNum), log(p, slot, ballotID, ballotNum)

# send back entire log 
p1bLog@pid(i, payload, slot, payloadBallotID, payloadBallotNum, ballotID, ballotNum) :~ p1a(pid, ballotID, ballotNum), log(payload, slot, payloadBallotID, payloadBallotNum), LogEntryMaxBallot(slot, payloadBallotID, payloadBallotNum), id(i)
######################## end reply to p1a 



######################## reply to p2a
log(payload, slot, ballotID, ballotNum) :- p2a(pid, payload, slot, ballotID, ballotNum), MaxBallot(ballotID, ballotNum)
p2b@pid(i, payload, slot, ballotID, ballotNum, maxBallotID, maxBallotNum) :~ p2a(pid, payload, slot, ballotID, ballotNum), id(i), MaxBallot(maxBallotID, maxBallotNum)
######################## end reply to p2a
        "#
    );

    launch_flow(df).await;
}
