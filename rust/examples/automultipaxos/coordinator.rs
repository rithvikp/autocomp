use hydroflow::bytes::BytesMut;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use std::{collections::HashMap, convert::TryFrom};

#[derive(clap::Args, Debug)]
pub struct CoordinatorArgs {
    #[clap(long = "coordinator.index")]
    coordinator_index: Option<u32>,

    #[clap(long = "coordinator.num_acceptor_partitions")]
    coordinator_num_acceptor_partitions: Option<u32>,
}

pub async fn run(cfg: CoordinatorArgs, mut ports: HashMap<String, ServerOrBound>) {
    let mut ports = hydroflow::util::cli::init().await;

    let p1a_vote_source = ports
        .remove("p1a_vote")
        .unwrap()
        .connect::<ConnectedBidi>()
        .await
        .into_source();

    let p1a_commit_port = ports
        .remove("p1a_commit")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let mut acceptors = p1a_commit_port.keys.clone();
    acceptors.sort();
    println!("[coordinator] acceptors: {:?}", acceptors);
    let p1a_commit_sink = p1a_commit_port.into_sink();

    let my_id = cfg.coordinator_index.unwrap();
    let num_acceptor_partitions = cfg.coordinator_num_acceptor_partitions.unwrap();

    let start_index = usize::try_from(my_id * num_acceptor_partitions).unwrap();
    let end_index = usize::try_from((my_id + 1) * num_acceptor_partitions).unwrap();
    let acceptor_partitions_slice = &acceptors[start_index..end_index];
    let acceptor_partitions = acceptor_partitions_slice.to_vec();

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input numAcceptorGroups `repeat_iter([(num_acceptor_partitions,),])`
.input acceptorPartitions `repeat_iter(acceptor_partitions.clone()) -> map(|p| (p,))`

# Debug
.output p1aVoteOut `for_each(|(i, aid, pid, ballot_id, ballot_num):(u32,u32,u32,u32,u32,)| println!("coordinator {:?} received p1aVote from acceptor: [{:?},{:?},{:?},{:?}]]", i, aid, pid, ballot_id, ballot_num))`
.output p1aCommitOut `for_each(|(i, aid, o, pid, ballot_id, ballot_num):(u32,u32,u32,u32,u32,u32,)| println!("coordinator {:?} sent p1aVote to acceptor {:?}: [{:?},{:?},{:?},{:?}]]", i, aid, o, pid, ballot_id, ballot_num))`

# p1aVote: acceptorPartitionID, proposerID, ballotID, ballotNum
.async p1aVoteU `null::<(u32,u32,u32,u32,)>()` `source_stream(p1a_vote_source) -> map(|v: Result<BytesMut, _>| deserialize_from_bytes::<(u32,u32,u32,u32,)>(v.unwrap()).unwrap())`
# p1aCommit: order, proposerID, ballotID, ballotNum
.async p1aCommit `map(|(node_id, v):(u32,(u32,u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p1a_commit_sink)` `null::<(u32,u32,u32,u32)>()`
######################## end relation definitions


# Debug
// p1aVoteOut(i, aid, pid, ballotID, ballotNum) :- p1aVoteU(aid, pid, ballotID, ballotNum), id(i)
// p1aCommitOut(i, a, o, pid, ballotID, ballotNum) :- chosenP1aVote(pid, ballotID, ballotNum), nextOrder(o), acceptorPartitions(a), id(i)
// p1aCommitOut(i, a, 0, pid, ballotID, ballotNum) :- chosenP1aVote(pid, ballotID, ballotNum), !nextOrder(o), acceptorPartitions(a), id(i)


p1aVote(aid, pid, i, n) :- p1aVoteU(aid, pid, i, n)
p1aVote(aid, pid, i, n) :+ p1aVote(aid, pid, i, n), !chosenP1aVote(pid, i, n)


numP1aVotes(count(aid), pid, i, n) :- p1aVote(aid, pid, i, n)
committedP1aVotes(pid, i, n) :- numP1aVotes(c, pid, i, n), numAcceptorGroups(c)
chosenP1aVote(choose(pid), choose(i), choose(n)) :- committedP1aVotes(pid, i, n)
p1aCommit@a(o, pid, i, n) :~ chosenP1aVote(pid, i, n), nextOrder(o), acceptorPartitions(a)
p1aCommit@a(0, pid, i, n) :~ chosenP1aVote(pid, i, n), !nextOrder(o), acceptorPartitions(a)


nextOrder(1) :+ chosenP1aVote(_, _, _), !nextOrder(o)
nextOrder(o) :+ !chosenP1aVote(_, _, _), nextOrder(o)
nextOrder(o+1) :+ chosenP1aVote(_, _, _), nextOrder(o)
"#
    );

    launch_flow(df).await
}
