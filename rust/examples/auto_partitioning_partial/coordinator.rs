use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use std::collections::HashMap;

#[derive(clap::Args, Debug)]
pub struct CoordinatorArgs {
    #[clap(long = "coordinator.index")]
    coordinator_index: Option<u32>,

    #[clap(long = "coordinator.num-replica-partitions")]
    coordinator_num_replica_partitions: Option<u32>,
}

pub async fn run(cfg: CoordinatorArgs, mut ports: HashMap<String, ServerOrBound>) {
    let from_coordinator = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let to_coordinator_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let mut replicas = from_coordinator.keys.clone();
    replicas.sort();
    let from_coordinator_sink = from_coordinator.into_sink();

    let my_id = cfg.coordinator_index.unwrap();
    let num_partitions = cfg.coordinator_num_replica_partitions.unwrap();

    let start_index = usize::try_from(my_id * num_partitions).unwrap();
    let end_index = usize::try_from((my_id + 1) * num_partitions).unwrap();
    let partitions_slice = &replicas[start_index..end_index];
    let partitions = partitions_slice.to_vec();

    let df = datalog!(
        r#"
        ######################## relation definitions
        # EDB
        .input numPartitions `repeat_iter([(num_partitions,),])`
        .input partitions `repeat_iter(partitions.clone()) -> map(|p| (p,))`
        
        # ballotVote: partitionID, ballot
        .async ballotVote `null::<(u32,u32,)>()` `source_stream(to_coordinator_source) -> map(|v| deserialize_from_bytes::<(u32,u32,)>(v.unwrap().1).unwrap())`
        # ballotCommit: order,ballot
        .async ballotCommit `map(|(node_id, v):(u32,(u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(from_coordinator_sink)` `null::<(u32,u32)>()`
        ######################## end relation definitions
        
        
        
        ballot(partition, b) :- ballotVote(partition, b)
        ballot(partition, b) :+ ballot(partition, b), !chosenBallot(b)
        
        
        numBallots(count(partition), b) :- ballot(partition, b)
        committedBallots(b) :- numBallots(n, b), numPartitions(n)
        chosenBallot(choose(b)) :- committedBallots(b)
        ballotCommit@addr(o, b) :~ chosenBallot(b), nextOrder(o), partitions(addr)
        ballotCommit@addr(0, b) :~ chosenBallot(b), !nextOrder(o), partitions(addr)
        
        
        nextOrder(1) :+ chosenBallot(_), !nextOrder(o)
        nextOrder(o) :+ !chosenBallot(_), nextOrder(o)
        nextOrder(o+1) :+ chosenBallot(_), nextOrder(o)
        "#
    );

    launch_flow(df).await;
}
