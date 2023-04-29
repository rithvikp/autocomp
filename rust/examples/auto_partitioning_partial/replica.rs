use std::collections::HashMap;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct ReplicaArgs {
    #[clap(long)]
    index: Option<u32>,

    #[clap(long = "replica.partition-index")]
    replica_partition_index: Option<u32>,
}

pub async fn run(cfg: ReplicaArgs, mut ports: HashMap<String, ServerOrBound>) {
    let to_replica_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let ballot_to_replica_source = ports
        .remove("receive_from$leaders$1")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let from_replica_port = ports
        .remove("send_to$leaders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let peers = from_replica_port.keys.clone();
    let from_replica_sink = from_replica_port.into_sink();

    let from_coordinator_source = ports
        .remove("receive_from$coordinators$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let to_coordinator = ports
        .remove("send_to$coordinators$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;
    let coordinator = to_coordinator.keys.clone();
    let to_coordinator_sink = to_coordinator.into_sink();

    let my_id: Vec<u32> = vec![cfg.index.unwrap()];
    let partition_id = cfg.replica_partition_index.unwrap();

    let df = datalog!(
        r#"
        .input myID `repeat_iter(my_id.clone()) -> map(|p| (p,))`
        .input partitionID `repeat_iter([(partition_id,),])` # ID scheme: Assuming n partitions. Acceptor i has partitions from i*n to (i+1)*n-1.
        .input leader `repeat_iter(peers.clone()) -> map(|p| (p,))`
        .input coordinator `repeat_iter(coordinator.clone()) -> map(|p| (p,))`
        
        .async ballotToReplicaU `null::<(u32,)>()` `source_stream(ballot_to_replica_source) -> map(|x| deserialize_from_bytes::<(u32,)>(x.unwrap().1).unwrap())`
        .async voteToReplicaU `null::<(u32,i64,u32,Rc<Vec<u8>>,)>()` `source_stream(to_replica_source) -> map(|x| deserialize_from_bytes::<(u32,i64,u32,Rc<Vec<u8>>,)>(x.unwrap().1).unwrap())`
        .async voteFromReplica `map(|(node_id, v): (u32, (u32,u32,i64,u32,Rc<Vec<u8>>,))| (node_id, serialize_to_bytes(v))) -> dest_sink(from_replica_sink)` `null::<(u32,u32,i64,u32,Rc<Vec<u8>>,)>()`
        .async fromCoordinatorU `null::<(u32,u32)>()` `source_stream(from_coordinator_source) -> map(|x| deserialize_from_bytes::<(u32,u32,)>(x.unwrap().1).unwrap())`
        .async toCoordinator `map(|(node_id, v): (u32, (u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(to_coordinator_sink)` `null::<(u32,u32)>()`
        
        ballots(b) :+ ballots(b)
        ballots(b) :- ballotToReplicaSealed(o, b)
        MaxBallot(max(b)) :- ballots(b)
                    
        .persist storage
        storage(v) :- voteToReplicaSealed(client, id, b, v) 
        voteFromReplica@addr(i, client, id, b, v) :~ voteToReplicaSealed(client, id, _, v), MaxBallot(b), leader(addr), myID(i)
        
        // ######################## partial partitioning
        processedI(o) :+ maxProcessedI(o)
        maxProcessedI(max(o)) :- processedI(o)
        maxReceivedI(max(o)) :- receivedI(o)
        unfreeze() :- maxReceivedI(o), maxProcessedI(o), !outstandingVote()
        unfreeze() :- !ballotToReplica(b), partitionID(p) # Include partitionID(p) so body includes positive terms
        
        ballotToReplica(b) :- ballotToReplicaU(b)
        ballotToReplica(b) :+ ballotToReplica(b), !fromCoordinatorU(o, b)
        toCoordinator@c(p, b) :~ ballotToReplicaU(b), partitionID(p), coordinator(c)
        
        ballotCommit(o, b) :- fromCoordinatorU(o, b)
        ballotCommit(o, b) :+ ballotCommit(o, b), !ballotToReplicaSealed(o, b) 
        receivedI(o) :- ballotCommit(o, b)
        nextToProcess(o+1) :- maxProcessedI(o)
        ballotToReplicaSealed(o, b) :- nextToProcess(o), ballotCommit(o, b)
        ballotToReplicaSealed(o, b) :- !nextToProcess(o2), ballotCommit(o, b), (o == 0)
        processedI(o) :+ ballotToReplicaSealed(o, b)
        outstandingVote() :- ballotToReplica(b), !ballotCommit(o, b)
        
        voteToReplica(client, id, b, v) :- voteToReplicaU(client, id, b, v)
        voteToReplica(client, id, b, v) :+ voteToReplica(client, id, b, v), !unfreeze()
        voteToReplicaSealed(client, id, b, v) :- voteToReplica(client, id, b, v), unfreeze()
        ######################## end partial partitioning
        "#
    );

    launch_flow(df).await;
}
