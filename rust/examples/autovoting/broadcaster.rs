use std::collections::HashMap;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use std::convert::TryFrom;
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct BroadcasterArgs {
    #[clap(long = "broadcaster.num-replica-partitions")]
    broadcaster_num_replica_partitions: Option<u32>,
    #[clap(long = "broadcaster.num-replica-groups")]
    broadcaster_num_replica_groups: Option<u32>,
}

pub async fn run(cfg: BroadcasterArgs, mut ports: HashMap<String, ServerOrBound>) {
    let to_broadcaster_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let to_participant_sink = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let num_participant_partitions = i64::from(cfg.broadcaster_num_replica_partitions.unwrap());
    let num_participant_groups = i64::from(cfg.broadcaster_num_replica_groups.unwrap());

    let mut participant_start_ids = Vec::<i64>::new();

    for i in 0..num_participant_groups {
        participant_start_ids.push(i * num_participant_partitions);
    }

    let df = datalog!(
        r#"
.input participantStartIDs `repeat_iter(participant_start_ids.clone()) -> map(|p| (p,))` # Assume = 0,m,2m,...,(n-1)*m, for n participants and m partitions        
.input numParticipantPartitions `repeat_iter([(num_participant_partitions,),])`
.async toBroadcaster `null::<(u32,i64,Rc<Vec<u8>>)>()` `source_stream(to_broadcaster_source) -> map(|x| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>)>(x.unwrap().1).unwrap())`
.async voteToParticipants `map(|(node_id, v)| (u32::try_from(node_id).unwrap(), serialize_to_bytes(v))) -> dest_sink(to_participant_sink)` `null::<(u32,i64,Rc<Vec<u8>>)>()`  

voteToParticipants@(p+(id%m))(client, id, v) :~ toBroadcaster(client, id, v), participantStartIDs(p), numParticipantPartitions(m)
    "#
    );

    launch_flow(df).await;
}
