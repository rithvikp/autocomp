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
pub struct ParticipantArgs {
    #[clap(long)]
    index: Option<u32>,
}

pub async fn run(cfg: ParticipantArgs, mut ports: HashMap<String, ServerOrBound>) {
    let to_participant_source = ports
        .remove("receive_from$broadcasters$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let from_participant_ports = ports
        .remove("send_to$collectors$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let collectors = from_participant_ports.keys.clone();
    let from_participant_sink = from_participant_ports.into_sink();

    let my_id = cfg.index.unwrap();
    let num_collector_partitions = i64::try_from(collectors.len()).unwrap();

    let df = datalog!(
        r#"
.input myID `repeat_iter([(my_id,),])`
.input numCollectorPartitions `repeat_iter([(num_collector_partitions,),])` # Assume id = 0,1,2...

.async voteToParticipant `null::<(u32,i64,Rc<Vec<u8>>)>()` `source_stream(to_participant_source) -> map(|x| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>,)>(x.unwrap().1).unwrap())`
.async voteFromParticipant `map(|(node_id, v)| (u32::try_from(node_id).unwrap(), serialize_to_bytes(v))) -> dest_sink(from_participant_sink)` `null::<(u32,u32,i64,Rc<Vec<u8>>,)>()`
    
// out(v%n, i, client, v) :- voteToParticipant(v, client), numCollectorPartitions(n), myID(i)
voteFromParticipant@(id%n)(i, client, id, v) :~ voteToParticipant(client, id, v), numCollectorPartitions(n), myID(i)
        "#
    );

    launch_flow(df).await;
}
