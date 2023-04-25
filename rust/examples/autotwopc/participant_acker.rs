use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use std::{collections::HashMap, rc::Rc};

#[derive(clap::Args, Debug)]
pub struct ParticipantAckerArgs {
    #[clap(long = "participant-acker.index")]
    participant_acker_index: Option<u32>,

    #[clap(long = "participant-acker.num-enders")]
    participant_acker_num_enders: Option<u32>,
}

pub async fn run(cfg: ParticipantAckerArgs, mut ports: HashMap<String, ServerOrBound>) {
    let commit_to_participant_source = ports
        .remove("receive_from$committers$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let ack_from_participant_sink = ports
        .remove("send_to$enders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let my_id = cfg.participant_acker_index.unwrap();
    let num_enders = i64::from(cfg.participant_acker_num_enders.unwrap());

    let df = datalog!(
        r#"
        ######################## relation definitions
# EDB
.input myID `repeat_iter([(my_id,),])`
.input numEnders `repeat_iter([(num_enders,),])`

.async commitToParticipant `null::<(u32,i64,Rc<Vec<u8>>)>()` `source_stream(commit_to_participant_source) -> map(|x| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>)>(x.unwrap().1).unwrap())`
.async ackFromParticipant `map(|(node_id, v)| (u32::try_from(node_id).unwrap(), serialize_to_bytes(v))) -> dest_sink(ack_from_participant_sink)` `null::<(u32,i64,Rc<Vec<u8>>,u32)>()`

######################## end relation definitions

ackFromParticipant@(id%n)(client, id, p, i) :~ commitToParticipant(client, id, p), myID(i), numEnders(n)
    "#
    );

    launch_flow(df).await;
}
