use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes,
};
use hydroflow_datalog::datalog;
use std::{collections::HashMap, rc::Rc};

#[derive(clap::Args, Debug)]
pub struct EnderArgs {
    #[clap(long = "ender.num-participants")]
    ender_num_participants: Option<u32>,
}

fn serialize(payload: Rc<Vec<u8>>) -> bytes::Bytes {
    // TODO: Remove this copy if possible
    return bytes::Bytes::from(payload.as_ref().to_vec());
}

pub async fn run(cfg: EnderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let ack_from_participant_source = ports
        .remove("receive_from$participant_ackers$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let client_send = ports
        .remove("send_to$clients$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let num_participants = i64::from(cfg.ender_num_participants.unwrap());

    let df = datalog!(
        r#"
        ######################## relation definitions
# EDB
.input numParticipants `repeat_iter([(num_participants,),])`

.async ackFromParticipant `null::<(u32,i64,Rc<Vec<u8>>,u32)>()` `source_stream(ack_from_participant_source) -> map(|v| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>,u32)>(v.unwrap().1).unwrap())`
.async clientOut `map(|(node_id, (v,)):(u32,(Rc<Vec<u8>>,))| (node_id, serialize(v))) -> dest_sink(client_send)` `null::<(Rc<Vec<u8>>,)>()`
######################## end relation definitions

# Phase 2b
AllAcks(client, id, payload, src) :+ AllAcks(client, id, payload, src), !completed(client, id, _)
AllAcks(client, id, payload, src) :- ackFromParticipant(client, id, payload, src)

NumAcks(client, id, count(src)) :- AllAcks(client, id, payload, src)
completed(client, id, payload) :- NumAcks(client, id, num), AllAcks(client, id, payload, src), numParticipants(num)
clientOut@client(payload) :~ completed(client, id, payload)
    "#
    );

    launch_flow(df).await;
}
