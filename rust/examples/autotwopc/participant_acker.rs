use hydroflow::{
    tokio_util::codec::{Framed, LinesCodec},
    util::{
        cli::{
            launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
            ConnectedTagged, ServerOrBound,
        },
        deserialize_from_bytes, serialize_to_bytes,
    },
};
use hydroflow_datalog::datalog;
use std::{collections::HashMap, path::Path, rc::Rc};
use tokio::fs;
use tokio::fs::OpenOptions;

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

    let ack_from_participant = ports
        .remove("send_to$enders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let peers = ack_from_participant.keys.clone();
    let peers_formatted = format!("{:?}", peers);
    let ack_from_participant_sink = ack_from_participant.into_sink();

    let my_id = cfg.participant_acker_index.unwrap();
    let num_enders = i64::from(cfg.participant_acker_num_enders.unwrap());
    let log_directory = "autotwopc_out";
    let participant_log = my_id.to_string() + "participant.txt";

    // logging
    fs::create_dir_all(log_directory.clone()).await.unwrap();
    let path = Path::new(".").join(log_directory).join(participant_log);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .unwrap();
    file.set_len(0).await.unwrap();
    let file_sink = Framed::new(file, LinesCodec::new());

    let df = datalog!(
        r#"
        ######################## relation definitions
# EDB
.input myID `repeat_iter([(my_id,),])`
.input numEnders `repeat_iter([(num_enders,),])`

.async commitToParticipant `null::<(u32,i64,Rc<Vec<u8>>)>()` `source_stream(commit_to_participant_source) -> map(|x| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>)>(x.unwrap().1).unwrap())`
.async ackFromParticipant `map(|(node_id, v)| (u32::try_from(node_id).unwrap(), serialize_to_bytes(v))) -> dest_sink(ack_from_participant_sink)` `null::<(u32,i64,Rc<Vec<u8>>,u32)>()`

.output logVote `map(|(client, id,p):(u32,i64,Rc<Vec<u8>>)| format!("client {:?}, id: {:?}, p: {:?}, coordinator: {:?}", client, id, p, peers_formatted)) -> dest_sink(file_sink)`
# For some reason Hydroflow can't infer the type of logVoteComplete, so we define it manually:
.input logCommitComplete `null::<(u32,i64,Rc<Vec<u8>>)>()`
######################## end relation definitions

logVote(client, id, p) :- commitToParticipant(client, id, p)
logCommitComplete(client, id, p) :+ commitToParticipant(client, id, p)
ackFromParticipant@(id%n)(client, id, p, i) :~ logCommitComplete(client, id, p), myID(i), numEnders(n)
    "#
    );

    launch_flow(df).await;
}
