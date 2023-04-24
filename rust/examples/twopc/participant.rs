use std::collections::HashMap;

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
use std::{path::Path, rc::Rc};
use tokio::fs;
use tokio::fs::OpenOptions;

#[derive(clap::Args, Debug)]
pub struct ParticipantArgs {
    #[clap(long)]
    index: Option<u32>,
}

pub async fn run(cfg: ParticipantArgs, mut ports: HashMap<String, ServerOrBound>) {
    let vote_to_participant_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let vote_from_participant_port = ports
        .remove("send_to$leaders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let peers = vote_from_participant_port.keys.clone();
    let vote_from_participant_sink = vote_from_participant_port.into_sink();

    let commit_to_participant_source = ports
        .remove("receive_from$leaders$1")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let ack_from_participant_sink = ports
        .remove("send_to$leaders$1")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let my_id: u32 = cfg.index.unwrap();

    // Logging
    let log_directory = "twopc_out";
    let participant_log = my_id.to_string() + "participant.txt";

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
.input coordinator `repeat_iter(peers.clone()) -> map(|p| (p,))`

.async voteToParticipant `null::<(u32,i64,Rc<Vec<u8>>,)>()` `source_stream(vote_to_participant_source) -> map(|x| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>,)>(x.unwrap().1).unwrap())`
.async voteFromParticipant `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(vote_from_participant_sink)` `null::<(u32,i64,Rc<Vec<u8>>,u32,)>()`
.async commitToParticipant `null::<(u32,i64,Rc<Vec<u8>>,)>()` `source_stream(commit_to_participant_source) -> map(|x| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>,)>(x.unwrap().1).unwrap())`
.async ackFromParticipant `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(ack_from_participant_sink)` `null::<(u32,i64,Rc<Vec<u8>>,u32,)>()`

.output logVote `map(|(client, id,p):(u32,i64,Rc<Vec<u8>>)| format!("client {:?}, id: {:?}, p: {:?}", client, id, p)) -> dest_sink(file_sink)`
// .output logVoteComplete `for_each(|(client,id,p,):(u32,i64,Rc<Vec<u8>>,)| println!("logVoteComplete: client {:?}, id: {:?}, p: {:?}", client, id, p))`
######################## end relation definitions

logVote(client, id, p) :- voteToParticipant(client, id, p)
logVoteComplete(client, id, p) :+ voteToParticipant(client, id, p)
voteFromParticipant@addr(client, id, p, i) :~ logVoteComplete(client, id, p), coordinator(addr), myID(i)
ackFromParticipant@addr(client, id, p, i) :~ commitToParticipant(client, id, p), coordinator(addr), myID(i)
        "#
    );

    launch_flow(df).await;
}
