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
pub struct ParticipantVoterArgs {
    #[clap(long = "participant-voter.index")]
    participant_voter_index: Option<u32>,

    #[clap(long = "participant-voter.num-committers")]
    participant_voter_num_committers: Option<u32>,
}

pub async fn run(cfg: ParticipantVoterArgs, mut ports: HashMap<String, ServerOrBound>) {
    let vote_to_participant_source = ports
        .remove("receive_from$vote_requesters$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let vote_from_participant_sink = ports
        .remove("send_to$committers$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let my_id = cfg.participant_voter_index.unwrap();
    let num_committers = i64::from(cfg.participant_voter_num_committers.unwrap());
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
.input numCommitters `repeat_iter([(num_committers,),])`

.async voteToParticipant `null::<(u32,i64,Rc<Vec<u8>>,)>()` `source_stream(vote_to_participant_source) -> map(|x| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>,)>(x.unwrap().1).unwrap())`
.async voteFromParticipant `map(|(node_id, v)| (u32::try_from(node_id).unwrap(), serialize_to_bytes(v))) -> dest_sink(vote_from_participant_sink)` `null::<(u32,i64,Rc<Vec<u8>>,u32)>()`

.output logVote `map(|(tid,p):(i64,Rc<Vec<u8>>)| format!("tid: {:?}, p: {:?}", tid, p)) -> dest_sink(file_sink)`
# For some reason Hydroflow can't infer the type of logVoteComplete, so we define it manually:
.input logVoteComplete `null::<(u32,i64,Rc<Vec<u8>>)>()`
######################## end relation definitions

logVote(id, p) :- voteToParticipant(client, id, p)
logVoteComplete(client, id, p) :+ voteToParticipant(client, id, p)
voteFromParticipant@(id%n)(client, id, p, i) :~ logVoteComplete(client, id, p), myID(i), numCommitters(n)
    "#
    );

    launch_flow(df).await;
}
