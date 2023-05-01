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
pub struct CommitterArgs {
    #[clap(long = "committer.num-participants")]
    committer_num_participants: Option<u32>,

    #[clap(long = "committer.num-participant-acker-partitions")]
    committer_num_participant_acker_partitions: Option<u32>,
}

pub async fn run(cfg: CommitterArgs, mut ports: HashMap<String, ServerOrBound>) {
    let vote_from_participant_source = ports
        .remove("receive_from$participant_voters$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let commit_to_participant = ports
        .remove("send_to$participant_ackers$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let peers = commit_to_participant.keys.clone();
    let peers_formatted = format!("{:?}", peers);
    // println!("peers: {:?}", peers);
    let commit_to_participant_sink = commit_to_participant.into_sink();

    let num_participants = i64::from(cfg.committer_num_participants.unwrap());
    let num_participant_acker_partitions =
        i64::from(cfg.committer_num_participant_acker_partitions.unwrap());
    let log_directory = "autotwopc_out";
    let coordinator_log = "coordinator.txt";

    // logging
    fs::create_dir_all(log_directory.clone()).await.unwrap();
    let path = Path::new(".").join(log_directory).join(coordinator_log);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .unwrap();
    file.set_len(0).await.unwrap();
    let file_sink = Framed::new(file, LinesCodec::new());

    let mut participant_acker_start_ids = Vec::<i64>::new();
    for i in 0..num_participants {
        participant_acker_start_ids.push(i * num_participant_acker_partitions);
    }

    let df = datalog!(
        r#"
        ######################## relation definitions
# EDB
.input numParticipants `repeat_iter([(num_participants,),])`
.input numParticipantACKers `repeat_iter([(num_participant_acker_partitions,),])`
.input participantACKerStartIDs `repeat_iter(participant_acker_start_ids.clone()) -> map(|p| (p,))` # Assume = 0,n,2n,...,n*m, for n participants and m acker partitions

.async voteFromParticipant `null::<(u32,i64,Rc<Vec<u8>>,u32)>()` `source_stream(vote_from_participant_source) -> map(|v| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>,u32)>(v.unwrap().1).unwrap())`
.async commitToParticipant `map(|(node_id, v):(i64,(u32,i64,Rc<Vec<u8>>))| (u32::try_from(node_id).unwrap(), serialize_to_bytes(v))) -> dest_sink(commit_to_participant_sink)` `null::<(u32,i64,Rc<Vec<u8>>)>()`

.output logCommit `map(|(client, id, p):(u32,i64,Rc<Vec<u8>>)| format!("client {:?}, id: {:?}, p: {:?}, participants: {:?}", client, id, p, peers_formatted)) -> dest_sink(file_sink)`
# For some reason Hydroflow can't infer the type of logCommitComplete, so we define it manually:
.input logCommitComplete `null::<(u32,i64,Rc<Vec<u8>>)>()`
######################## end relation definitions

# Phase 1b, Phase 2a
AllVotes(client, id, payload, src) :+ AllVotes(client, id, payload, src), !committed(client, id, _)
AllVotes(client, id, payload, src) :- voteFromParticipant(client, id, payload, src)

NumYesVotes(client, id, count(src)) :- AllVotes(client, id, payload, src)
committed(client, id, payload) :- NumYesVotes(client, id, num), AllVotes(client, id, payload, src), numParticipants(num) 
logCommit(client, id, payload) :- committed(client, id, payload)
logCommitComplete(client, id, payload) :+ committed(client, id, payload)
commitToParticipant@(s+(id%n))(client, id, payload) :~ logCommitComplete(client, id, payload), numParticipantACKers(n), participantACKerStartIDs(s) 
    "#
    );

    launch_flow(df).await;
}
