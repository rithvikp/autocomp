use frankenpaxos::voting_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::{
    util::{
        cli::{
            launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
            ConnectedTagged, ServerOrBound,
        },
        deserialize_from_bytes, serialize_to_bytes,
    },
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::{collections::HashMap, io::Cursor, rc::Rc};

#[derive(clap::Args, Debug)]
pub struct VoteRequesterArgs {
    #[clap(long = "vote-requester.num-participants")]
    vote_requester_num_participants: Option<u32>,

    #[clap(long = "vote-requester.num-participant-voter-partitions")]
    vote_requester_num_participant_voter_partitions: Option<u32>
}

pub async fn run(cfg: VoteRequesterArgs, mut ports: HashMap<String, ServerOrBound>) {

    let vote_to_vote_requester_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let vote_to_participant_sink = ports
        .remove("send_to$participant_voters$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let num_participants = i64::from(cfg.vote_requester_num_participants.unwrap());
    let num_participant_voter_partitions = i64::from(cfg.vote_requester_num_participant_voter_partitions.unwrap());
    
    let mut participant_voter_start_ids = Vec::<i64>::new();
    for i in 0..num_participants {
        participant_voter_start_ids.push(i * num_participant_voter_partitions);
    }

    let df = datalog!(
        r#"
        ######################## relation definitions
        # EDB
        .input numParticipantVoters `repeat_iter([(num_participant_voter_partitions,),])` 
        .input participantVoterStartIDs `repeat_iter(participant_voter_start_ids.clone()) -> map(|p| (p,))` # Assume = 0,n,2n,...,n*m, for n participants and m voter partitions
        
        .async voteToVoteRequester `null::<(u32,i64,Rc<Vec<u8>>,)>()` `source_stream(vote_to_vote_requester_source) -> map(|v| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>,)>(v.unwrap().1).unwrap())`
        .async voteToParticipant `map(|(node_id, v):(i64,(u32,i64,Rc<Vec<u8>>,))| (u32::try_from(node_id).unwrap(), serialize_to_bytes(v))) -> dest_sink(vote_to_participant_sink)` `null::<(u32,i64,Rc<Vec<u8>>,)>()`
        ######################## end relation definitions
        
        voteToParticipant@(s+(id%n))(client, id, p) :~ voteToVoteRequester(client, id, p), numParticipantVoters(n), participantVoterStartIDs(s)
    "#
    );

    launch_flow(df).await;
}