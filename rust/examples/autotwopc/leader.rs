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
pub struct LeaderArgs {
    #[clap(long, default_value = "1")]
    flush_every_n: usize,

    #[clap(long = "leader.num-vote-requester-partitions")]
    num_vote_requester_partitions: Option<u32>
}

// Returns (client_id, request_id, reply)
fn deserialize(
    (client_id, msg): (u32, BytesMut),
    client_requests: &prometheus::Counter,
) -> (u32, i64, Rc<Vec<u8>>) {
    let s = voting_proto::LeaderInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();

    match s.request.unwrap() {
        voting_proto::leader_inbound::Request::ClientRequest(r) => {
            client_requests.inc();
            let out = voting_proto::ClientReply {
                id: r.id,
                accepted: true,
                command: r.command,
            };
            let mut buf = Vec::new();
            out.encode(&mut buf).unwrap();
            return (client_id, r.id, Rc::new(buf));
        }
        _ => panic!("Unexpected message from the client"),
    }
}

pub async fn run(cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let client_requests = prometheus::register_counter!("autotwopc_requests_total", "help").unwrap();

    // Client setup
    let client_recv = ports
        .remove("receive_from$clients$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    // Vote requester setup
    let vote_to_vote_requester_sink = ports
        .remove("send_to$vote_requesters$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let num_vote_requester_partitions = i64::from(cfg.num_vote_requester_partitions.unwrap());

    let df = datalog!(
        r#"
        ######################## relation definitions
        # EDB
        .async clientIn `null::<(u32,i64,Rc<Vec<u8>>,)>()` `source_stream(client_recv) -> map(|x| deserialize(x.unwrap(), &client_requests))`
        .input numVoteRequesters `repeat_iter([(num_vote_requester_partitions,),])`

        .async voteToVoteRequester `map(|(node_id, v):(i64,(u32,i64,Rc<Vec<u8>>,))| (u32::try_from(node_id).unwrap(), serialize_to_bytes(v))) -> dest_sink(vote_to_vote_requester_sink)` `null::<(u32,i64,Rc<Vec<u8>>,)>()`
        ######################## end relation definitions
        
        # Phase 1a
        voteToVoteRequester@(id%n)(client, id, p) :~ clientIn(client, id, p), numVoteRequesters(n)
    "#
    );

    launch_flow(df).await;
}