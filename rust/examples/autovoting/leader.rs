use frankenpaxos::voting_proto;
use hydroflow::util::{
    batched_sink,
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::{collections::HashMap, convert::TryFrom, io::Cursor, time::Duration};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {
    #[clap(long, default_value = "1")]
    flush_every_n: usize,
}

fn deserialize(msg: impl AsRef<[u8]>, vote_requests: &prometheus::Counter) -> (i64,) {
    let s = voting_proto::LeaderInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();

    match s.request.unwrap() {
        voting_proto::leader_inbound::Request::ClientRequest(r) => {
            vote_requests.inc();
            return (r.id,);
        }
        _ => panic!("Unexpected message from the client"),
    }
}

pub async fn run(cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let vote_requests = prometheus::register_counter!("voting_requests_total", "help").unwrap();

    // Client setup
    let client_recv = ports
        .remove("receive_from$clients$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    // Broadcaster setup
    let to_broadcaster_port = ports
        .remove("send_to$broadcasters$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let broadcasters = to_broadcaster_port.keys.clone();
    let to_broadcaster_unbatched_sink = to_broadcaster_port.into_sink();
    let to_broadcaster_sink = batched_sink(
        to_broadcaster_unbatched_sink,
        cfg.flush_every_n,
        Duration::from_secs(10),
    );

    let num_broadcaster_partitions: Vec<i64> = vec![i64::try_from(broadcasters.len()).unwrap()];

    let df = datalog!(
        r#"
.async clientIn `null::<(i64,)>()` `source_stream(client_recv) -> map(|x| deserialize(x.unwrap().1, &vote_requests))`
.input numBroadcasterPartitions `repeat_iter(num_broadcaster_partitions.clone()) -> map(|p| (p,))`

.async toBroadcaster `map(|(node_id, v)| (u32::try_from(node_id).unwrap(), serialize_to_bytes(v))) -> dest_sink(to_broadcaster_sink)` `null::<(i64,)>()`

        
toBroadcaster@(v%n)(v) :~ clientIn(v), numBroadcasterPartitions(n)
    "#
    );

    launch_flow(df).await;
}
