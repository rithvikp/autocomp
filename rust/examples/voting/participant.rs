use std::collections::HashMap;

use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;

#[derive(clap::Args, Debug)]
pub struct ParticipantArgs {
    #[clap(long)]
    index: Option<u32>,
}

pub async fn run(cfg: ParticipantArgs, mut ports: HashMap<String, ServerOrBound>) {
    let to_replica_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let from_replica_port = ports
        .remove("send_to$leaders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let peers = from_replica_port.keys.clone();
    let from_replica_sink = from_replica_port.into_sink();

    let my_id: Vec<u32> = vec![cfg.index.unwrap()];

    let df = datalog!(
        r#"
.input myID `repeat_iter_external(my_id.clone()) -> map(|p| (p,))`
.input leader `repeat_iter_external(peers.clone()) -> map(|p| (p,))`
.async voteToReplica `null::<(i64,)>()` `source_stream(to_replica_source) -> map(|x| deserialize_from_bytes::<(i64,)>(x.unwrap().1).unwrap())`
.async voteFromReplica `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(from_replica_sink)` `null::<(u32,i64,)>()`

voteFromReplica@addr(i, v) :~ voteToReplica(v), leader(addr), myID(i)
        "#
    );

    launch_flow(df).await;
}
