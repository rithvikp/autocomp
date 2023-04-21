use frankenpaxos::multipaxos_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::tokio_stream::wrappers::IntervalStream;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::rc::Rc;
use std::{collections::HashMap, convert::TryFrom, io::Cursor};

#[derive(clap::Args, Debug)]
pub struct AcceptorArgs {
    #[clap(long = "acceptor.index")]
    acceptor_index: Option<u32>,

    #[clap(long = "acceptor.coordinator_index")]
    coordinator_index: Option<u32>,
}

pub async fn run(cfg: AcceptorArgs, mut ports: HashMap<String, ServerOrBound>) {
    let df = datalog!(
        r#"
        "#
    );

    launch_flow(df).await
}
