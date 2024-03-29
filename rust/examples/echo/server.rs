use hydroflow::util::cli::{
    launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource, ConnectedTagged,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::io::Cursor;

#[derive(clap::Args, Debug)]
pub struct ServerArgs {
    #[clap(long)]
    persist_log: bool,
}

fn deserialize(msg: impl AsRef<[u8]>) -> (i64,) {
    let s =
        frankenpaxos::echo_proto::BenchmarkServerInbound::decode(&mut Cursor::new(msg.as_ref()))
            .unwrap();
    return (s.id,);
}

fn serialize(v: (i64,)) -> bytes::Bytes {
    let s = frankenpaxos::echo_proto::BenchmarkClientInbound { id: v.0 };
    let mut buf = Vec::new();
    s.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

pub async fn run_server(cfg: ServerArgs) {
    let mut ports = hydroflow::util::cli::init().await;
    let client_recv = ports
        .remove("receive_from$clients$0")
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

    if cfg.persist_log {
        // TODO: Do something
    }

    let df = datalog!(
        r#"
        .async client_in `null::<(i64,)>()` `source_stream(client_recv) -> map(|x| deserialize(x.unwrap().1))`
        .async client_out `map(|(node_id, id)| (node_id, serialize(id))) -> dest_sink(client_send)` `null::<(u32, i64,)>()`

        client_out@0(x) :~ client_in(x)
    "#
    );

    launch_flow(df).await;
}
