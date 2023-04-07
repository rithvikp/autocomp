use hydroflow::util::cli::{ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource};
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
        .remove("receive_from$clients")
        .unwrap()
        .connect::<ConnectedBidi>()
        .await
        .into_source();

    let client_send = ports
        .remove("receive_from$clients")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    if cfg.persist_log {
        // TODO: Do something
    }

    let mut df = datalog!(
        r#"
        .async client_in `null::<(i64,)>()` `source_stream(client_recv) -> map(|x| deserialize(x.unwrap()))`

        .async client_out `map(|(node_id, id)| (node_id, serialize(id))) -> dest_sink(client_send)` `null::<(u32, i64,)>()`

        .output stdout `for_each(|tup| println!("echo {:?}", tup))`

        stdout(x) :- client_in(x)
        client_out@0(x) :~ client_in(x)
    "#
    );

    df.run_async().await;
}
