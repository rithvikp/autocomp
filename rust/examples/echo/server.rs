use hydroflow::util::{
    cli::{Connected, ConnectedBidi},
    deserialize_from_bytes,
};
use hydroflow_datalog::datalog;

#[derive(clap::Args, Debug)]
#[allow(non_snake_case)] // For consistency with the Scala code
pub struct ServerArgs {
    #[clap(long)]
    persistLog: bool,
    #[clap(long, default_value = "0.0.0.0")]
    prometheusHost: String,
    #[clap(long, default_value = "8009")]
    prometheusPort: i32,
}

pub async fn run_server(cfg: ServerArgs) {
    let mut ports = hydroflow::util::cli::init().await;
    let broadcast_recv = ports
        .remove("broadcast")
        .unwrap()
        .connect::<ConnectedBidi>()
        .await
        .take_source();

    frankenpaxos::serve_prometheus(cfg.prometheusHost, cfg.prometheusPort);

    let mut df = datalog!(
        r#"
        .async broadcast `null::<(String,)>()` `source_stream(broadcast_recv) -> map(|x| deserialize_from_bytes::<(String,)>(x.unwrap()))`
        .output stdout `for_each(|tup| println!("echo {:?}", tup))`
        stdout(x) :- broadcast(x)
    "#
    );

    df.run_async().await;
}
