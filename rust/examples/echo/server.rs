use hydroflow::util::{
    cli::{Connected, ConnectedBidi},
    deserialize_from_bytes,
};
use hydroflow_datalog::datalog;

#[derive(clap::Args, Debug)]
pub struct ServerArgs {
    #[clap(long)]
    persist_log: bool,
}

pub async fn run_server(cfg: ServerArgs) {
    let mut ports = hydroflow::util::cli::init().await;
    let broadcast_recv = ports
        .remove("broadcast")
        .unwrap()
        .connect::<ConnectedBidi>()
        .await
        .take_source();

    if cfg.persist_log {
        // TODO: Do something
    }

    let mut df = datalog!(
        r#"
        .async broadcast `null::<(String,)>()` `source_stream(broadcast_recv) -> map(|x| deserialize_from_bytes::<(String,)>(x.unwrap()))`
        .output stdout `for_each(|tup| println!("echo {:?}", tup))`
        stdout(x) :- broadcast(x)
    "#
    );

    df.run_async().await;
}
