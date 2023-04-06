use hydroflow::hydroflow_syntax;
use hydroflow::util::{
    cli::{ConnectedBidi, ConnectedSource},
    deserialize_from_bytes,
};
use hydroflow_datalog::datalog;

#[derive(clap::Args, Debug)]
pub struct ServerArgs {
    #[clap(long)]
    persist_log: bool,
}

fn deserialize(msg: impl AsRef<[u8]>) -> (String,) {
    let x = String::from_utf8(msg.as_ref().to_vec()).unwrap();
    println!("echo {x}");
    return (x,);
}

pub async fn run_server(cfg: ServerArgs) {
    let mut ports = hydroflow::util::cli::init().await;
    let client_recv = ports
        .remove("receive_from$clients")
        .unwrap()
        .connect::<ConnectedBidi>()
        .await
        .into_source();

    println!("Running server...");

    if cfg.persist_log {
        // TODO: Do something
    }

    let mut df = datalog!(
        r#"
        .async broadcast `null::<(String,)>()` `source_stream(client_recv) -> map(|x| deserialize(x.unwrap()))`
        .output stdout `for_each(|tup| println!("echo {:?}", tup))`
        stdout(x) :- broadcast(x)
    "#
    );

    df.run_async().await;

    // let df = hydroflow_syntax! {
    //     source_stream(client_recv) ->
    //         map(|x| String::from_utf8(x.unwrap().to_vec()).unwrap()) ->
    //         for_each(|x| println!("echo {:?}", x));
    // };
    // hydroflow::util::cli::launch_flow(df).await;
}
