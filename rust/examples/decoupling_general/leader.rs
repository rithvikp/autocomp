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
pub struct LeaderArgs {
}

fn deserialize(msg: BytesMut) -> Option<(u32,i64,u32,Rc<Vec<u8>>)> {
    //todo unwrap, dummy stuff
    Some((0,0,0,Rc::new(vec![])))
}

fn hash_and_serialize(id: i64, ballot: u32, payload: Rc<Vec<u8>>) -> bytes::Bytes {
    //todo serialize
    //todo store hash over all previous inputs in a mutable structure and append the new output. Can't do this in Dedalus because we don't have a hash function.
    bytes::Bytes::from("".to_string())
}

pub async fn run(cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    // Client setup
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

    let df = datalog!(
        r#"
        .input startBallot `repeat_iter([(0 as u32,),])`
.async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> filter_map(|x: Result<(u32, BytesMut,), _>| (deserialize(x.unwrap().1)))`
.async clientOut `map(|(node_id, (id, ballot, payload,))| (node_id, hash_and_serialize(id, ballot, payload))) -> dest_sink(client_send)` `null::<(i64,u32,Rc<Vec<u8>>)>()`

# Buffer inputs, choose 1 at a time.
clientBuffer(client, id, b, v) :- clientIn(client, id, b, v)
clientBuffer(client, id, b, v) :+ clientBuffer(client, id, b, v), !nextIn(client, id)
nextIn(choose(client), choose(id)) :- clientBuffer(client, id, b, v)

# Automatically update epoch.
ballots(b) :- startBallot(b)
maxBallot(max(b)) :- ballots(b)
ballots(b+1) :+ maxBallot(b)

# Store 1 input and track previous input.
.persist storage
storage(v) :- nextIn(client, id), clientBuffer(client, id, b, v)

# Output current input plus hash of previous (computed in rust).
clientOut@client(id, b, v) :~ nextIn(client, id), clientBuffer(client, id, _, v), maxBallot(b)
        "#
    );

    launch_flow(df).await
}