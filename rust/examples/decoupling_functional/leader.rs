use bytes::Bytes;
use frankenpaxos::automicrobenchmarks_proto;
use hydroflow::bytes::BytesMut;
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
pub struct LeaderArgs {}

fn deserialize(msg: BytesMut) -> Option<(u32, i64, u32, Rc<Vec<u8>>)> {
    //todo unwrap, dummy stuff
    Some((0, 0, 0, Rc::new(vec![])))
}

fn serialize(id: i64, ballot: u32, payload: Rc<Vec<u8>>) -> bytes::Bytes {
    //todo serialize
    Bytes::from("".to_string())
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
        .async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> filter_map(|x: Result<(u32, BytesMut,), _>| (deserialize(x.unwrap().1)))`
.async clientOut `map(|(node_id, (id, ballot, payload,))| (node_id, serialize(id, ballot, payload))) -> dest_sink(client_send)` `null::<(i64,u32,Rc<Vec<u8>>)>()`

# ballot is guaranteed to either be empty or contain the 1 current ballot
newBallots(b) :- clientIn(client, id, b, v)
newBallots(b) :- ballot(b)
MaxNewBallot(max(b)) :- newBallots(b)
ballot(b) :+ MaxNewBallot(b)

.persist storage
storage(v) :- clientIn(client, id, b, v)
        
# Attach ballot at the time clientIn arrived to the output
clientOut@client(id, b, v) :~ clientIn(client, id, _, v), ballot(b)
clientOut@client(id, 0, v) :~ clientIn(client, id, _, v), !ballot(b)
        "#
    );

    launch_flow(df).await
}
