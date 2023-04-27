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

fn serialize(id: i64, ballot: u32, payload: Rc<Vec<u8>>) -> bytes::Bytes {
    //todo serialize
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

     // Replica setup
    let to_replica_port = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let peers = to_replica_port.keys.clone();
    let num_replicas = peers.len();
    let to_replica_sink = to_replica_port.into_sink();

    let from_replica_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let df = datalog!(
        r#"
        .input replicas `repeat_iter(peers.clone()) -> map(|p| (p,))`
        .input numReplicas `repeat_iter([(num_replicas,),])`
        
        .async voteToReplica `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(to_replica_sink)` `null::<(u32,i64,u32,Rc<Vec<u8>>)>()`
        .async voteFromReplica `null::<(u32,u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(from_replica_source) -> map(|v| deserialize_from_bytes::<(u32,u32,i64,u32,Rc<Vec<u8>>)>(v.unwrap().1).unwrap())`
        
        .async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> filter_map(|x: Result<(u32, BytesMut,), _>| (deserialize(x.unwrap().1)))`
        .async clientOut `map(|(node_id, (id, ballot, payload,))| (node_id, serialize(id, ballot, payload))) -> dest_sink(client_send)` `null::<(i64,u32,Rc<Vec<u8>>)>()`
        
        voteToReplica@addr(client, id, b, v) :~ clientIn(client, id, b, v), replicas(addr)
                
        allVotes(l, client, id, b, v) :- voteFromReplica(l, client, id, b, v)
        allVotes(l, client, id, b, v) :+ allVotes(l, client, id, b, v), !committed(client, id, _, _)
        voteCounts(count(l), client, id) :- allVotes(l, client, id, b, v)
        committed(client, id, b, v) :- voteCounts(n, client, id), numReplicas(n), allVotes(l, client, id, b, v)
        clientOut@client(id, b, v) :~ committed(client, id, b, v)
        "#
    );

    launch_flow(df).await
}