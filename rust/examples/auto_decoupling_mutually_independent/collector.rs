use frankenpaxos::automicrobenchmarks_proto;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct CollectorArgs {
    #[clap(long = "collector.num-replicas")]
    collector_num_replicas: Option<u32>,
}

fn encrypt_and_serialize(id: i64, payload: Rc<Vec<u8>>) -> bytes::Bytes {
    let out = automicrobenchmarks_proto::ClientInbound {
        request: Some(
            automicrobenchmarks_proto::client_inbound::Request::ClientReply(
                automicrobenchmarks_proto::ClientReply {
                    id,
                    ballot: None,
                    payload: Some(payload.as_ref().clone()),
                },
            ),
        ),
    };
    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

pub async fn run(cfg: CollectorArgs, mut ports: HashMap<String, ServerOrBound>) {
    // Client setup
    let from_replica_source = ports
        .remove("receive_from$replicas$0")
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

    let num_replicas = cfg.collector_num_replicas.unwrap();

    let df = datalog!(
        r#"
        .input numReplicas `repeat_iter([(num_replicas,),])`
        
        .async voteFromReplica `null::<(u32,u32,i64,Rc<Vec<u8>>)>()` `source_stream(from_replica_source) -> map(|v| deserialize_from_bytes::<(u32,u32,i64,Rc<Vec<u8>>)>(v.unwrap().1).unwrap())`
        .async clientOut `map(|(node_id, (id, payload,))| (node_id, encrypt_and_serialize(id, payload))) -> dest_sink(client_send)` `null::<(i64,Rc<Vec<u8>>)>()`
        
        allVotes(l, client, id, v) :- voteFromReplica(l, client, id, v)
        allVotes(l, client, id, v) :+ allVotes(l, client, id, v), !committed(client, id, _)
        voteCounts(count(l), client, id) :- allVotes(l, client, id, v)
        committed(client, id, v) :- voteCounts(n, client, id), numReplicas(n), allVotes(l, client, id, v)
        clientOut@client(id, v) :~ committed(client, id, v)
        "#
    );

    launch_flow(df).await
}
