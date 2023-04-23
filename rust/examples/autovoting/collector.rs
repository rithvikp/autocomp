use frankenpaxos::voting_proto;
use std::collections::HashMap;

use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use rand::{distributions::Alphanumeric, Rng};

#[derive(clap::Args, Debug)]
pub struct CollectorArgs {
    #[clap(long = "collector.num-replica-partitions")]
    collector_num_replica_partitions: Option<u32>,
    #[clap(long = "collector.num-replica-groups")]
    collector_num_replica_groups: Option<u32>,
}

fn serialize(v: (i64,)) -> bytes::Bytes {
    let s = voting_proto::ClientReply {
        id: v.0,
        accepted: true,
    };
    let mut buf = Vec::new();
    s.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

pub async fn run(cfg: CollectorArgs, mut ports: HashMap<String, ServerOrBound>) {
    let client_send = ports
        .remove("send_to$clients$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let from_participant_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let num_participant_groups = i64::from(cfg.collector_num_replica_groups.unwrap());
    let id: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();

    // println!("collector {id} started");

    let df = datalog!(
        r#"
.input numParticipants `repeat_iter([(num_participant_groups,),])` # Assume = 0,1,2...num_participants
.async clientOut `map(|(node_id, id)| (node_id, serialize(id))) -> dest_sink(client_send)` `null::<(u32, i64,)>()`

.async voteFromParticipant `null::<(u32,u32,i64,)>()` `source_stream(from_participant_source) -> map(|v| deserialize_from_bytes::<(u32,u32,i64,)>(v.unwrap().1).unwrap())`

allVotes(l, client, v) :- voteFromParticipant(l, client, v)
allVotes(l, client, v) :+ allVotes(l, client, v), !committed(client, v)
voteCounts(count(l), client, v) :- allVotes(l, client, v)
committed(client, v) :- voteCounts(n, client, v), numParticipants(n)

clientOut@client(v) :~ committed(client, v)
        "#
    );

    launch_flow(df).await;
}
