use frankenpaxos::multipaxos_proto;
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
pub struct ProxyLeaderArgs {
    #[clap(long = "proxy-leader.index")]
    proxy_leader_index: Option<u32>,

    #[clap(long = "proxy-leader.num-acceptor-rows")]
    proxy_leader_num_acceptor_rows: Option<u32>,

    #[clap(long = "proxy-leader.num-acceptor-columns")]
    proxy_leader_num_acceptor_columns: Option<u32>,

    #[clap(long = "proxy-leader.f")]
    f: Option<u32>,
}

fn serialize(payload: Rc<Vec<u8>>, slot: u32) -> bytes::Bytes {
    let command =
        multipaxos_proto::CommandBatchOrNoop::decode(&mut Cursor::new(payload.as_ref())).unwrap();

    let out = multipaxos_proto::ReplicaInbound {
        request: Some(multipaxos_proto::replica_inbound::Request::Chosen(
            multipaxos_proto::Chosen {
                slot: i32::try_from(slot).unwrap() - 1, // Dedalus starts at slot 1
                command_batch_or_noop: command,
            },
        )),
    };

    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

pub async fn run(cfg: ProxyLeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let p2a_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let p2a_port = ports
        .remove("send_to$acceptors$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let acceptors = p2a_port.keys.clone();
    let p2a_sink = p2a_port.into_sink();

    let p2b_source = ports
        .remove("receive_from$acceptors$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    // Replica setup
    let replica_port = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let replicas = replica_port.keys.clone();
    let replica_send = replica_port.into_sink();

    let my_id = cfg.proxy_leader_index.unwrap();
    let f = cfg.f.unwrap();
    let num_acceptor_rows = cfg.proxy_leader_num_acceptor_rows.unwrap();
    let num_acceptor_columns = cfg.proxy_leader_num_acceptor_columns.unwrap();
    // Assume acceptor IDs in the grid are assigned like so:
    // 01
    // 23
    // Let start IDs = beginning of rows. Then can add slot%num_columns to write to columns.
    let mut acceptor_start_ids: Vec<u32> = Vec::new();
    for i in 0..num_acceptor_rows {
        acceptor_start_ids.push(i * num_acceptor_columns);
    }
    

    let df = datalog!(
        r#"
        ######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input quorum `repeat_iter([(f+1,),])`
.input acceptorStartIDs `repeat_iter(acceptor_start_ids.clone()) -> map(|p| (p,))` # Assume = 0,m,2m,...,(n-1)*m, for n acceptors and m partitions
.input numAcceptorColumns `repeat_iter([(num_acceptor_columns,),])`
.input replicas `repeat_iter(replicas.clone()) -> map(|p| (p,))`

.async clientOut `map(|(node_id, (payload, slot,))| (node_id, serialize(payload, slot))) -> dest_sink(replica_send)` `null::<(Rc<Vec<u8>>, u32,)>()`

# p2a: proposerID, payload, slot, ballotID, ballotNum
.async p2aIn `null::<(u32,Rc<Vec<u8>>,u32,u32,u32,)>()` `source_stream(p2a_source) -> map(|v| deserialize_from_bytes::<(u32,Rc<Vec<u8>>,u32,u32,u32,)>(v.unwrap().1).unwrap())`
.async p2aBroadcast `map(|(node_id, v):(u32,(u32,Rc<Vec<u8>>,u32,u32,u32))| (node_id, serialize_to_bytes(v))) -> dest_sink(p2a_sink)` `null::<(u32,Rc<Vec<u8>>,u32,u32,u32)>()` 
# p2b: acceptorRow, acceptorColumn, payload, slot, ballotID, ballotNum, maxBallotID, maxBallotNum
.async p2bU `null::<(u32,u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32)>()` `source_stream(p2b_source) -> map(|v| deserialize_from_bytes::<(u32,u32,Rc<Vec<u8>>,u32,u32,u32,u32,u32,)>(v.unwrap().1).unwrap())`
######################## end relation definitions

p2b(ar, ac, p, s, i, n, mi, mn) :- p2bU(ar, ac, p, s, i, n, mi, mn)
p2b(ar, ac, p, s, i, n, mi, mn) :+ p2b(ar, ac, p, s, i, n, mi, mn), !commit(_, s) # drop all p2bs if slot s is committed

p2aBroadcast@(aid+(slot%n))(pid, payload, slot, id, num) :~ p2aIn(pid, payload, slot, id, num), numAcceptorColumns(n), acceptorStartIDs(aid)

# Write quorum is columner (count number of unique ar rows per ac column)
CountMatchingP2bs(payload, slot, count(ar), ac, i, num) :- p2b(ar, ac, payload, slot, i, num, payloadBallotID, payloadBallotNum)
commit(payload, slot) :- CountMatchingP2bs(payload, slot, c, ac, i, num), quorum(c)
clientOut@r(payload, slot) :~ commit(payload, slot), replicas(r)
"#
    );

    launch_flow(df).await
}
