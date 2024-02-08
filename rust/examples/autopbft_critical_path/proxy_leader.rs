use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use std::{collections::HashMap, rc::Rc};

#[derive(clap::Args, Debug)]
pub struct ProxyLeaderArgs {
    #[clap(long = "proxy-leader.index")]
    proxy_leader_index: Option<u32>,
    #[clap(long = "proxy-leader.f")]
    proxy_leader_f: Option<u32>,
    #[clap(long = "proxy-leader.num-prepreparer-partitions")]
    proxy_leader_num_prepreparer_partitions: Option<u32>,
}

// Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <o = command operation>, sig(c) = signature of o).
fn create_pre_prepare(slot: u32, digest: Rc<Vec<u8>>, sigps: Rc<Vec<Vec<u8>>>, command_id: Rc<Vec<u8>>, command: Rc<Vec<u8>>, sigc: Rc<Vec<Vec<u8>>>, receiver: u32, num_prepreparer_partitions: u32) -> (u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>) {
    let receiver_leader_id = receiver / num_prepreparer_partitions;
    (slot, digest, Rc::new(sigps[receiver_leader_id as usize].clone()), command_id, command, sigc)
}

pub async fn run(cfg: ProxyLeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let my_id = cfg.proxy_leader_index.unwrap();
    // println!("Proxy leader {:?} started", my_id);

    let pre_prepare_to_prepreparer_sink = ports
        .remove("send_to$prepreparers$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let pre_prepare_from_leader_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let f = cfg.proxy_leader_f.unwrap();
    let num_prepreparer_partitions = cfg.proxy_leader_num_prepreparer_partitions.unwrap();
    let prepreparer_start_ids: Vec<u32> = (0u32..((3*f+1) * num_prepreparer_partitions))
        .step_by(num_prepreparer_partitions.try_into().unwrap())
        .collect();

    // println!("Proxy leader {:?} ready", my_id);

    let df = datalog!(
        r#"
######################## relation definitions
# EDB
.input id `repeat_iter([(my_id,),])`
.input prepreparerStartIDs `repeat_iter(prepreparer_start_ids.clone()) -> map(|p| (p,))` # Assume = 0,m,2m,...,(n-1)*m, for n prepreparers and m partitions
.input numPrepreparerPartitions `repeat_iter([(num_prepreparer_partitions,),])` 

# IDB

# Pre-prepare (v = view, n = slot, d = hash digest of m, sig(p) = signature of (v,n,d), m = <o = command operation, t = timestamp, c = client>, sig(c) = signature of (o,t,c)).
# Simplified pre-prepare (n = slot, d = hash digest of m, sig(p) = signature of (n,d), m = <command_id, o = command operation>, sig(c) = signature of o).
.async prePrepareIn `null::<(u32, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>)>()` `source_stream(pre_prepare_from_leader_source) -> map(|x| deserialize_from_bytes::<(u32, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<Vec<u8>>>)>(x.unwrap().1).unwrap())`
.async prePrepareOut `map(|(node_id, (slot, digest, sigps, command_id, command, sigc))| (node_id, serialize_to_bytes(create_pre_prepare(slot, digest, sigps, command_id, command, sigc, node_id, num_prepreparer_partitions)))) -> dest_sink(pre_prepare_to_prepreparer_sink)` `null::<(Rc<Vec<u8>>, u32,)>()`

######################## end relation definitions

prePrepareOut@(r+(slot%n))(slot, digest, sigp, commandID, command, sigc) :~ prePrepareIn(slot, digest, sigp, commandID, command, sigc), prepreparerStartIDs(r), numPrepreparerPartitions(n)
    "#
    );

    launch_flow(df).await;
}
