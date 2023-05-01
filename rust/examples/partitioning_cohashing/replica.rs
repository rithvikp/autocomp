use aes_gcm::{aead::Aead, aead::KeyInit, Aes128Gcm, Nonce};
use std::collections::HashMap;

use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct ReplicaArgs {
    #[clap(long)]
    index: Option<u32>,
}

fn serialize(
    (node_id, client, id, v): (u32, u32, i64, Rc<Vec<u8>>),
    cipher: &Aes128Gcm,
) -> (u32, u32, i64, Rc<Vec<u8>>) {
    let iv = Nonce::from_slice(b"unique nonce");
    let mut encrypted_payload = v.as_ref().clone();
    for _ in 0..100 {
        encrypted_payload = cipher.encrypt(iv, encrypted_payload.as_slice()).unwrap();
    }
    return (node_id, client, id, Rc::new(encrypted_payload));
}

pub async fn run(cfg: ReplicaArgs, mut ports: HashMap<String, ServerOrBound>) {
    let key_bytes = hex::decode("bfeed277024d4700c7edf24127858917").unwrap();
    let cipher = Aes128Gcm::new_from_slice(key_bytes.as_slice()).unwrap();

    let to_replica_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let from_replica_port = ports
        .remove("send_to$leaders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;

    let peers = from_replica_port.keys.clone();
    let from_replica_sink = from_replica_port.into_sink();

    let my_id: Vec<u32> = vec![cfg.index.unwrap()];

    let df = datalog!(
        r#"
        .input myID `repeat_iter(my_id.clone()) -> map(|p| (p,))`
.input leader `repeat_iter(peers.clone()) -> map(|p| (p,))`
.async voteToReplica `null::<(u32,i64,Rc<Vec<u8>>,)>()` `source_stream(to_replica_source) -> map(|x| deserialize_from_bytes::<(u32,i64,Rc<Vec<u8>>,)>(x.unwrap().1).unwrap())`
.async voteFromReplica `map(|(node_id, v)| (node_id, serialize_to_bytes(serialize(v, &cipher)))) -> dest_sink(from_replica_sink)` `null::<(u32,u32,i64,Rc<Vec<u8>>,)>()`
            
.persist storage
storage(v) :- voteToReplica(client, id, v) 
voteFromReplica@addr(i, client, id, v) :~ voteToReplica(client, id, v), leader(addr), myID(i)
        "#
    );

    launch_flow(df).await;
}
