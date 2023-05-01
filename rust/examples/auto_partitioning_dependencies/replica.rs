use aes_gcm::{aead::Aead, aead::KeyInit, Aes128Gcm, Nonce};
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes, serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct ReplicaArgs {
    #[clap(long)]
    index: Option<u32>,
}

fn deserialize_and_hash(
    (client, id, ballot, v): (u32, i64, u32, Rc<Vec<u8>>),
) -> (u32, i64, u32, Rc<Vec<u8>>, u32) {
    let mut hasher = DefaultHasher::new();
    v.hash(&mut hasher);
    let hashed = hasher.finish();
    // for _ in 0..1000 {
    //     let mut hasher = DefaultHasher::new();
    //     hashed.hash(&mut hasher);
    //     hashed = hasher.finish();
    // }
    (client, id, ballot, v, hashed as u32)
}

fn serialize(
    (node_id, client, id, ballot, v): (u32, u32, i64, u32, Rc<Vec<u8>>),
    cipher: &Aes128Gcm,
) -> (u32, u32, i64, u32, Rc<Vec<u8>>) {
    let iv = Nonce::from_slice(b"unique nonce");
    let mut encrypted_payload = v.as_ref().clone();
    for _ in 0..100 {
        encrypted_payload = cipher.encrypt(iv, encrypted_payload.as_slice()).unwrap();
    }
    return (node_id, client, id, ballot, Rc::new(encrypted_payload));
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
        .async voteToReplica `null::<(u32,i64,u32,Rc<Vec<u8>>,)>()` `source_stream(to_replica_source) -> map(|x| deserialize_and_hash(deserialize_from_bytes::<(u32,i64,u32,Rc<Vec<u8>>,)>(x.unwrap().1).unwrap()))`
        .async voteFromReplica `map(|(node_id, v)| (node_id, serialize_to_bytes(serialize(v, &cipher)))) -> dest_sink(from_replica_sink)` `null::<(u32,u32,i64,u32,Rc<Vec<u8>>,)>()`
                    
        .persist storage
        storage(v, hashed) :+ voteToReplica(client, id, b, v, hashed)
        collisions(choose(v2), hashed) :- voteToReplica(client, id, b, v1, hashed), storage(v2, hashed)
        voteFromReplica@addr(i, client, id, b, v2) :~ voteToReplica(client, id, b, v, hashed), collisions(v2, hashed), leader(addr), myID(i)
        voteFromReplica@addr(i, client, id, b, v) :~ voteToReplica(client, id, b, v, hashed), !collisions(_, hashed), leader(addr), myID(i)
        "#
    );

    launch_flow(df).await;
}
