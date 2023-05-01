use aes_gcm::{aead::Aead, aead::KeyInit, Aes128Gcm, Nonce};
use frankenpaxos::automicrobenchmarks_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    serialize_to_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::rc::Rc;
use std::{collections::HashMap, io::Cursor};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {}

fn decrypt_and_deserialize(msg: BytesMut, cipher: &Aes128Gcm) -> (i64, u32, Rc<Vec<u8>>) {
    let s =
        automicrobenchmarks_proto::ServerInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();
    println!("Received message with id {}", s.id);
    let iv = Nonce::from_slice(b"unique nonce");
    let mut decrypted_payload = s.payload.clone();
    for _ in 0..100 {
        decrypted_payload = cipher.decrypt(iv, decrypted_payload.as_ref()).unwrap();
    }
    return (s.id, s.ballot, Rc::new(decrypted_payload));
}

pub async fn run(_cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let key_bytes = hex::decode("bfeed277024d4700c7edf24127858917").unwrap();
    let cipher = Aes128Gcm::new_from_slice(key_bytes.as_slice()).unwrap();

    // Client setup
    let client_recv = ports
        .remove("receive_from$clients$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let to_responder = ports
        .remove("send_to$responders$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;
    let peers = to_responder.keys.clone();
    let to_responder_sink = to_responder.into_sink();

    let order_metadata_sink = ports
        .remove("send_to$responders$1")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await
        .into_sink();

    let df = datalog!(
        r#"
.input responders `repeat_iter(peers.clone()) -> map(|p| (p,))`
.input tick `repeat_iter(vec![()]) -> map(|_| (context.current_tick() as u32,))`

.async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> map(|x| {let input = x.unwrap(); let v = decrypt_and_deserialize(input.1, &cipher); (input.0, v.0, v.1, v.2,)})`
.async toResponder `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(to_responder_sink)` `null::<(i64,u32,Rc<Vec<u8>>)>()`
.async orderMetadata `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(order_metadata_sink)` `null::<(u32,u32)>()`

# Buffer inputs, choose 1 at a time.
clientBuffer(client, id, b, v) :- clientIn(client, id, b, v)
clientBuffer(client, id, b, v) :+ clientBuffer(client, id, b, v), !nextIn(client, id)
nextIn(choose(client), choose(id)) :- clientBuffer(client, id, b, v)
nextPayload(client, id, b, v) :- nextIn(client, id), clientBuffer(client, id, b, v)

# Store 1 input and track previous input.
.persist storage
storage(v) :- nextPayload(client, id, b, v)

toResponder@addr(client, id, b, v, t1) :~ nextPayload(client, id, b, v), responders(addr), tick(t1)
batchTimes(t1) :+ nextPayload(_, _, _, _), tick(t1) # Since there's only 1 r to be batched, (n != 0) is implied
batchTimes(t1) :+ !nextPayload(_, _, _, _), batchTimes(t1) # Persist if no batch
orderMetadata@addr(t1, prevT) :~ tick(t1), batchTimes(prevT), responders(addr)
orderMetadata@addr(t1, 0) :~ tick(t1), !batchTimes(prevT), responders(addr)
        "#
    );

    launch_flow(df).await
}
