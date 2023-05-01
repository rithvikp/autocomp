use aes_gcm::{aead::Aead, aead::KeyInit, Aes128Gcm, Nonce};
use frankenpaxos::automicrobenchmarks_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    deserialize_from_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct ResponderArgs {}

fn encrypt_and_serialize(
    id: i64,
    ballot: u32,
    payload: Rc<Vec<u8>>,
    cipher: &Aes128Gcm,
    prev_hash: &mut u64,
) -> bytes::Bytes {
    let mut prev_hash_bytes = prev_hash.to_be_bytes().to_vec();
    prev_hash_bytes.append(&mut payload.as_ref().clone());
    let mut hasher = DefaultHasher::new();
    prev_hash_bytes.hash(&mut hasher);
    *prev_hash = hasher.finish();

    let iv = Nonce::from_slice(b"unique nonce");
    for _ in 0..100 {
        prev_hash_bytes = cipher.encrypt(iv, prev_hash_bytes.as_slice()).unwrap();
    }
    println!("sending out message with id: {}", id);

    let out = automicrobenchmarks_proto::ClientInbound {
        request: Some(
            automicrobenchmarks_proto::client_inbound::Request::ClientReply(
                automicrobenchmarks_proto::ClientReply {
                    id,
                    ballot: Some(ballot),
                    payload: Some(prev_hash_bytes),
                },
            ),
        ),
    };
    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

pub async fn run(_cfg: ResponderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let key_bytes = hex::decode("bfeed277024d4700c7edf24127858917").unwrap();
    let cipher = Aes128Gcm::new_from_slice(key_bytes.as_slice()).unwrap();

    let mut prev_hash: u64 = 0;

    let to_responder_source = ports
        .remove("receive_from$leaders$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let order_metadata_source = ports
        .remove("receive_from$leaders$1")
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
        .async toResponder `null::<(u32,i64,u32,Rc<Vec<u8>>,u32)>()` `source_stream(to_responder_source) -> map(|x: Result<(u32, BytesMut,), _>| deserialize_from_bytes::<(u32,i64,u32,Rc<Vec<u8>>,u32)>(x.unwrap().1).unwrap())`
        .async orderMetadata `null::<(u32,u32)>()` `source_stream(order_metadata_source) -> map(|x: Result<(u32, BytesMut,), _>| deserialize_from_bytes::<(u32,u32)>(x.unwrap().1).unwrap())`
        .async clientOut `map(|(node_id, (id, ballot, payload,))| (node_id, encrypt_and_serialize(id, ballot, payload, &cipher, &mut prev_hash))) -> dest_sink(client_send)` `null::<(i64,u32,Rc<Vec<u8>>)>()`
        .output outputBufferOut `for_each(|(client, id, b, v, t1)| println!("outputBufferOut: client: {}, id: {}, b: {}, t1: {}", client, id, b, t1))`
        // .output orderMetadatBufferOut `for_each(|(t1, prevT)| println!("orderMetadataBufferOut: t1: {}, prevT: {}", t1, prevT))`
        .output canSealOut `for_each(|(t1,)| println!("canSealOut: t1: {}", t1))`
        .output clientOutOut `for_each(|(client, id, b, v, t1)| println!("clientOut: client: {}, id: {}, b: {}, t1: {}", client, id, b, t1))`
        
        outputBufferOut(client, id, b, v, t1) :- outputBuffer(client, id, b, v, t1)
        orderMetadatBufferOut(t1, prevT) :- orderMetadataBuffer(t1, prevT)
        canSealOut(t1) :- canSeal(t1)
        clientOutOut(client, id, b, v, t1) :- outputBuffer(client, id, b, v, t1), canSeal(t1)

        outputBuffer(client, id, b, v, t1) :- toResponder(client, id, b, v, t1)
        outputBuffer(client, id, b, v, t1) :+ outputBuffer(client, id, b, v, t1), !canSeal(t1)
        orderMetadataBuffer(t1, prevT) :- orderMetadata(t1, prevT)
        orderMetadataBuffer(t1, prevT) :+ orderMetadataBuffer(t1, prevT), !canSeal(t1)
        canSeal(t1) :- outputBuffer(client, id, b, v, t1), orderMetadataBuffer(t1, prevT), sealed(prevT)
        canSeal(t1) :- outputBuffer(client, id, b, v, t1), orderMetadataBuffer(t1, prevT), (prevT == 0)
        sealed(t1) :+ sealed(t1), !canSeal(_)
        sealed(t1) :+ canSeal(t1)
        
        # Output current input plus hash of all previous payloads (computed in rust).
        clientOut@client(id, b, v) :~ outputBuffer(client, id, b, v, t1), canSeal(t1)
        "#
    );

    launch_flow(df).await
}
