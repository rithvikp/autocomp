use aes_gcm::{aead::Aead, aead::KeyInit, Aes128Gcm, Nonce};
use frankenpaxos::automicrobenchmarks_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::util::cli::{
    launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource, ConnectedTagged,
    ServerOrBound,
};
use hydroflow_datalog::datalog;
use prost::Message;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::{collections::HashMap, io::Cursor};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {}

fn decrypt_and_deserialize(msg: BytesMut, cipher: &Aes128Gcm) -> (i64, u32, Rc<Vec<u8>>) {
    let s =
        automicrobenchmarks_proto::ServerInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();
    let iv = Nonce::from_slice(b"unique nonce");
    let mut decrypted_payload = s.payload.clone();
    for _ in 0..100 {
        decrypted_payload = cipher.decrypt(iv, decrypted_payload.as_ref()).unwrap();
    }
    return (s.id, s.ballot, Rc::new(decrypted_payload));
}

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

pub async fn run(_cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let key_bytes = hex::decode("bfeed277024d4700c7edf24127858917").unwrap();
    let cipher1 = Aes128Gcm::new_from_slice(key_bytes.as_slice()).unwrap();
    let cipher2 = Aes128Gcm::new_from_slice(key_bytes.as_slice()).unwrap();

    let mut prev_hash: u64 = 0;

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
.async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> map(|x| {let input = x.unwrap(); let v = decrypt_and_deserialize(input.1, &cipher1); (input.0, v.0, v.1, v.2,)})`
.async clientOut `map(|(node_id, (id, ballot, payload,))| (node_id, encrypt_and_serialize(id, ballot, payload, &cipher2, &mut prev_hash))) -> dest_sink(client_send)` `null::<(i64,u32,Rc<Vec<u8>>)>()`

# Buffer inputs, choose 1 at a time.
clientBuffer(client, id, b, v) :- clientIn(client, id, b, v)
clientBuffer(client, id, b, v) :+ clientBuffer(client, id, b, v), !nextIn(client, id)
nextIn(choose(client), choose(id)) :- clientBuffer(client, id, b, v)
nextPayload(client, id, b, v) :- nextIn(client, id), clientBuffer(client, id, b, v)

# Store 1 input and track previous input.
.persist storage
storage(v) :- nextPayload(client, id, b, v)

# Output current input plus hash of all previous payloads (computed in rust).
clientOut@client(id, b, v) :~ nextPayload(client, id, b, v)
        "#
    );

    launch_flow(df).await
}
