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
    let iv = Nonce::from_slice(b"unique nonce");
    let s =
        automicrobenchmarks_proto::ServerInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();
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

    let df = datalog!(
        r#"
        .input responders `repeat_iter(peers.clone()) -> map(|p| (p,))`

        .async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> map(|x| {let input = x.unwrap(); let v = decrypt_and_deserialize(input.1, &cipher); (input.0, v.0, v.1, v.2,)})`
        .async toResponder `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(to_responder_sink)` `null::<(u32,i64,u32,Rc<Vec<u8>>)>()`
        
        # ballot is guaranteed to either be empty or contain the 1 current ballot
        newBallots(b) :- clientIn(client, id, b, v)
        newBallots(b) :- ballot(b)
        MaxNewBallot(max(b)) :- newBallots(b)
        ballot(b) :+ MaxNewBallot(b)
        
        .persist storage
        storage(v) :- clientIn(client, id, b, v)
                
        # Attach ballot at the time clientIn arrived to the output
        output(client, id, b, v) :- clientIn(client, id, _, v), ballot(b)
        output(client, id, 0, v) :- clientIn(client, id, _, v), !ballot(b)
        toResponder@addr(client, id, b, v) :~ output(client, id, b, v), responders(addr)
        "#
    );

    launch_flow(df).await
}
