use bytes::Bytes;
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
use rand::rngs::ThreadRng;
use rsa::pkcs8::DecodePublicKey;
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use std::collections::HashMap;
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct ResponderArgs {}

fn encrypt_and_serialize(
    id: i64,
    ballot: u32,
    payload: Rc<Vec<u8>>,
    rand: &mut ThreadRng,
    key: &RsaPublicKey,
) -> bytes::Bytes {
    let encrypted_payload: Vec<u8> = key.encrypt(rand, Pkcs1v15Encrypt, &payload).unwrap();
    let out = automicrobenchmarks_proto::ClientInbound {
        request: Some(
            automicrobenchmarks_proto::client_inbound::Request::ClientReply(
                automicrobenchmarks_proto::ClientReply {
                    id,
                    ballot: Some(ballot),
                    payload: Some(encrypted_payload),
                },
            ),
        ),
    };
    let mut buf = Vec::new();
    out.encode(&mut buf).unwrap();
    return bytes::Bytes::from(buf);
}

pub async fn run(cfg: ResponderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let mut rng = rand::thread_rng();

    let public_pem = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3WYRq2ZTa0P+PIy2OKBD
mkeARauGuQsm8/fekUo8ImSVVdsnrne1XGxad+ykj5fB1Miw6aoCYKkzT5pzNWG7
gA5XOWUyfvGVfSMcjxl65zXdDeaG03S014dHwZwhdMmnEl2sjRyGEBDT/FRkysoT
+O+dlF48yvgYpMaVpiQOLpEUSa+FkiqZNd/jn2rT1hwyWaWVnVvcCIWDxdA8X/NO
sL+pqSSjQm/m9m2JVN+yqMdu1v3gBVLk26bl+MWzjedptqEgK0qNevT0R++E3jgB
CDHrpRhZ3Dg4ay7FqpsvkyyNdUMaf0yVa7faTCcoPtMv9RDI4NmmWExrSZpAg7g3
JQIDAQAB
-----END PUBLIC KEY-----";

    let public_key = RsaPublicKey::from_public_key_pem(&public_pem.trim()).unwrap();

    // Client setup
    let to_responder_source = ports
        .remove("receive_from$leaders$0")
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
        .async toResponder `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(to_responder_source) -> map(|x: Result<(u32, BytesMut,), _>| deserialize_from_bytes::<(u32,i64,u32,Rc<Vec<u8>>)>(x.unwrap().1).unwrap())`
.async clientOut `map(|(node_id, (id, ballot, payload,))| (node_id, encrypt_and_serialize(id, ballot, payload, &mut rng, &public_key))) -> dest_sink(client_send)` `null::<(i64,u32,Rc<Vec<u8>>)>()`

clientOut@client(id, b, v) :~ toResponder(client, id, b, v)
        "#
    );

    launch_flow(df).await
}
