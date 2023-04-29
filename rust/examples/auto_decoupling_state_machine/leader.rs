use bytes::Bytes;
use frankenpaxos::automicrobenchmarks_proto;
use hydroflow::bytes::BytesMut;
use hydroflow::util::{
    cli::{
        launch_flow, ConnectedBidi, ConnectedDemux, ConnectedSink, ConnectedSource,
        ConnectedTagged, ServerOrBound,
    },
    serialize_to_bytes, deserialize_from_bytes,
};
use hydroflow_datalog::datalog;
use prost::Message;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePublicKey;
use rsa::{RsaPrivateKey, Pkcs1v15Encrypt, RsaPublicKey};
use std::rc::Rc;
use std::{collections::HashMap, io::Cursor};

#[derive(clap::Args, Debug)]
pub struct LeaderArgs {}

fn decrypt_and_deserialize(msg: BytesMut, key: &RsaPrivateKey) -> (i64, u32, Rc<Vec<u8>>) {
    let s =
        automicrobenchmarks_proto::ServerInbound::decode(&mut Cursor::new(msg.as_ref())).unwrap();
    let decrypted_payload = key.decrypt(Pkcs1v15Encrypt, &s.payload).unwrap();
    return (s.id, s.ballot, Rc::new(decrypted_payload));
}

pub async fn run(cfg: LeaderArgs, mut ports: HashMap<String, ServerOrBound>) {
    let private_pem = "-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEA3WYRq2ZTa0P+PIy2OKBDmkeARauGuQsm8/fekUo8ImSVVdsn
rne1XGxad+ykj5fB1Miw6aoCYKkzT5pzNWG7gA5XOWUyfvGVfSMcjxl65zXdDeaG
03S014dHwZwhdMmnEl2sjRyGEBDT/FRkysoT+O+dlF48yvgYpMaVpiQOLpEUSa+F
kiqZNd/jn2rT1hwyWaWVnVvcCIWDxdA8X/NOsL+pqSSjQm/m9m2JVN+yqMdu1v3g
BVLk26bl+MWzjedptqEgK0qNevT0R++E3jgBCDHrpRhZ3Dg4ay7FqpsvkyyNdUMa
f0yVa7faTCcoPtMv9RDI4NmmWExrSZpAg7g3JQIDAQABAoIBAQC7yQ4lpgYNTj9+
1g7c2rpcSlc3QLRlkVr5xXzHGQMGXO/5QYhXx/tRKCpX26T8kkh6lbrQhj28BOeb
ljIvzfR+OiurZ/U7jOcxm61mhsMjByG235fuFOgqgvjO9AHAkCWgQ0nO6qmfSNa1
CFPxWRM1qu+qX4AK3AHzCOj3YU+SGDHzbQkTvui5UXnuPj79dmRd17WEKlbTwfmG
iGblbFKhQknH/rDzvnwHVmvwwegRT8EDAtHtjcawx5a4bRXnehDxuK4D2zQsOrJM
NAejpY8aezuu8k4RlEKZms9NXSLM0oRjwNF+uKy2RPisWimSEkzX1+MQu9lNN7JB
8jnv+pnhAoGBAPQct5h7dYBMgVX+5qXwE1L7d4guNIzHQhxvfrhSV0krg/pMnCdc
Gho1COEz7I3qC5LPS3Rm6cXB+6THAj4K9Vl8diGGoKjPI0BBZ3lALorTSg9Ox92e
7asZ2bej7SNU0H33Fq9StbZ1RXVLbnhrxx0PC3wSv/rWSE2MvO9dBAc5AoGBAOgu
MF9DE2nh8J8Qd9APd7YZDqRc22PEVD+28vlufyjJLnZ15JLcOD5TRFbksrgwuQVl
lCPkhkXCjoFoRhpyTBVEOScj/OofOMF0jq/kqOWiS8rYVXtqY+wcc4PqBenStQNB
TZXmiTIO3ItlgIPjS6uLKq/gx7eBLrkbYPehb2NNAoGAd94mFR0CjLaEh+q77V2e
z4+lmVw+96Xcg8QH8JCWRdq1WtIFl5o1QwspyhPKI8F6X2OlBIqIXlArtjT2l+qf
oYppI3tNJrdX8vRqz/JBKHAqHKwtnlitEeiGrRM0nbXHDyOmov61NdwN+FPK8hzS
UCWUGR/H328bX+Es4UjGcpkCgYEAngUxruQ8BkhcOCZBJT4hv7H2IS+Bsbkhjeu6
GsF1khC6qq06UCnJrcGGNY2ZhXrDskScOoGCUWBFFRYfAEjiN2cjbtwzejSdsCzg
IB6ERSXcZ8YtB741G7GRfSp9s9JQmFKNt4QbxBIRUF5YxWqhSbOB7goSY3Zskldr
+53H7dECgYEA31jL5SObUyeUyZqNRRvvMZ1msBeBGVEItjWr8IwK5j6BLRPEwzHO
/Ll+kqrm/wQLtM+W+ou9iXCYWby71CW2UXzmnzzEIkTNAMDXLsl0a6eOtbZDYU/f
Bgdg9r29Eahe2ZvI6ja8ekkXuvY9iKiDs8Q+ZzGRoJywlld/un7f0Bc=
-----END RSA PRIVATE KEY-----";

    let private_key = RsaPrivateKey::from_pkcs1_pem(&private_pem.trim()).unwrap();

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

.async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> map(|x| {let input = x.unwrap(); let v = decrypt_and_deserialize(input.1, &private_key); (input.0, v.0, v.1, v.2,)})`
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
