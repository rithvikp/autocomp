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

    let to_replica = ports
        .remove("send_to$replicas$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;
    let peers = to_replica.keys.clone();
    let to_replica_sink = to_replica.into_sink();

    let ballot_to_collector = ports
        .remove("send_to$collectors$0")
        .unwrap()
        .connect::<ConnectedDemux<ConnectedBidi>>()
        .await;
    let collectors = ballot_to_collector.keys.clone();
    let ballot_to_collector_sink = ballot_to_collector.into_sink();

    let df = datalog!(
        r#"
        .input replicas `repeat_iter(peers.clone()) -> map(|p| (p,))`
.input collector `repeat_iter(collectors.clone()) -> map(|p| (p,))`

.async clientIn `null::<(u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(client_recv) -> map(|x| {let input = x.unwrap(); let v = decrypt_and_deserialize(input.1, &private_key); (input.0, v.0, v.1, v.2,)})`
.async voteToReplica `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(to_replica_sink)` `null::<(u32,i64,u32,Rc<Vec<u8>>)>()`
.async ballotToCollector `map(|(node_id, v)| (node_id, serialize_to_bytes(v))) -> dest_sink(ballot_to_collector_sink)` `null::<(u32,i64,u32)>()`

# ballot is guaranteed to either be empty or contain the 1 current ballot
newBallots(b) :- clientIn(client, id, b, v)
newBallots(b) :- ballot(b)
MaxNewBallot(max(b)) :- newBallots(b)
ballot(b) :+ MaxNewBallot(b)

voteToReplica@addr(client, id, b, v) :~ clientIn(client, id, b, v), replicas(addr)
# Record ballot at the time clientIn arrived
writeTimeBallot(client, id, b) :- clientIn(client, id, _, _), ballot(b)
writeTimeBallot(client, id, 0) :- clientIn(client, id, _, _), !ballot(b)
ballotToCollector@addr(client, id, b) :~ writeTimeBallot(client, id, b), collector(addr)
        "#
    );

    launch_flow(df).await
}
