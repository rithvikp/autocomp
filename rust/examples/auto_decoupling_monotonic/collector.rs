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
use rand::rngs::ThreadRng;
use rsa::pkcs8::DecodePublicKey;
use rsa::{RsaPublicKey, Pkcs1v15Encrypt};
use std::rc::Rc;
use std::{collections::HashMap, io::Cursor};

#[derive(clap::Args, Debug)]
pub struct CollectorArgs {
    #[clap(long = "collector.num-replicas")]
    collector_num_replicas: Option<u32>,
}

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
                    id: id,
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

pub async fn run(cfg: CollectorArgs, mut ports: HashMap<String, ServerOrBound>) {
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

    let from_replica_source = ports
        .remove("receive_from$replicas$0")
        .unwrap()
        .connect::<ConnectedTagged<ConnectedBidi>>()
        .await
        .into_source();

    let ballot_from_leader_source = ports
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

    let num_replicas = cfg.collector_num_replicas.unwrap();

    let df = datalog!(
        r#"
        .input numReplicas `repeat_iter([(num_replicas,),])`

.async clientOut `map(|(node_id, (id, ballot, payload,))| (node_id, encrypt_and_serialize(id, ballot, payload, &mut rng, &public_key))) -> dest_sink(client_send)` `null::<(i64,u32,Rc<Vec<u8>>)>()`
.async voteFromReplica `null::<(u32,u32,i64,u32,Rc<Vec<u8>>)>()` `source_stream(from_replica_source) -> map(|v| deserialize_from_bytes::<(u32,u32,i64,u32,Rc<Vec<u8>>)>(v.unwrap().1).unwrap())`
.async ballotFromLeader `null::<(u32,i64,u32)>()` `source_stream(ballot_from_leader_source) -> map(|v| deserialize_from_bytes::<(u32,i64,u32)>(v.unwrap().1).unwrap())`


# Record ballot at the time clientIn arrived
writeTimeBallot(client, id, b) :- ballotFromLeader(client, id, b)
writeTimeBallot(client, id, b) :+ writeTimeBallot(client, id, b), !committed(client, id, _, _)

allVotes(l, client, id, b, v) :- voteFromReplica(l, client, id, b, v)
allVotes(l, client, id, b, v) :+ allVotes(l, client, id, b, v), !committed(client, id, _, _)
voteCounts(count(l), client, id) :- allVotes(l, client, id, b, v)
committed(client, id, b, v) :- voteCounts(n, client, id), numReplicas(n), allVotes(l, client, id, b, v) 
# Attach ballot at the time clientIn arrived to the output
clientOut@client(id, b, v) :~ committed(client, id, _, v), writeTimeBallot(client, id, b)
        "#
    );

    launch_flow(df).await
}
