use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(
        &[
            "../shared/src/main/scala/frankenpaxos/echo/Echo.proto",
            "../shared/src/main/scala/frankenpaxos/voting/Voting.proto",
            "../shared/src/main/scala/frankenpaxos/multipaxos/MultiPaxos.proto",
        ],
        &[
            "../shared/src/main/scala/frankenpaxos",
            // TODO: This is a hack to allow the scalapb import to be resolved.
            "../jvm/target/protobuf_external",
        ],
    )?;
    Ok(())
}
