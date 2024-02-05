use clap::Parser;
mod pbft_replica;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Service {
    PBFTReplica,
}

#[derive(Parser, Debug)]
struct Args {
    #[clap(long)]
    service: Service,

    #[clap(long, default_value = "0.0.0.0")]
    prometheus_host: String,
    #[clap(long, default_value = "-1")]
    prometheus_port: i32,

    #[clap(flatten)]
    pbft_replica: pbft_replica::PBFTReplicaArgs,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    frankenpaxos::serve_prometheus(args.prometheus_host, args.prometheus_port);
    let ports = hydroflow::util::cli::init().await;

    match args.service {
        Service::PBFTReplica => pbft_replica::run(args.pbft_replica, ports).await,
    };
}
