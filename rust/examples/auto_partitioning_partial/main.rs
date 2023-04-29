use clap::Parser;
mod leader;
mod replica;
mod coordinator;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Service {
    Leader,
    Replica,
    Coordinator,
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
    leader: leader::LeaderArgs,

    #[clap(flatten)]
    replica: replica::ReplicaArgs,

    #[clap(flatten)]
    coordinator: coordinator::CoordinatorArgs,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    frankenpaxos::serve_prometheus(args.prometheus_host, args.prometheus_port);
    let ports = hydroflow::util::cli::init().await;

    match args.service {
        Service::Leader => leader::run(args.leader, ports).await,
        Service::Replica => replica::run(args.replica, ports).await,
        Service::Coordinator => coordinator::run(args.coordinator, ports).await,
    };
}
