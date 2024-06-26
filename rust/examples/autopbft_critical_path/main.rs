use clap::Parser;
mod leader;
mod proxy_leader;
mod prepreparer;
mod preparer;
mod committer;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Service {
    Leader,
    ProxyLeader,
    Prepreparer,
    Preparer,
    Committer,
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
    proxy_leader: proxy_leader::ProxyLeaderArgs,

    #[clap(flatten)]
    prepreparer: prepreparer::PrepreparerArgs,

    #[clap(flatten)]
    preparer: preparer::PreparerArgs,

    #[clap(flatten)]
    committer: committer::CommitterArgs,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    frankenpaxos::serve_prometheus(args.prometheus_host, args.prometheus_port);
    let ports = hydroflow::util::cli::init().await;

    match args.service {
        Service::Leader => leader::run(args.leader, ports).await,
        Service::ProxyLeader => proxy_leader::run(args.proxy_leader, ports).await,
        Service::Prepreparer => prepreparer::run(args.prepreparer, ports).await,
        Service::Preparer => preparer::run(args.preparer, ports).await,
        Service::Committer => committer::run(args.committer, ports).await,
    };
}
