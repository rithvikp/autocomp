use clap::Parser;
mod acceptor;
mod coordinator;
mod leader;
mod p2a_proxy_leader;
mod p2b_proxy_leader;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Service {
    Leader,
    Acceptor,
    Coordinator,
    P2aProxyLeader,
    P2bProxyleader,
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
    acceptor: acceptor::AcceptorArgs,

    #[clap(flatten)]
    coordinator: coordinator::CoordinatorArgs,

    #[clap(flatten)]
    p2a_proxy_leader: p2a_proxy_leader::P2aProxyLeaderArgs,

    #[clap(flatten)]
    p2b_proxy_leader: p2b_proxy_leader::P2bProxyLeaderArgs,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    frankenpaxos::serve_prometheus(args.prometheus_host, args.prometheus_port);
    let ports = hydroflow::util::cli::init().await;

    match args.service {
        Service::Leader => leader::run(args.leader, ports).await,
        Service::Acceptor => acceptor::run(args.acceptor, ports).await,
        Service::Coordinator => coordinator::run(args.coordinator, ports).await,
        Service::P2aProxyLeader => p2a_proxy_leader::run(args.p2a_proxy_leader, ports).await,
        Service::P2bProxyleader => p2b_proxy_leader::run(args.p2b_proxy_leader, ports).await,
    };
}
