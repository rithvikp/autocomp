use clap::Parser;
mod broadcaster;
mod collector;
mod leader;
mod participant;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Service {
    Leader,
    Collector,
    Broadcaster,
    Participant,
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
    participant: participant::ParticipantArgs,

    #[clap(flatten)]
    collector: collector::CollectorArgs,

    #[clap(flatten)]
    broadcaster: broadcaster::BroadcasterArgs,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let ports = hydroflow::util::cli::init().await;

    frankenpaxos::serve_prometheus(args.prometheus_host, args.prometheus_port);

    match args.service {
        Service::Leader => leader::run(args.leader, ports).await,
        Service::Collector => collector::run(args.collector, ports).await,
        Service::Broadcaster => broadcaster::run(args.broadcaster, ports).await,
        Service::Participant => participant::run(args.participant, ports).await,
    };
}
