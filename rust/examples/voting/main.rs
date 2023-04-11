use clap::Parser;
mod leader;
mod participant;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Service {
    Leader,
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
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    frankenpaxos::serve_prometheus(args.prometheus_host, args.prometheus_port);

    match args.service {
        Service::Leader => leader::run(args.leader).await,
        Service::Participant => participant::run(args.participant).await,
    };
}
