use clap::Parser;
mod committer;
mod ender;
mod leader;
mod participant_acker;
mod participant_voter;
mod vote_requester;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Service {
    Leader,
    VoteRequester,
    ParticipantVoter,
    Committer,
    ParticipantAcker,
    Ender,
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
    vote_requester: vote_requester::VoteRequesterArgs,

    #[clap(flatten)]
    participant_voter: participant_voter::ParticipantVoterArgs,

    #[clap(flatten)]
    committer: committer::CommitterArgs,

    #[clap(flatten)]
    participant_acker: participant_acker::ParticipantAckerArgs,

    #[clap(flatten)]
    ender: ender::EnderArgs,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    frankenpaxos::serve_prometheus(args.prometheus_host, args.prometheus_port);
    let ports = hydroflow::util::cli::init().await;

    match args.service {
        Service::Leader => leader::run(args.leader, ports).await,
        Service::VoteRequester => vote_requester::run(args.vote_requester, ports).await,
        Service::ParticipantVoter => participant_voter::run(args.participant_voter, ports).await,
        Service::Committer => committer::run(args.committer, ports).await,
        Service::ParticipantAcker => participant_acker::run(args.participant_acker, ports).await,
        Service::Ender => ender::run(args.ender, ports).await,
    };
}
