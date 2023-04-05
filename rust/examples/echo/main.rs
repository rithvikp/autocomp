use clap::Parser;
mod server;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Service {
    Server,
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
    server: server::ServerArgs,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    frankenpaxos::serve_prometheus(args.prometheus_host, args.prometheus_port);

    match args.service {
        Service::Server => server::run_server(args.server).await,
    };
}
