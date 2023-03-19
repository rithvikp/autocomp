use clap::Parser;
mod server;

#[derive(clap::Subcommand, Debug)]
enum Service {
    Server(server::ServerArgs),
}

#[derive(Parser, Debug)]
struct Args {
    #[clap(long, default_value = "0.0.0.0")]
    prometheus_host: String,
    #[clap(long, default_value = "8009")]
    prometheus_port: i32,
    #[clap(subcommand)]
    service: Service,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    frankenpaxos::serve_prometheus(args.prometheus_host, args.prometheus_port);

    match args.service {
        Service::Server(cfg) => server::run_server(cfg).await,
    };
}
