use clap::Parser;
mod server;

#[derive(clap::Subcommand, Debug)]
enum Service {
    Server(server::ServerArgs),
}

#[derive(Parser, Debug)]
struct Args {
    #[clap(subcommand)]
    service: Service,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.service {
        Service::Server(cfg) => server::run_server(cfg).await,
    };
}
