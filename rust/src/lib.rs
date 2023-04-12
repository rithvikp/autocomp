pub fn serve_prometheus(host: String, port: i32) {
    if port == -1 {
        return;
    }
    let addr = format!("{}:{}", host, port).parse().unwrap();
    prometheus_exporter::start(addr)
        .expect(format!("failed to start Prometheus for host {host}, port {port}").as_str());
}

pub mod echo_proto {
    include!(concat!(env!("OUT_DIR"), "/frankenpaxos.echo.rs"));
}

pub mod voting_proto {
    include!(concat!(env!("OUT_DIR"), "/frankenpaxos.voting.rs"));
}
