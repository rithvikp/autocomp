pub fn serve_prometheus(host: String, port: i32) {
    if port == -1 {
        return;
    }
    let addr = format!("{}:{}", host, port).parse().unwrap();
    prometheus_exporter::start(addr).unwrap();
}
