use metrics::counter;

pub fn metrics_incr_http_request_count(method: &str, api: &str, status: u16) {
    let labels = [("method", method), ("api", api), ("status", status)];
    counter!("query_http_requests_count", 1.0, labels);
}

pub fn metrics_incr_http_slow_request_count(method: &str, api: &str, status: u16) {
    let labels = [("method", method), ("api", api), ("status", status)];
    counter!("query_http_slow_requests_count", 1.0, labels);
}
