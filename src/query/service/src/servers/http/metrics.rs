use metrics::counter;

pub fn metrics_incr_http_request_count(method: String, api: String, status: String) {
    let labels = [("method", method), ("api", api), ("status", status)];
    counter!("query_http_requests_count", 1, &labels);
}

pub fn metrics_incr_http_slow_request_count(method: String, api: String, status: String) {
    let labels = [("method", method), ("api", api), ("status", status)];
    counter!("query_http_slow_requests_count", 1, &labels);
}
