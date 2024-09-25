use std::any::Any;

use databend_common_metrics::http::metrics_incr_http_response_panics_count;
use http::StatusCode;

#[derive(Clone, Debug)]
pub(crate) struct PanicHandler {}

impl PanicHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl poem::middleware::PanicHandler for PanicHandler {
    type Response = (StatusCode, &'static str);

    fn get_response(&self, _err: Box<dyn Any + Send + 'static>) -> Self::Response {
        metrics_incr_http_response_panics_count();
        (StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    }
}
