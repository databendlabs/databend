// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
