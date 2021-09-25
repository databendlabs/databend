// Copyright 2020 Datafuse Labs.
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

use axum::body::Body;
use axum::extract::Query;
use axum::response::IntoResponse;
use common_base::Profiling;
use common_base::tokio::time::Duration;
use common_tracing::tracing;

use crate::api::http::debug::PProfRequest;

// run pprof
// example: /debug/pprof/profile?seconds=5&frequency=99
// req query contains pprofrequest information
pub async fn debug_pprof_handler(req: Option<Query<PProfRequest>>) -> impl IntoResponse {
    let profile = match req {
        Some(query) => {
            let duration = Duration::from_secs(query.seconds);
            tracing::info!(
                "start pprof request second: {:?} frequency: {:?}",
                query.seconds,
                query.frequency
            );
            Profiling::create(duration, i32::from(query.frequency))
        }
        None => {
            let duration = Duration::from_secs(PProfRequest::default_seconds());
            tracing::info!(
                "start pprof request second: {:?} frequency: {:?}",
                PProfRequest::default_seconds(),
                PProfRequest::default_frequency()
            );
            Profiling::create(duration, i32::from(PProfRequest::default_frequency()))
        }
    };
    let body = profile.dump_flamegraph().await.unwrap();

    tracing::info!("finished pprof request");
    Body::from(body).into_response()
}
