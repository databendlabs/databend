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

use axum::extract::TypedHeader;
use axum::extract::Query;
use headers::ContentType;
use crate::api::http::debug::PProfRequest;
use common_runtime::tokio::time::Duration;
use common_profling::Profiling;
use axum::response::IntoResponse;
use axum::response::Html;
use common_tracing::tracing;

// run pprof
// example: /debug/pprof/profile?seconds=5&frequency=99
// req query contains pprofrequest information
pub async fn debug_pprof_handler(content_type: Option<TypedHeader<ContentType>>, req: Option<Query<PProfRequest>>) -> impl IntoResponse {
    let profile = match req {
        Some(query)=> {
            let duration = Duration::from_secs(query.seconds);
            tracing::info!("start pprof request second: {:?} frequency: {:?}", query.seconds, query.frequency);
            Profiling::create(duration, i32::from(query.frequency))
        }
        None => {
            let duration = Duration::from_secs(PProfRequest::default_seconds());
            tracing::info!("start pprof request second: {:?} frequency: {:?}", PProfRequest::default_seconds(), PProfRequest::default_frequency());
            Profiling::create(duration, i32::from(PProfRequest::default_frequency()))
        }
    };
    let body;
    if content_type.is_some() && content_type.unwrap().0.to_string().eq("text/html") {
        body = profile.dump_flamegraph().await.unwrap();
    } else {
        body = profile.dump_proto().await.unwrap();
    }

    tracing::info!("finished pprof request");
    Html(body).into_response()

}