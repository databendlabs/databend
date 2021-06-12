// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::num::NonZeroI32;

use warp::Filter;

use crate::configs::Config;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PProfRequest {
    #[serde(default = "PProfRequest::default_seconds")]
    seconds: u64,
    #[serde(default = "PProfRequest::default_frequency")]
    frequency: NonZeroI32,
}

impl PProfRequest {
    fn default_seconds() -> u64 {
        30
    }
    fn default_frequency() -> NonZeroI32 {
        NonZeroI32::new(99).unwrap()
    }
}

pub fn pprof_handler(
    _cfg: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("debug" / "pprof" / "profile")
        .and(warp::get())
        .and(warp::header::optional::<String>("Accept"))
        .and(warp::query::<PProfRequest>())
        .and_then(handlers::pprof)
}

mod handlers {
    use std::time::Duration;

    use common_profling::Profiling;

    use crate::api::http::debug::pprof::PProfRequest;

    pub async fn pprof(
        header: Option<String>,
        req: PProfRequest,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        let body;
        let duration = Duration::from_secs(req.seconds);
        let profile = Profiling::create(duration, req.frequency.get());

        if let Some(accept) = header {
            // Browser.
            if accept.contains("text/html") {
                body = profile.dump_flamegraph().await.unwrap();
            } else {
                body = profile.dump_proto().await.unwrap();
            }
        } else {
            body = profile.dump_proto().await.unwrap();
        }
        Ok(warp::reply::html(body))
    }
}
