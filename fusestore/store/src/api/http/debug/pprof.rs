// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use warp::Filter;

use crate::api::http::debug::PProfRequest;
use crate::configs::Config;

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
    use common_tracing::tracing;

    use crate::api::http::debug::pprof::PProfRequest;

    pub async fn pprof(
        header: Option<String>,
        req: PProfRequest,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        let body;
        let duration = Duration::from_secs(req.seconds);
        let profile = Profiling::create(duration, req.frequency.get());

        tracing::info!("start pprof request:{:?}", req);
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
        tracing::info!("finished pprof request:{:?}", req);
        Ok(warp::reply::html(body))
    }
}
