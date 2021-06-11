// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use warp::Filter;

use crate::configs::Config;

pub fn pprof_handler(
    _cfg: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("debug" / "pprof")
        .and(warp::get())
        .and_then(handlers::pprof)
}

mod handlers {
    use std::time::Duration;

    use common_profling::Profiling;

    pub async fn pprof() -> Result<impl warp::Reply, std::convert::Infallible> {
        let duration = Duration::from_secs(10);
        let profile = Profiling::create(duration);
        let body = profile.dump().await.unwrap();
        Ok(warp::reply::html(body))
    }
}
