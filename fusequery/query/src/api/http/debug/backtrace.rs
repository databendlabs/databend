// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use warp::Filter;

use crate::configs::Config;

pub fn backtrace_handler(
    _cfg: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("debug" / "backtrace")
        .and(warp::get())
        .and(warp::header::optional::<String>("Accept"))
        .and_then(handlers::backtrace)
}

mod handlers {
    use backtrace::Backtrace;

    pub async fn backtrace(
        _header: Option<String>,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        let bt = Backtrace::new();

        Ok(warp::reply::html(format!("{:?}", bt)))
    }
}
