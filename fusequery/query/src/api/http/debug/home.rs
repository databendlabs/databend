// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::num::NonZeroI32;

use common_exception::Result;
use warp::Filter;
use warp::Rejection;
use warp::Reply;

use crate::api::http::debug::pprof::pprof_handler;
use crate::configs::Config;
use crate::sessions::SessionManagerRef;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PProfRequest {
    #[serde(default = "PProfRequest::default_seconds")]
    pub(crate) seconds: u64,
    #[serde(default = "PProfRequest::default_frequency")]
    pub(crate) frequency: NonZeroI32,
}

impl PProfRequest {
    fn default_seconds() -> u64 {
        5
    }
    fn default_frequency() -> NonZeroI32 {
        NonZeroI32::new(99).unwrap()
    }
}

pub struct DebugRouter {
    sessions: SessionManagerRef,
}

impl DebugRouter {
    pub fn create(sessions: SessionManagerRef) -> Self {
        DebugRouter { sessions }
    }

    pub fn build(&self) -> Result<impl Filter<Extract = impl Reply, Error = Rejection> + Clone> {
        let cfg = self.sessions.get_conf();
        Ok(warp::path!("v1" / "configs").map(move || format!("{:?}", cfg)))
    }
}

pub fn debug_handler(
    cfg: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    debug_home_handler().or(pprof_handler(cfg))
}

fn debug_home_handler() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
{
    warp::path!("debug").map(move || {
        warp::reply::html(format!(
            r#"<a href="/debug/pprof/profile?seconds={}">pprof/profile</a>"#,
            PProfRequest::default_seconds()
        ))
    })
}
