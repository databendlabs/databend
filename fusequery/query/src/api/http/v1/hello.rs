// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use warp::Filter;
use warp::Rejection;
use warp::Reply;

use crate::configs::Config;
use crate::sessions::SessionManagerRef;

pub struct HelloRouter {
    sessions: SessionManagerRef,
}

impl HelloRouter {
    pub fn create(sessions: SessionManagerRef) -> Self {
        HelloRouter { sessions }
    }

    pub fn build(&self) -> Result<impl Filter<Extract = impl Reply, Error = Rejection> + Clone> {
        let cfg = self.sessions.get_conf();
        Ok(warp::path!("v1" / "hello").map(move || format!("{:?}", cfg)))
    }
}
