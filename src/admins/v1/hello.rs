// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use warp::Filter;

use crate::configs::Config;
use crate::error::FuseQueryResult;

pub fn hello_handler(
    cfg: Config,
) -> FuseQueryResult<impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone> {
    Ok(warp::path!("v1" / "hello").map(move || format!("{:?}", cfg)))
}
