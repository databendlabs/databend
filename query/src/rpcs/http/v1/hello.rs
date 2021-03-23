// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use fuse_query_configs::Config;
use warp::Filter;

pub fn hello_handler(
    cfg: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "hello").map(move || format!("{:?}", cfg))
}
