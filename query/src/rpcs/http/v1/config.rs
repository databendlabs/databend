// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use fuse_query_configs::Config;
use warp::Filter;

pub fn config_handler(
    cfg: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "configs").map(move || format!("{:?}", cfg))
}
