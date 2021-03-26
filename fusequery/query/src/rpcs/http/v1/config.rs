// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use warp::Filter;

use crate::configs::Config;

pub fn config_handler(
    cfg: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "configs").map(move || format!("{:?}", cfg))
}
