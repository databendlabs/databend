// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use warp::Filter;

use crate::configs::Config;
use crate::error::FuseQueryResult;

pub fn config_handler(
    cfg: Config,
) -> FuseQueryResult<impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone> {
    Ok(warp::path!("v1" / "config").map(move || format!("{:?}", cfg)))
}
