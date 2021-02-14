// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use warp::Filter;

use crate::configs::Config;

pub fn hello_handler(
    cfg: Config,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "hello").map(move || format!("{:?}", cfg))
}
