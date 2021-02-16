// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use warp::Filter;

use crate::clusters::ClusterRef;
use crate::error::FuseQueryResult;

pub fn cluster_nodes_handler(
    cluster: ClusterRef,
) -> FuseQueryResult<impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone> {
    let nodes = cluster.get_nodes()?;
    Ok(warp::path!("v1" / "cluster" / "nodes").map(move || format!("{:?}", nodes)))
}
