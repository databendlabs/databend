// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::clusters::ClusterRef;
use warp::Filter;

pub fn cluster_nodes_handler(
    cluster: ClusterRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "cluster" / "nodes")
        .map(move || format!("{:?}", cluster.get_nodes().unwrap()))
}
