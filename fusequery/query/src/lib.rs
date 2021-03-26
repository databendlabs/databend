// Copyright 2020 The VectorQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

mod error_test;

pub mod tests;

pub mod datasources;
pub mod datastreams;
pub mod error;
pub mod interpreters;
pub mod optimizers;
pub mod planners;
pub mod processors;
pub mod rpcs;
pub mod servers;
pub mod sessions;
pub mod sql;
pub mod transforms;

pub use common_datablocks;
pub use common_datavalues;
pub use common_functions;
pub use common_planners;
pub use fuse_query_clusters as clusters;
pub use fuse_query_configs as configs;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("queryrpc");
    tonic::include_proto!("queryflight");
}
