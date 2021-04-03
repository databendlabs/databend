// Copyright 2020 The VectorQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
pub mod tests;

pub mod api;
pub mod clusters;
pub mod configs;
pub mod datasources;
pub mod interpreters;
pub mod metrics;
pub mod optimizers;
pub mod pipelines;
pub mod planners;
pub mod servers;
pub mod sessions;
pub mod sql;

pub use common_datablocks;
pub use common_datavalues;
pub use common_functions;
pub use common_planners;
pub use common_streams;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("queryrpc");
    tonic::include_proto!("queryflight");
}
