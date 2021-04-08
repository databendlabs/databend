// Copyright 2020 The VectorQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
pub mod tests;

pub mod api;
pub mod configs;
pub mod engine;
pub mod metrics;

#[allow(clippy::all)]
pub mod protobuf {
    // tonic::include_proto!("store_meta");
    include!(concat!(env!("OUT_DIR"), concat!("/store_meta.rs")));
}
