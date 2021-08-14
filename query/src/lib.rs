// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#![feature(hash_raw_entry)]
#![feature(core_intrinsics)]
#![feature(arbitrary_self_types)]

#[cfg(test)]
pub mod tests;

pub mod api;
pub mod catalogs;
pub mod clusters;
pub mod configs;
pub mod datasources;
pub mod functions;
pub mod interpreters;
pub mod metrics;
pub mod optimizers;
pub mod pipelines;
pub mod servers;
pub mod sessions;
pub mod sql;
pub mod common;
