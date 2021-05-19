// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod optimizer_projection_push_down_test;
#[cfg(test)]
mod optimizer_scatters_test;

mod optimizer;
mod optimizer_projection_push_down;
mod optimizer_scatters;


pub use optimizer::IOptimizer;
pub use optimizer::Optimizer;
pub use optimizer_projection_push_down::ProjectionPushDownOptimizer;
pub use optimizer_scatters::ScattersOptimizer;
