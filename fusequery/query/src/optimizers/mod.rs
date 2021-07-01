// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod optimizer_constant_folding_test;
#[cfg(test)]
mod optimizer_projection_push_down_test;
#[cfg(test)]
mod optimizer_scatters_test;
#[cfg(test)]
mod optimizer_test;

mod optimizer;
mod optimizer_constant_folding;
mod optimizer_projection_push_down;
mod optimizer_scatters;
mod optimizer_statistics_exact;

pub use optimizer::Optimizer;
pub use optimizer::Optimizers;
pub use optimizer_constant_folding::ConstantFoldingOptimizer;
pub use optimizer_projection_push_down::ProjectionPushDownOptimizer;
pub use optimizer_scatters::ScattersOptimizer;
pub use optimizer_statistics_exact::StatisticsExactOptimizer;
