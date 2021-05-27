// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod optimizer_constant_folding_test;
#[cfg(test)]
mod optimizer_projection_push_down_test;
#[cfg(test)]
mod optimizer_test;

mod optimizer;
mod optimizer_constant_folding;
mod optimizer_projection_push_down;

pub use optimizer::IOptimizer;
pub use optimizer::Optimizer;
pub use optimizer_constant_folding::ConstantFoldingOptimizer;
pub use optimizer_projection_push_down::ProjectionPushDownOptimizer;
