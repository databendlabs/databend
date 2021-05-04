// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod optimizer_alias_push_down_test;
#[cfg(test)]
mod optimizer_limit_push_down_test;
#[cfg(test)]
mod optimizer_projection_push_down_test;

mod optimizer;
mod optimizer_alias_push_down;
mod optimizer_limit_push_down;
mod optimizer_projection_push_down;

pub use optimizer::IOptimizer;
pub use optimizer::Optimizer;
pub use optimizer_alias_push_down::AliasPushDownOptimizer;
pub use optimizer_limit_push_down::LimitPushDownOptimizer;
pub use optimizer_projection_push_down::ProjectionPushDownOptimizer;
