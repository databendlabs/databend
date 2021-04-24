// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod optimizer_filter_push_down_test;
#[cfg(test)]
mod optimizer_group_push_down_test;
#[cfg(test)]
mod optimizer_limit_push_down_test;

mod optimizer;
mod optimizer_common;
mod optimizer_filter_push_down;
mod optimizer_group_push_down;
mod optimizer_limit_push_down;

pub use optimizer::IOptimizer;
pub use optimizer::Optimizer;
pub use optimizer_common::OptimizerCommon;
pub use optimizer_filter_push_down::FilterPushDownOptimizer;
pub use optimizer_group_push_down::GroupByPushDownOptimizer;
pub use optimizer_limit_push_down::LimitPushDownOptimizer;
