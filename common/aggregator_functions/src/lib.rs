// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod aggregator_test;

mod aggregator;
mod aggregator_avg;
mod aggregator_count;
mod aggregator_max;
mod aggregator_min;
mod aggregator_sum;
mod aggregator_function_factory;

pub use aggregator::AggregatorFunction;
pub use aggregator_avg::AggregatorAvgFunction;
pub use aggregator_count::AggregatorCountFunction;
pub use aggregator_max::AggregatorMaxFunction;
pub use aggregator_min::AggregatorMinFunction;
pub use aggregator_sum::AggregatorSumFunction;
