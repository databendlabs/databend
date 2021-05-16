// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod aggregator_test;

mod aggregator;
mod aggregate_avg;
mod aggregate_count;
mod aggregate_max;
mod aggregate_min;
mod aggregate_sum;
mod aggregate_function_factory;
mod aggregate_function;

mod test;
pub use test::*;

pub use aggregate_function::AggregateFunctionCtx;
pub use aggregate_function::IAggreagteFunction;
pub use aggregate_function_factory::AggregateFunctionFactory;
pub use aggregator::AggregatorFunction;
pub use aggregate_avg::AggregateAvgFunction;
pub use aggregate_count::AggregateCountFunction;
pub use aggregate_max::AggregateMaxFunction;
pub use aggregate_min::AggregateMinFunction;
pub use aggregate_sum::AggregateSumFunction;
