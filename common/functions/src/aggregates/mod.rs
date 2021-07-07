// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod aggregate_batch_test;
#[cfg(test)]
mod aggregate_combinator_test;
#[cfg(test)]
mod aggregate_function_test;

mod aggregate_arg_max;
mod aggregate_arg_min;
mod aggregate_avg;
mod aggregate_combinator_distinct;
mod aggregate_combinator_if;
mod aggregate_count;
mod aggregate_function;
mod aggregate_function_factory;
mod aggregate_max;
mod aggregate_min;
mod aggregate_sum;
mod aggregator;
mod aggregator_common;

pub use aggregate_arg_max::AggregateArgMaxFunction;
pub use aggregate_arg_min::AggregateArgMinFunction;
pub use aggregate_avg::AggregateAvgFunction;
pub use aggregate_combinator_distinct::AggregateDistinctCombinator;
pub use aggregate_combinator_if::AggregateIfCombinator;
pub use aggregate_count::AggregateCountFunction;
pub use aggregate_function::AggregateFunction;
pub use aggregate_function_factory::AggregateFunctionFactory;
pub use aggregate_max::AggregateMaxFunction;
pub use aggregate_min::AggregateMinFunction;
pub use aggregate_sum::AggregateSumFunction;
pub use aggregator::Aggregators;
pub use aggregator_common::*;
