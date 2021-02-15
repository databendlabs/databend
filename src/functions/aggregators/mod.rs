// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

mod aggregator_test;

mod aggregator;
mod aggregator_avg;
mod aggregator_count;
mod aggregator_max;
mod aggregator_min;
mod aggregator_sum;

pub use aggregator::AggregatorFunction;
pub use aggregator_avg::AggregatorAvgFunction;
pub use aggregator_count::AggregatorCountFunction;
pub use aggregator_max::AggregatorMaxFunction;
pub use aggregator_min::AggregatorMinFunction;
pub use aggregator_sum::AggregatorSumFunction;
