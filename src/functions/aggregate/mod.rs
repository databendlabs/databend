// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

mod tests;

mod aggregate_count;
mod aggregate_max;
mod aggregate_sum;

pub use self::aggregate_count::CountAggregateFunction;
pub use self::aggregate_max::MaxAggregateFunction;
pub use self::aggregate_sum::SumAggregateFunction;
