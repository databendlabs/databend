// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use fuse_query::datavalues::*;

macro_rules! bench_suit {
    ($C: expr, $OP: expr, $ARR: expr) => {{
        $C.bench_function(format!("{}", $OP).as_str(), |b| {
            b.iter(|| {
                let _ = data_array_aggregate_op($OP, $ARR.clone());
            })
        });
    };};
}

fn criterion_benchmark_aggregator(c: &mut Criterion) {
    let data: Vec<u64> = (0..1000000).collect();
    let arr = Arc::new(UInt64Array::from(data));
    let _ = bench_suit!(c, DataValueAggregateOperator::Count, arr.clone());
    let _ = bench_suit!(c, DataValueAggregateOperator::Max, arr.clone());
    let _ = bench_suit!(c, DataValueAggregateOperator::Min, arr.clone());
    let _ = bench_suit!(c, DataValueAggregateOperator::Avg, arr.clone());
    let _ = bench_suit!(c, DataValueAggregateOperator::Sum, arr.clone());
}

criterion_group!(benches, criterion_benchmark_aggregator,);
criterion_main!(benches);
