// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::bench_pipeline::benches,
    benchmarks::bench_aggregator::benches,
}
