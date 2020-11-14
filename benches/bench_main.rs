// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::bench_pipeline_executor_count::benches,
    benchmarks::bench_pipeline_executor_sum::benches,
    benchmarks::bench_pipeline_executor_max::benches,
}
