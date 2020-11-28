// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::bench_pipeline_filter::benches,
}
