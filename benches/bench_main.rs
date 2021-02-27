// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::bench_pipeline::benches,
    benchmarks::bench_aggregator::benches,
}
