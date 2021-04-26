// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::bench_aggregate_query_sql::benches,
}
