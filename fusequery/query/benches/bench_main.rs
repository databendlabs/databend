// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use criterion::criterion_main;

mod suites;

criterion_main! {
    suites::bench_aggregate_query_sql::benches,
    suites::bench_filter_query_sql::benches,
    suites::bench_limit_query_sql::benches,
    suites::bench_sort_query_sql::benches,
}
