// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;

use crate::benchmarks::criterion_benchmark_suite;

fn criterion_benchmark_aggregate_query(c: &mut Criterion) {
    let queries = vec![
        "SELECT MIN(number) FROM numbers_mt(10000000)",
        "SELECT MAX(number) FROM numbers_mt(10000000)",
        "SELECT COUNT(number) FROM numbers_mt(10000000)",
        "SELECT SUM(number) FROM numbers_mt(10000000)",
        "SELECT AVG(number) FROM numbers_mt(10000000)",
        "SELECT COUNT(number) FROM numbers_mt(10000000) WHERE number>10 and number<20",
        "SELECT MIN(number), MAX(number), AVG(number), COUNT(number) FROM numbers_mt(10000000)",
        "SELECT COUNT(number) FROM numbers_mt(1000000) GROUP BY number%3",
        "SELECT COUNT(number) FROM numbers_mt(1000000) GROUP BY number%3, number%4",
    ];

    for query in queries {
        criterion_benchmark_suite(c, query);
    }
}

criterion_group!(benches, criterion_benchmark_aggregate_query);
criterion_main!(benches);
