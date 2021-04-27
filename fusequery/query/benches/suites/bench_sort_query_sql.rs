// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;

use crate::suites::criterion_benchmark_suite;

fn criterion_benchmark_sort_query(c: &mut Criterion) {
    let queries = vec![
        "SELECT number FROM numbers_mt(10000000) ORDER BY number DESC LIMIT 10",
        "SELECT number FROM numbers_mt(10000000) ORDER BY number ASC LIMIT 10",
    ];

    for query in queries {
        criterion_benchmark_suite(c, query);
    }
}

criterion_group!(benches, criterion_benchmark_sort_query);
criterion_main!(benches);
