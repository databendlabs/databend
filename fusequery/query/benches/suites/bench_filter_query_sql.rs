// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;

use crate::suites::criterion_benchmark_suite;

fn criterion_benchmark_filter_query(c: &mut Criterion) {
    let queries = vec!["SELECT number FROM numbers_mt(10000000) WHERE number>100 AND number<200"];

    for query in queries {
        criterion_benchmark_suite(c, query);
    }
}

criterion_group!(benches, criterion_benchmark_filter_query);
criterion_main!(benches);
