// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
