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
