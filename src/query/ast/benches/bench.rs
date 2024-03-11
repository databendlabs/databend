// Copyright 2021 Datafuse Labs
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

#[macro_use]
extern crate criterion;

use criterion::black_box;
use criterion::Criterion;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_parser");
    group.sample_size(10);

    group.bench_function("large_statement", |b| {
        b.iter(|| {
            let case = r#"explain SELECT SUM(count) FROM (SELECT ((((((((((((true)and(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))or((('780820706')=('')))) IS NOT NULL AND ((((((((((true)AND(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))OR((('780820706')=(''))))) ::INT64)as count FROM t0) as res;"#;
            let tokens = tokenize_sql(case).unwrap();
            let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
            black_box(stmt);
        })
    });
    group.bench_function("large_query", |b| {
        b.iter(|| {
            let case = r#"SELECT SUM(count) FROM (SELECT ((((((((((((true)and(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))or((('780820706')=('')))) IS NOT NULL AND ((((((((((true)AND(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))OR((('780820706')=(''))))) ::INT64)as count FROM t0) as res;"#;
            let tokens = tokenize_sql(case).unwrap();
            let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
            black_box(stmt);
        })
    });
    group.bench_function("deep_query", |b| {
        b.iter(|| {
            let case = r#"SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers"#;
            let tokens = tokenize_sql(case).unwrap();
            let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
            black_box(stmt);
        })
    });
    group.bench_function("wide_expr", |b| {
        b.iter(|| {
            let case = r#"a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a"#;
            let tokens = tokenize_sql(case).unwrap();
            let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
            black_box(expr);
        })
    });
    group.bench_function("deep_expr", |b| {
        b.iter(|| {
            let case = r#"((((((((((((((((((((((((((((((1))))))))))))))))))))))))))))))"#;
            let tokens = tokenize_sql(case).unwrap();
            let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
            black_box(expr);
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
