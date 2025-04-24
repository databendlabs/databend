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

fn main() {
    divan::main()
}

// bench                  fastest       │ slowest       │ median        │ mean          │ samples │ iters
// ╰─ dummy                             │               │               │               │         │
//    ├─ deep_query       122 µs        │ 324.3 µs      │ 127.2 µs      │ 130.1 µs      │ 100     │ 100
//    ├─ large_query      1.366 ms      │ 1.686 ms      │ 1.409 ms      │ 1.417 ms      │ 100     │ 100
//    ├─ large_statement  1.336 ms      │ 1.441 ms      │ 1.391 ms      │ 1.39 ms       │ 100     │ 100
//    ╰─ wide_expr        556 µs        │ 697.2 µs      │ 578.3 µs      │ 580.5 µs      │ 100     │ 100
#[divan::bench_group(max_time = 0.5)]
mod dummy {
    use databend_common_ast::parser::parse_expr;
    use databend_common_ast::parser::parse_sql;
    use databend_common_ast::parser::tokenize_sql;
    use databend_common_ast::parser::Dialect;

    #[divan::bench]
    fn large_statement() {
        let case = r#"explain SELECT SUM(count) FROM (SELECT ((((((((((((true)and(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))or((('780820706')=('')))) IS NOT NULL AND ((((((((((true)AND(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))OR((('780820706')=(''))))) ::INT64)as count FROM t0) as res;"#;
        let tokens = tokenize_sql(case).unwrap();
        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
        divan::black_box(stmt);
    }

    #[divan::bench]
    fn large_query() {
        let case = r#"SELECT SUM(count) FROM (SELECT ((((((((((((true)and(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))or((('780820706')=('')))) IS NOT NULL AND ((((((((((true)AND(true)))or((('614')like('998831')))))or(false)))and((true IN (true, true, (-1014651046 NOT BETWEEN -1098711288 AND -1158262473))))))OR((('780820706')=(''))))) ::INT64)as count FROM t0) as res;"#;
        let tokens = tokenize_sql(case).unwrap();
        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
        divan::black_box(stmt);
    }

    #[divan::bench]
    fn deep_query() {
        let case = r#"SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers UNION ALL SELECT * FROM numbers"#;
        let tokens = tokenize_sql(case).unwrap();
        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
        divan::black_box(stmt);
    }

    #[divan::bench]
    fn wide_expr() {
        let case = r#"a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a AND a"#;
        let tokens = tokenize_sql(case).unwrap();
        let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
        divan::black_box(expr);
    }
}
