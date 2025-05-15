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

// bench                     fastest       │ slowest       │ median        │ mean          │ samples │ iters
// ╰─ dummy                                │               │               │               │         │
//    ├─ deep_function_call  732.9 ms      │ 732.9 ms      │ 732.9 ms      │ 732.9 ms      │ 1       │ 1
//    ├─ deep_query          319.4 µs      │ 515.6 µs      │ 333.4 µs      │ 335.3 µs      │ 100     │ 100
//    ├─ large_query         1.998 ms      │ 2.177 ms      │ 2.032 ms      │ 2.038 ms      │ 100     │ 100
//    ├─ large_statement     1.952 ms      │ 2.079 ms      │ 2.016 ms      │ 2.011 ms      │ 100     │ 100
//    ╰─ wide_expr           620.4 µs      │ 783.7 µs      │ 646 µs        │ 646.4 µs      │ 100     │ 100

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

    #[divan::bench]
    fn deep_function_call() {
        let case = r#"ROUND(6378.138 * 2 * ASIN(SQRT(POW(SIN(RADIANS(CASE WHEN MOD(EXTRACT(SECOND FROM CURRENT_TIMESTAMP), 2) = 0 THEN 45.6789 ELSE 30.1234 END - IFNULL(NULLIF((SELECT 37.7749), 0), 15.4321))), 2) + POW(SIN(RADIANS((SELECT -122.4194) / 2)), 2) * COS(RADIANS(LEAST(60, GREATEST(20, (SELECT 25.5))))))) * (1000 + (RAND() * 500 - 250)), 2)"#;
        let tokens = tokenize_sql(case).unwrap();
        let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
        divan::black_box(expr);
    }
}
