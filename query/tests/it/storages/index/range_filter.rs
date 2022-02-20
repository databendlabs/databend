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

use std::collections::HashMap;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::*;
use databend_query::storages::index::range_filter::build_verifiable_expr;
use databend_query::storages::index::range_filter::left_bound_for_like_pattern;
use databend_query::storages::index::range_filter::right_bound_for_like_pattern;
use databend_query::storages::index::range_filter::BlockStatistics;
use databend_query::storages::index::range_filter::StatColumns;
use databend_query::storages::index::ColumnStatistics;
use databend_query::storages::index::RangeFilter;

#[test]
fn test_range_filter() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", i32::to_data_type()),
        DataField::new("c", Vu8::to_data_type()),
    ]);

    let mut stats: BlockStatistics = HashMap::new();
    stats.insert(0u32, ColumnStatistics {
        min: DataValue::Int64(1),
        max: DataValue::Int64(20),
        null_count: 1,
        in_memory_size: 0,
    });
    stats.insert(1u32, ColumnStatistics {
        min: DataValue::Int64(3),
        max: DataValue::Int64(10),
        null_count: 0,
        in_memory_size: 0,
    });
    stats.insert(2u32, ColumnStatistics {
        min: DataValue::String("abc".as_bytes().to_vec()),
        max: DataValue::String("bcd".as_bytes().to_vec()),
        null_count: 0,
        in_memory_size: 0,
    });

    struct Test {
        name: &'static str,
        expr: Expression,
        expect: bool,
        error: &'static str,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "a < 1 and b > 3",
            expr: col("a").lt(lit(1)).and(col("b").gt(lit(3i32))),
            expect: false,
            error: "",
        },
        Test {
            name: "1 > -a or 3 >= b",
            expr: lit(1).gt(neg(col("a"))).or(lit(3i32).gt_eq(col("b"))),
            expect: true,
            error: "",
        },
        Test {
            name: "a = 1 and b != 3",
            expr: col("a").eq(lit(1)).and(col("b").not_eq(lit(3))),
            expect: true,
            error: "",
        },
        Test {
            name: "a is null",
            expr: Expression::create_scalar_function("isNull", vec![col("a")]),
            expect: true,
            error: "",
        },
        Test {
            name: "a is not null",
            expr: Expression::create_scalar_function("isNotNull", vec![col("a")]),
            expect: true,
            error: "",
        },
        Test {
            name: "null",
            expr: Expression::create_literal(DataValue::Null),
            expect: false,
            error: "",
        },
        Test {
            name: "b >= 0 and c like '%sys%'",
            expr: col("b")
                .gt_eq(lit(0))
                .and(Expression::create_binary_expression("like", vec![
                    col("c"),
                    lit("%sys%".as_bytes()),
                ])),
            expect: true,
            error: "",
        },
        Test {
            name: "c like 'ab_'",
            expr: Expression::create_binary_expression("like", vec![
                col("c"),
                lit("ab_".as_bytes()),
            ]),
            expect: true,
            error: "",
        },
        Test {
            name: "c like 'bcdf'",
            expr: Expression::create_binary_expression("like", vec![
                col("c"),
                lit("bcdf".as_bytes()),
            ]),
            expect: false,
            error: "",
        },
        Test {
            name: "c not like 'ac%'",
            expr: Expression::create_binary_expression("not like", vec![
                col("c"),
                lit("ac%".as_bytes()),
            ]),
            expect: true,
            error: "",
        },
        Test {
            name: "a + b > 30",
            expr: add(col("a"), col("b")).gt(lit(30i32)),
            expect: false,
            error: "",
        },
        Test {
            name: "a + b < 10",
            expr: add(col("a"), col("b")).lt(lit(10i32)),
            expect: true,
            error: "",
        },
        Test {
            name: "a - b <= -10",
            expr: sub(col("a"), col("b")).lt_eq(lit(-10i32)),
            expect: true,
            error:
                "Code: 1067, displayText = Function '-' is not monotonic in the variables range.",
        },
        Test {
            name: "a < b",
            expr: col("a").lt(col("b")),
            expect: true,
            error: "",
        },
        Test {
            name: "a + 9 < b",
            expr: add(col("a"), lit(9)).lt(col("b")),
            expect: false,
            error: "",
        },
    ];

    for test in tests {
        let prune = RangeFilter::try_create(&test.expr, schema.clone())?;

        match prune.eval(&stats) {
            Ok(actual) => assert_eq!(test.expect, actual, "{:#?}", test.name),
            Err(e) => assert_eq!(test.error, e.to_string(), "{}", test.name),
        }
    }

    Ok(())
}

#[test]
fn test_build_verifiable_function() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", i32::to_data_type()),
        DataField::new("c", Vu8::to_data_type()),
    ]);

    struct Test {
        name: &'static str,
        expr: Expression,
        expect: &'static str,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "a < 1 and b > 3",
            expr: col("a").lt(lit(1)).and(col("b").gt(lit(3))),
            expect: "((min_a < 1) and (max_b > 3))",
        },
        Test {
            name: "1 > -a or 3 >= b",
            expr: lit(1).gt(neg(col("a"))).or(lit(3).gt_eq(col("b"))),
            expect: "((min_(negate a) < 1) or (min_b <= 3))",
        },
        Test {
            name: "a = 1 and b != 3",
            expr: col("a").eq(lit(1)).and(col("b").not_eq(lit(3))),
            expect: "(((min_a <= 1) and (max_a >= 1)) and ((min_b != 3) or (max_b != 3)))",
        },
        Test {
            name: "a is null",
            expr: Expression::create_scalar_function("isNull", vec![col("a")]),
            expect: "(nulls_a > 0)",
        },
        Test {
            name: "a is not null",
            expr: Expression::create_scalar_function("isNotNull", vec![col("a")]),
            expect: "isNotNull(min_a)",
        },
        Test {
            name: "b >= 0 and c like 0xffffff",
            expr: col("b")
                .gt_eq(lit(0))
                .and(Expression::create_binary_expression("like", vec![
                    col("c"),
                    lit(vec![255u8, 255, 255]),
                ])),
            expect: "((max_b >= 0) and (max_c >= ffffff))",
        },
        Test {
            name: "c like 'sys_'",
            expr: Expression::create_binary_expression("like", vec![
                col("c"),
                lit("sys_".as_bytes()),
            ]),
            expect: "((max_c >= sys) and (min_c < syt))",
        },
        Test {
            name: "c like 'sys\\%'",
            expr: Expression::create_binary_expression("like", vec![
                col("c"),
                lit("sys\\%".as_bytes()),
            ]),
            expect: "((max_c >= sys%) and (min_c < sys&))",
        },
        Test {
            name: "c like 'sys\\t'",
            expr: Expression::create_binary_expression("like", vec![
                col("c"),
                lit("sys\\t".as_bytes()),
            ]),
            expect: "((max_c >= sys\\t) and (min_c < sys\\u))",
        },
        Test {
            name: "c not like 'sys\\%'",
            expr: Expression::create_binary_expression("not like", vec![
                col("c"),
                lit("sys\\%".as_bytes()),
            ]),
            expect: "((min_c != sys%) or (max_c != sys%))",
        },
        Test {
            name: "c not like 'sys\\s'",
            expr: Expression::create_binary_expression("not like", vec![
                col("c"),
                lit("sys\\s".as_bytes()),
            ]),
            expect: "((min_c != sys\\s) or (max_c != sys\\s))",
        },
        Test {
            name: "c not like 'sys%'",
            expr: Expression::create_binary_expression("not like", vec![
                col("c"),
                lit("sys%".as_bytes()),
            ]),
            expect: "((min_c < sys) or (max_c >= syt))",
        },
        Test {
            name: "c not like 'sys%a'",
            expr: Expression::create_binary_expression("not like", vec![
                col("c"),
                lit("sys%a".as_bytes()),
            ]),
            expect: "true",
        },
        Test {
            name: "c not like 0xffffff%",
            expr: Expression::create_binary_expression("not like", vec![
                col("c"),
                lit(vec![255u8, 255, 255, 37]),
            ]),
            expect: "(min_c < ffffff)",
        },
        Test {
            name: "abs(a) = b - 3",
            expr: Expression::create_scalar_function("abs", vec![col("a")])
                .eq(add(col("b"), lit(3))),
            expect: "((min_abs(a) <= max_(b + 3)) and (max_abs(a) >= min_(b + 3)))",
        },
        Test {
            name: "a + b <= 3",
            expr: add(col("a"), col("b")).lt_eq(lit(3)),
            expect: "(min_(a + b) <= 3)",
        },
        Test {
            name: "a + b <= 10 - a",
            expr: add(col("a"), col("b")).lt_eq(sub(lit(10), col("a"))),
            expect: "true",
        },
        Test {
            name: "a <= b + rand()",
            expr: add(
                col("a"),
                add(col("b"), Expression::create_scalar_function("rand", vec![])),
            ),
            expect: "true",
        },
    ];

    for test in tests {
        let mut stat_columns: StatColumns = Vec::new();
        let res = build_verifiable_expr(&test.expr, &schema, &mut stat_columns);
        let actual = format!("{:?}", res);
        assert_eq!(test.expect, actual, "{:#?}", test.name);
    }

    Ok(())
}

#[test]
fn test_bound_for_like_pattern() -> Result<()> {
    struct Test {
        name: &'static str,
        pattern: Vec<u8>,
        left: Vec<u8>,
        right: Vec<u8>,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "ordinary string",
            pattern: vec![b'a', b'b', b'c'],
            left: vec![b'a', b'b', b'c'],
            right: vec![b'a', b'b', b'd'],
        },
        Test {
            name: "contain _",
            pattern: vec![b'a', b'_', b'c'],
            left: vec![b'a'],
            right: vec![b'b'],
        },
        Test {
            name: "contain %",
            pattern: vec![b'a', b'%', b'c'],
            left: vec![b'a'],
            right: vec![b'b'],
        },
        Test {
            name: "contain \\_",
            pattern: vec![b'a', b'\\', b'_', b'c'],
            left: vec![b'a', b'_', b'c'],
            right: vec![b'a', b'_', b'd'],
        },
        Test {
            name: "contain \\",
            pattern: vec![b'a', b'\\', b'b', b'c'],
            left: vec![b'a', b'\\', b'b', b'c'],
            right: vec![b'a', b'\\', b'b', b'd'],
        },
        Test {
            name: "left is empty",
            pattern: vec![b'%', b'b', b'c'],
            left: vec![],
            right: vec![],
        },
        Test {
            name: "right is empty",
            pattern: vec![255u8, 255, 255],
            left: vec![255u8, 255, 255],
            right: vec![],
        },
    ];

    for test in tests {
        let left = left_bound_for_like_pattern(&test.pattern);
        assert_eq!(test.left, left, "{:#?}", test.name);
        let right = right_bound_for_like_pattern(left);
        assert_eq!(test.right, right, "{:#?}", test.name);
    }

    Ok(())
}
