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

use databend_common_column::types::timestamp_tz;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::type_check;
use databend_common_expression::types::timestamp_tz::TimestampTzType;
use databend_common_expression::types::*;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::test_utils;
use goldenfile::Mint;
use jsonb::OwnedJsonb;

use crate::scalars::run_ast;

#[test]
fn test_type_check() {
    let mut mint = Mint::new("tests/it/type_check/testdata");
    let json = "{}".parse::<OwnedJsonb>().unwrap().to_vec();

    let columns = [
        ("s", StringType::from_data(vec!["s"])),
        ("n_s", StringType::from_data(vec!["s"]).wrap_nullable(None)),
        ("i8", Int8Type::from_data(vec![0])),
        ("n_i8", Int8Type::from_data(vec![0]).wrap_nullable(None)),
        ("u8", UInt8Type::from_data(vec![0])),
        ("n_u8", UInt8Type::from_data(vec![0]).wrap_nullable(None)),
        ("i16", Int16Type::from_data(vec![0])),
        ("n_i16", Int16Type::from_data(vec![0]).wrap_nullable(None)),
        ("u16", UInt16Type::from_data(vec![0])),
        ("n_u16", UInt16Type::from_data(vec![0]).wrap_nullable(None)),
        ("i32", Int32Type::from_data(vec![0])),
        ("n_i32", Int32Type::from_data(vec![0]).wrap_nullable(None)),
        ("u32", UInt32Type::from_data(vec![0])),
        ("n_u32", UInt32Type::from_data(vec![0]).wrap_nullable(None)),
        ("i64", Int64Type::from_data(vec![0])),
        ("n_i64", Int64Type::from_data(vec![0]).wrap_nullable(None)),
        ("u64", UInt64Type::from_data(vec![0])),
        ("n_u64", UInt64Type::from_data(vec![0]).wrap_nullable(None)),
        ("f32", Float32Type::from_data(vec![0.0])),
        (
            "n_f32",
            Float32Type::from_data(vec![0.0]).wrap_nullable(None),
        ),
        ("f64", Float64Type::from_data(vec![0.0])),
        (
            "n_f64",
            Float64Type::from_data(vec![0.0]).wrap_nullable(None),
        ),
        ("b", BooleanType::from_data(vec![true])),
        (
            "n_b",
            BooleanType::from_data(vec![true]).wrap_nullable(None),
        ),
        ("d", DateType::from_data(vec![0])),
        ("n_d", DateType::from_data(vec![0]).wrap_nullable(None)),
        ("ts", TimestampType::from_data(vec![0])),
        (
            "n_ts",
            TimestampType::from_data(vec![0]).wrap_nullable(None),
        ),
        (
            "ts_tz",
            TimestampTzType::from_data(vec![timestamp_tz::default()]),
        ),
        (
            "n_ts_tz",
            TimestampTzType::from_data(vec![timestamp_tz::default()]).wrap_nullable(None),
        ),
        ("j", VariantType::from_data(vec![json.clone()])),
        (
            "n_j",
            VariantType::from_data(vec![json.clone()]).wrap_nullable(None),
        ),
        (
            "d128",
            Decimal128Type::from_data_with_size(vec![0_i128], None),
        ),
        (
            "n_d128",
            Decimal128Type::from_opt_data_with_size(vec![Some(0_i128)], None),
        ),
        (
            "d256",
            Decimal256Type::from_data_with_size(vec![i256::ZERO], None),
        ),
        (
            "n_d256",
            Decimal256Type::from_opt_data_with_size(vec![Some(i256::ZERO)], None),
        ),
    ];

    // 8 and 16 are just smaller 32.
    let size = ["32", "64"];

    let signed = size.iter().map(|s| format!("i{}", s)).collect::<Vec<_>>();
    let unsigned = size.iter().map(|s| format!("u{}", s)).collect::<Vec<_>>();
    let nullable_signed = size.iter().map(|s| format!("n_i{}", s)).collect::<Vec<_>>();
    let nullable_unsigned = size.iter().map(|s| format!("n_u{}", s)).collect::<Vec<_>>();
    let float = size
        .iter()
        .flat_map(|s| [format!("f{s}"), format!("n_f{s}")].into_iter())
        .collect::<Vec<_>>();
    let decimal = ["d128", "n_d128", "d256", "n_d256"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();

    let all_num = signed
        .iter()
        .chain(unsigned.iter())
        .chain(nullable_signed.iter())
        .chain(nullable_unsigned.iter())
        .chain(float.iter())
        .chain(decimal.iter())
        .collect::<Vec<_>>();

    for (name, types) in [
        ("signed", &signed),
        ("unsigned", &unsigned),
        ("nullable_signed", &nullable_signed),
        ("nullable_unsigned", &nullable_unsigned),
        ("float", &float),
        ("decimal", &decimal),
    ] {
        let file = &mut mint.new_goldenfile(format!("{name}.txt")).unwrap();
        let pair = types
            .iter()
            .flat_map(|lhs| all_num.iter().map(move |rhs| (lhs, *rhs)))
            .collect::<Vec<_>>();
        for (lhs, rhs) in pair {
            run_ast(file, format!("{lhs} > {rhs}"), &columns);
            run_ast(file, format!("{lhs} = {rhs}"), &columns);
        }

        for ty in types {
            run_ast(file, format!("{ty} > 1"), &columns);
            run_ast(file, format!("{ty} = 1"), &columns);
            run_ast(file, format!("1 > {ty}"), &columns);
            run_ast(file, format!("1 = {ty}"), &columns);

            run_ast(file, format!("{ty} > 1.0"), &columns);
            run_ast(file, format!("{ty} = 1.0"), &columns);
            run_ast(file, format!("1.0 > {ty}"), &columns);
            run_ast(file, format!("1.0 = {ty}"), &columns);

            run_ast(file, format!("{ty} > '1'"), &columns);
            run_ast(file, format!("{ty} = '1'"), &columns);
            run_ast(file, format!("'1' > {ty}"), &columns);
            run_ast(file, format!("'1' = {ty}"), &columns);

            run_ast(file, format!("{ty} > 1::uint64"), &columns);
            run_ast(file, format!("{ty} = 1::uint64"), &columns);
            run_ast(file, format!("1::uint64 > {ty}"), &columns);
            run_ast(file, format!("1::uint64 = {ty}"), &columns);
            run_ast(file, format!("{ty} = true"), &columns);
        }
    }
}

#[test]
fn test_find_leveled_eq_filters() {
    let cases = vec![
        (
            "database = 'a' or database = 'b'",
            vec![],
            vec![
                Scalar::String("a".to_string()),
                Scalar::String("b".to_string()),
            ],
            vec![],
        ),
        ("database = 'a' or c like 'xxb'", vec![], vec![], vec![]),
        (
            "catalog = 'x' and database = 'a' and table = 'b' and c like '%xxxx%'",
            vec![Scalar::String("x".to_string())],
            vec![Scalar::String("a".to_string())],
            vec![Scalar::String("b".to_string())],
        ),
        (
            "catalog = 'x' and (database = 'a' or database = 'b') and table = 'b' and c like '%xxxx%'",
            vec![Scalar::String("x".to_string())],
            vec![
                Scalar::String("a".to_string()),
                Scalar::String("b".to_string()),
            ],
            vec![Scalar::String("b".to_string())],
        ),
        (
            "catalog = 'x' and (database = 'a' or database = 'b' or table = 'b') and c like '%xxxx%'",
            vec![Scalar::String("x".to_string())],
            vec![],
            vec![],
        ),
        (
            "catalog = 'x' and (database = 'a' or database = 'b' or table = 'b') and c like '%xxxx%'",
            vec![Scalar::String("x".to_string())],
            vec![],
            vec![],
        ),
        (
            "catalog = 'x' and (database = 'a' or database = 'b') and database = 'c' and c like '%xxxx%'",
            vec![Scalar::String("x".to_string())],
            vec![Scalar::String("c".to_string())],
            vec![],
        ),
        ("not (database = 'default')", vec![], vec![], vec![]),
        (
            "not (database = 'default' or database = 'abcd')",
            vec![],
            vec![],
            vec![],
        ),
    ];

    let cols = vec![
        ("catalog", DataType::String),
        ("database", DataType::String),
        ("table", DataType::String),
        ("c", DataType::String),
    ];

    for (text, expected_catalog, expected_database, expected_table) in cases {
        let raw_expr = test_utils::parse_raw_expr(text, &cols);

        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        let expr = type_check::rewrite_function_to_cast(expr);
        let expr = expr
            .project_column_ref(|i| Ok(cols[*i].0.to_string()))
            .unwrap();

        let func_ctx = FunctionContext::default();
        let scalars = FilterHelpers::find_leveled_eq_filters(
            &expr,
            &["catalog", "database", "table"],
            &func_ctx,
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();

        assert_eq!(scalars, vec![
            expected_catalog,
            expected_database,
            expected_table
        ]);
    }
}
