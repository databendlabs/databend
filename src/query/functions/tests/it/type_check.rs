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
use databend_common_expression::ColumnRef;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::type_check;
use databend_common_expression::types::timestamp_tz::TimestampTzType;
use databend_common_expression::types::*;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use goldenfile::Mint;
use jsonb::OwnedJsonb;

use crate::scalars::run_ast;

fn checked_function_to_cast(
    name: &str,
    params: &[Scalar],
    argument_type: DataType,
) -> Option<type_check::FunctionCallToCastResult> {
    let argument = Expr::ColumnRef(ColumnRef {
        span: None,
        id: 0,
        data_type: argument_type.clone(),
        display_name: "arg".to_string(),
    });
    let checked = type_check::check_function(
        None,
        name,
        params,
        std::slice::from_ref(&argument),
        &BUILTIN_FUNCTIONS,
    )
    .unwrap();
    checked.as_function_call().and_then(|call| {
        type_check::function_call_to_cast(
            name,
            params,
            &argument_type,
            &call.return_type,
            &BUILTIN_FUNCTIONS,
        )
    })
}

#[test]
fn test_function_call_to_cast_uses_resolved_cast_function() {
    let cast = checked_function_to_cast("to_int64", &[], DataType::String).unwrap();
    assert!(!cast.is_try);
    assert_eq!(cast.dest_type, DataType::Number(NumberDataType::Int64));

    let cast = checked_function_to_cast("try_to_int64", &[], DataType::String).unwrap();
    assert!(cast.is_try);
    assert_eq!(
        cast.dest_type,
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64)))
    );

    // Aliases are accepted because check_function resolves them to the same FunctionID.
    assert!(checked_function_to_cast("date", &[], DataType::String).is_some());

    let decimal_params = [
        Scalar::Number(NumberScalar::Int64(10)),
        Scalar::Number(NumberScalar::Int64(2)),
    ];
    let cast = checked_function_to_cast(
        "to_numeric",
        &decimal_params,
        DataType::Number(NumberDataType::Int64),
    )
    .unwrap();
    assert_eq!(
        cast.dest_type,
        DataType::Decimal(DecimalSize::new(10, 2).unwrap())
    );

    // Factory params must match those reconstructed by Expr::Cast.
    let mismatched_decimal_params = [
        Scalar::Number(NumberScalar::Int64(11)),
        Scalar::Number(NumberScalar::Int64(2)),
    ];
    assert!(
        type_check::function_call_to_cast(
            "to_decimal",
            &mismatched_decimal_params,
            &DataType::Number(NumberDataType::Int64),
            &DataType::Decimal(DecimalSize::new(10, 2).unwrap()),
            &BUILTIN_FUNCTIONS,
        )
        .is_none()
    );

    // CAST(String AS Variant) executes parse_json, not to_variant.
    assert!(checked_function_to_cast("parse_json", &[], DataType::String).is_some());
    assert!(checked_function_to_cast("to_variant", &[], DataType::String).is_none());
    assert!(
        checked_function_to_cast("to_variant", &[], DataType::Number(NumberDataType::Int64))
            .is_some()
    );

    // The same return type is insufficient: Cast dispatch must select the same canonical function.
    assert!(checked_function_to_cast("length", &[], DataType::String).is_none());
}

#[test]
fn test_nullable_function_call_to_cast_uses_complete_cast_resolution() {
    let nullable = |ty| DataType::Nullable(Box::new(ty));
    let decimal_params = [
        Scalar::Number(NumberScalar::Int64(10)),
        Scalar::Number(NumberScalar::Int64(2)),
    ];

    // Keep this table explicit: every function selected by Expr::Cast must prove that its checked
    // nullable overload is also accepted by function_call_to_cast.
    let cases = [
        (
            "to_boolean",
            Some("try_to_boolean"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_binary",
            Some("try_to_binary"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_string",
            Some("try_to_string"),
            vec![],
            nullable(DataType::Number(NumberDataType::Int64)),
        ),
        (
            "to_uint8",
            Some("try_to_uint8"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_uint16",
            Some("try_to_uint16"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_uint32",
            Some("try_to_uint32"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_uint64",
            Some("try_to_uint64"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_int8",
            Some("try_to_int8"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_int16",
            Some("try_to_int16"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_int32",
            Some("try_to_int32"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_int64",
            Some("try_to_int64"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_float32",
            Some("try_to_float32"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_float64",
            Some("try_to_float64"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_timestamp",
            Some("try_to_timestamp"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_timestamp_tz",
            Some("try_to_timestamp_tz"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_interval",
            Some("try_to_interval"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_date",
            Some("try_to_date"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_variant",
            Some("try_to_variant"),
            vec![],
            nullable(DataType::Number(NumberDataType::Int64)),
        ),
        (
            "to_decimal",
            Some("try_to_decimal"),
            decimal_params.to_vec(),
            nullable(DataType::Number(NumberDataType::Int64)),
        ),
        ("to_bitmap", None, vec![], nullable(DataType::String)),
        (
            "to_geometry",
            Some("try_to_geometry"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "to_geography",
            Some("try_to_geography"),
            vec![],
            nullable(DataType::String),
        ),
        (
            "parse_json",
            Some("try_parse_json"),
            vec![],
            nullable(DataType::String),
        ),
    ];

    for (name, try_name, params, argument_type) in cases {
        assert!(
            checked_function_to_cast(name, &params, argument_type.clone()).is_some(),
            "{name}({argument_type}) must be rewritten from the function selected by Expr::Cast"
        );

        if let Some(try_name) = try_name {
            assert!(
                checked_function_to_cast(try_name, &params, argument_type.clone()).is_some(),
                "{try_name}({argument_type}) must be rewritten from the function selected by TRY_CAST"
            );
        }
    }
}

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
        let raw_expr = parse_raw_expr(text, &cols, &BUILTIN_FUNCTIONS);

        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        let expr = type_check::rewrite_function_to_cast(expr, &BUILTIN_FUNCTIONS);
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
