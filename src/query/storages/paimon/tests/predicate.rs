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

use databend_common_expression::FunctionID;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberScalar;
use databend_common_storages_paimon::PaimonPredicateAnalysis;
use databend_common_storages_paimon::analyze_predicate;
use databend_common_storages_paimon::can_push_limit;
use paimon::spec::DataField;
use paimon::spec::DataType as PaimonDataType;
use paimon::spec::IntType;
use paimon::spec::LocalZonedTimestampType;
use paimon::spec::TimestampType as PaimonTimestampType;

fn fields() -> Vec<DataField> {
    vec![
        DataField::new(0, "id".to_string(), PaimonDataType::Int(IntType::new())),
        DataField::new(
            1,
            "name".to_string(),
            PaimonDataType::VarChar(paimon::spec::VarCharType::string_type()),
        ),
    ]
}

fn column(name: &str, data_type: DataType) -> RemoteExpr<String> {
    RemoteExpr::ColumnRef {
        span: None,
        id: name.to_string(),
        data_type,
        display_name: name.to_string(),
    }
}

fn int_column(name: &str) -> RemoteExpr<String> {
    column(
        name,
        DataType::Number(databend_common_expression::types::NumberDataType::Int32),
    )
}

fn string_column(name: &str) -> RemoteExpr<String> {
    column(name, DataType::String)
}

fn int_literal(value: i32) -> RemoteExpr<String> {
    RemoteExpr::Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::Int32(value)),
        data_type: DataType::Number(databend_common_expression::types::NumberDataType::Int32),
    }
}

fn string_literal(value: &str) -> RemoteExpr<String> {
    RemoteExpr::Constant {
        span: None,
        scalar: Scalar::String(value.to_string()),
        data_type: DataType::String,
    }
}

fn timestamp_literal(micros: i64) -> RemoteExpr<String> {
    RemoteExpr::Constant {
        span: None,
        scalar: Scalar::Timestamp(micros),
        data_type: DataType::Timestamp,
    }
}

fn function(name: &str, args: Vec<RemoteExpr<String>>) -> RemoteExpr<String> {
    RemoteExpr::FunctionCall {
        span: None,
        id: Box::new(FunctionID::Builtin {
            name: name.to_string(),
            id: 0,
        }),
        generics: vec![],
        args,
        return_type: DataType::Boolean,
    }
}

fn partitioned_fields() -> Vec<DataField> {
    vec![
        DataField::new(0, "part".to_string(), PaimonDataType::Int(IntType::new())),
        DataField::new(1, "id".to_string(), PaimonDataType::Int(IntType::new())),
    ]
}

fn dummy_read_builder(fields: &[DataField]) -> paimon::ReadBuilder<'static> {
    let mut schema_builder = paimon::spec::Schema::builder();
    for field in fields {
        schema_builder = schema_builder.column(field.name(), field.data_type().clone());
    }
    if fields.iter().any(|field| field.name() == "part") {
        schema_builder = schema_builder.partition_keys(["part"]);
    }
    let table = Box::leak(Box::new(paimon::Table::new(
        paimon::io::FileIOBuilder::new("fs").build().unwrap(),
        paimon::catalog::Identifier::new("db", "t"),
        std::env::temp_dir()
            .join("paimon-predicate-test")
            .to_string_lossy()
            .into_owned(),
        paimon::spec::TableSchema::new(0, &schema_builder.build().unwrap()),
        None,
    )));
    table.new_read_builder()
}

#[test]
fn test_supported_comparisons_and_null_checks() {
    let fields = fields();
    for (op, col, lit) in [
        ("eq", "id", 1),
        ("lt", "id", 2),
        ("lte", "id", 2),
        ("gt", "id", 0),
        ("gte", "id", 0),
        ("noteq", "id", 1),
    ] {
        let expr = function(op, vec![int_column(col), int_literal(lit)]);
        let analysis = analyze_predicate(&expr, &fields);
        assert!(analysis.predicate.is_some(), "op {op}");
        assert!(!analysis.has_residual, "op {op}");
    }

    for op in ["is_null", "is_not_null"] {
        let expr = function(op, vec![string_column("name")]);
        let analysis = analyze_predicate(&expr, &fields);
        assert!(analysis.predicate.is_some(), "op {op}");
        assert!(!analysis.has_residual, "op {op}");
    }
}

#[test]
fn test_reverse_comparison() {
    let fields = fields();
    let expr = function("eq", vec![int_literal(1), int_column("id")]);
    let analysis = analyze_predicate(&expr, &fields);
    assert!(analysis.predicate.is_some());
    assert!(!analysis.has_residual);
}

#[test]
fn test_and_or_in_between_not_in() {
    let fields = fields();
    let and_expr = function("and", vec![
        function("eq", vec![int_column("id"), int_literal(1)]),
        function("gt", vec![int_column("id"), int_literal(0)]),
    ]);
    let and_analysis = analyze_predicate(&and_expr, &fields);
    assert!(and_analysis.predicate.is_some());
    assert!(!and_analysis.has_residual);

    let or_expr = function("or", vec![
        function("eq", vec![int_column("id"), int_literal(1)]),
        function("eq", vec![int_column("id"), int_literal(2)]),
    ]);
    let or_analysis = analyze_predicate(&or_expr, &fields);
    assert!(or_analysis.predicate.is_some());
    assert!(!or_analysis.has_residual);

    let in_expr = function("inlist", vec![
        int_column("id"),
        int_literal(1),
        int_literal(2),
        int_literal(3),
    ]);
    let in_analysis = analyze_predicate(&in_expr, &fields);
    assert!(in_analysis.predicate.is_some());
    assert!(!in_analysis.has_residual);

    let not_in_expr = function("not_inlist", vec![
        int_column("id"),
        int_literal(1),
        int_literal(2),
    ]);
    let not_in_analysis = analyze_predicate(&not_in_expr, &fields);
    assert!(not_in_analysis.predicate.is_some());
    assert!(!not_in_analysis.has_residual);

    let between_expr = function("between", vec![
        int_column("id"),
        int_literal(1),
        int_literal(5),
    ]);
    let between_analysis = analyze_predicate(&between_expr, &fields);
    assert!(between_analysis.predicate.is_some());
    assert!(!between_analysis.has_residual);
}

#[test]
fn test_reject_not_not_between_function_and_partial_or() {
    let fields = fields();
    let not_expr = function("not", vec![function("eq", vec![
        int_column("id"),
        int_literal(1),
    ])]);
    let not_analysis = analyze_predicate(&not_expr, &fields);
    assert!(not_analysis.predicate.is_none());
    assert!(not_analysis.has_residual);

    let not_between = function("not", vec![function("between", vec![
        int_column("id"),
        int_literal(1),
        int_literal(5),
    ])]);
    let not_between_analysis = analyze_predicate(&not_between, &fields);
    assert!(not_between_analysis.predicate.is_none());
    assert!(not_between_analysis.has_residual);

    let function_expr = function("eq", vec![
        function("lower", vec![string_column("name")]),
        string_literal("x"),
    ]);
    let function_analysis = analyze_predicate(&function_expr, &fields);
    assert!(function_analysis.predicate.is_none());
    assert!(function_analysis.has_residual);

    let partial_or = function("or", vec![
        function("eq", vec![int_column("id"), int_literal(1)]),
        function("not", vec![function("eq", vec![
            int_column("id"),
            int_literal(2),
        ])]),
    ]);
    let partial_or_analysis = analyze_predicate(&partial_or, &fields);
    assert!(partial_or_analysis.predicate.is_none());
    assert!(partial_or_analysis.has_residual);

    let partial_and = function("and", vec![
        function("eq", vec![int_column("id"), int_literal(1)]),
        function("not", vec![function("eq", vec![
            string_column("name"),
            string_literal("x"),
        ])]),
    ]);
    let partial_analysis = analyze_predicate(&partial_and, &fields);
    assert!(partial_analysis.predicate.is_some());
    assert!(partial_analysis.has_residual);
}

#[test]
fn test_decimal_scale_mismatch_rejected() {
    let fields = vec![DataField::new(
        0,
        "amount".to_string(),
        PaimonDataType::Decimal(paimon::spec::DecimalType::new(10, 2).expect("decimal")),
    )];
    let expr = function("eq", vec![
        column(
            "amount",
            DataType::Decimal(DecimalSize::new_unchecked(10, 2)),
        ),
        RemoteExpr::Constant {
            span: None,
            scalar: Scalar::Decimal(databend_common_expression::types::DecimalScalar::Decimal64(
                100,
                DecimalSize::new_unchecked(10, 3),
            )),
            data_type: DataType::Decimal(DecimalSize::new_unchecked(10, 3)),
        },
    ]);
    let analysis = analyze_predicate(&expr, &fields);
    assert!(analysis.predicate.is_none());
    assert!(analysis.has_residual);
}

#[test]
fn test_timestamp_predicate_translation() {
    let fields = vec![
        DataField::new(
            0,
            "ts".to_string(),
            PaimonDataType::Timestamp(PaimonTimestampType::new(6).unwrap()),
        ),
        DataField::new(
            1,
            "lts".to_string(),
            PaimonDataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
        ),
    ];
    for (col, micros) in [("ts", 1_500i64), ("lts", -500i64)] {
        let expr = function("eq", vec![
            column(col, DataType::Timestamp),
            timestamp_literal(micros),
        ]);
        let analysis = analyze_predicate(&expr, &fields);
        assert!(analysis.predicate.is_some(), "column {col}");
        assert!(!analysis.has_residual, "column {col}");
    }
}

#[test]
fn test_can_push_limit_matrix() {
    let fields = fields();
    let non_partition_builder = dummy_read_builder(&fields);

    assert!(can_push_limit(
        Some(10),
        &PaimonPredicateAnalysis {
            predicate: None,
            has_residual: false,
        },
        &non_partition_builder
    ));

    let data_predicate_on_non_partition = analyze_predicate(
        &function("eq", vec![int_column("id"), int_literal(1)]),
        &fields,
    );

    let partitioned = partitioned_fields();
    let partition_builder = dummy_read_builder(&partitioned);
    let exact_partition = analyze_predicate(
        &function("eq", vec![int_column("part"), int_literal(1)]),
        &partitioned,
    );
    assert!(!exact_partition.has_residual);
    assert!(can_push_limit(
        Some(10),
        &exact_partition,
        &partition_builder
    ));

    let data_predicate = analyze_predicate(
        &function("eq", vec![int_column("id"), int_literal(1)]),
        &partitioned,
    );
    assert!(!can_push_limit(
        Some(10),
        &data_predicate,
        &partition_builder
    ));

    let residual = analyze_predicate(
        &function("and", vec![
            function("eq", vec![int_column("part"), int_literal(1)]),
            function("not", vec![function("eq", vec![
                int_column("id"),
                int_literal(1),
            ])]),
        ]),
        &partitioned,
    );
    assert!(residual.has_residual);
    assert!(!can_push_limit(Some(10), &residual, &partition_builder));

    assert!(!can_push_limit(
        Some(10),
        &data_predicate_on_non_partition,
        &non_partition_builder
    ));
}
