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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_expression::ColumnRef as ExprColumnRef;
use databend_common_expression::Constant as ExprConstant;
use databend_common_expression::Expr;
use databend_common_expression::Function;
use databend_common_expression::FunctionCall as ExprFunctionCall;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionID;
use databend_common_expression::Scalar;
use databend_common_expression::StatEvaluator;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::number::F32;
use databend_common_expression::types::number::F64 as ExprF64;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnBinding;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::ColumnStat;
use databend_common_sql::optimizer::ir::ColumnStatSet;
use databend_common_sql::optimizer::ir::HistogramBuilder;
use databend_common_sql::optimizer::ir::SelectivityEstimator;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::FunctionCall as ScalarFunctionCall;
use databend_common_statistics::DEFAULT_HISTOGRAM_BUCKETS;
use databend_common_statistics::Datum;
use databend_common_statistics::F64;
use databend_common_statistics::Histogram;
use databend_common_statistics::TypedHistogram;
use databend_common_statistics::TypedHistogramBucket;
use proptest::prelude::*;

fn column_binding(name: &str, index: usize, data_type: DataType) -> ColumnBinding {
    ColumnBindingBuilder::new(
        name.to_string(),
        Symbol::new(index),
        Box::new(data_type),
        Visibility::Visible,
    )
    .build()
}

fn column_expr(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
    let column = column_binding(name, index, data_type);
    ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column })
}

fn constant_expr(value: Scalar) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr { span: None, value })
}

fn comparison_expr(func_name: &str, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
    function_expr(func_name, vec![left, right])
}

fn function_expr(func_name: &str, arguments: Vec<ScalarExpr>) -> ScalarExpr {
    ScalarExpr::FunctionCall(ScalarFunctionCall {
        span: None,
        func_name: func_name.to_string(),
        params: vec![],
        arguments,
    })
}

#[test]
fn zero_cardinality_comparison_selectivity_is_finite() {
    let column_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(10),
        ndv: NdvEstimate::exact(0.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    let expr = comparison_expr(
        "gt",
        column_expr("a", 0, DataType::Number(NumberDataType::UInt64)),
        constant_expr(Scalar::Number(NumberScalar::UInt64(5))),
    );

    let mut estimator = SelectivityEstimator::new(column_stats, StatCardinality::estimate(0.0));
    let estimated_rows = estimator
        .apply(&[expr])
        .expect("zero-cardinality comparison should estimate");

    assert_eq!(estimated_rows, 0.0);
    assert!(estimated_rows.is_finite());
}

#[test]
fn distorted_histogram_comparison_estimate_narrows_range() {
    let column_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(1000),
        ndv: NdvEstimate::exact(100.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::Float(TypedHistogram {
            accuracy: false,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(
                F64::from(0.0),
                F64::from(1000.0),
                100.0,
                100.0,
            )],
            avg_spacing: Some(1e13),
        })),
    })]);
    let expr = comparison_expr(
        "gt",
        column_expr("a", 0, DataType::Number(NumberDataType::UInt64)),
        constant_expr(Scalar::Number(NumberScalar::UInt64(100))),
    );

    let mut estimator = SelectivityEstimator::new(column_stats, StatCardinality::estimate(100.0));
    let estimated_rows = estimator
        .apply(&[expr])
        .expect("distorted histogram comparison should estimate");
    let stat = &estimator.column_stats()[&Symbol::new(0)];

    assert!((estimated_rows - 50.0).abs() < f64::EPSILON);
    assert_eq!(stat.min, Datum::UInt(101));
    assert_eq!(stat.max, Datum::UInt(1000));
}

#[test]
fn float_full_domain_arithmetic_selectivity_is_not_empty() {
    let column = column_expr("a", 0, DataType::Number(NumberDataType::Float64));
    let expr = comparison_expr(
        "lt",
        function_expr("minus", vec![
            function_expr("plus", vec![
                column.clone(),
                constant_expr(Scalar::Number(NumberScalar::Float64(ExprF64::from(1.0)))),
            ]),
            column,
        ]),
        constant_expr(Scalar::Number(NumberScalar::Float64(ExprF64::from(10.0)))),
    );

    let mut estimator =
        SelectivityEstimator::new(ColumnStatSet::new(), StatCardinality::estimate(10.0));
    let estimated_rows = estimator
        .apply(&[expr])
        .expect("float arithmetic comparison should estimate");

    assert!(
        estimated_rows > 0.0,
        "full Float64 domain arithmetic is not proof of an empty result"
    );
}

#[test]
fn static_derive_stat_functions_have_selectivity_smoke() {
    let (function_names, cases) = reflected_static_derive_stat_smoke_cases();
    assert!(!cases.is_empty(), "derive_stat smoke should find cases");

    let mut derived_by_function = function_names
        .into_iter()
        .map(|function_name| (function_name, false))
        .collect::<HashMap<_, _>>();
    for case in cases {
        let derived = case.run();
        derived_by_function
            .entry(case.function_name.clone())
            .and_modify(|has_derived| *has_derived |= derived)
            .or_insert(derived);
    }
    let mut functions_without_derived_stat = derived_by_function
        .into_iter()
        .filter_map(|(function_name, derived)| (!derived).then_some(function_name))
        .collect::<Vec<_>>();
    functions_without_derived_stat.sort();
    assert!(
        functions_without_derived_stat.is_empty(),
        "derive_stat smoke found no derived stat for function names: {}",
        functions_without_derived_stat.join(", ")
    );
}

struct DeriveStatSmokeCase {
    function_name: String,
    display_name: String,
    expr: Expr<ColumnBinding>,
}

impl DeriveStatSmokeCase {
    fn run(&self) -> bool {
        let column_stats = self
            .expr
            .column_refs()
            .into_iter()
            .map(|(binding, data_type)| {
                let column_stat = smoke_column_stat(&data_type).unwrap_or_else(|| {
                    panic!(
                        "derive_stat smoke cannot build column stats for {} argument type {:?}",
                        self.display_name, data_type
                    )
                });
                (binding, data_type, column_stat)
            })
            .collect::<Vec<_>>();
        let input_stats = column_stats
            .iter()
            .map(|(binding, data_type, column_stat)| {
                let stat = column_stat.to_arg_stat(data_type).unwrap_or_else(|err| {
                    panic!(
                        "derive_stat smoke cannot convert {} argument {:?} to ArgStat: {err}",
                        self.display_name, data_type
                    )
                });
                (binding.clone(), stat)
            })
            .collect::<HashMap<_, _>>();

        let stat = StatEvaluator::run(
            &self.expr,
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
            StatCardinality::estimate(100.0),
            &input_stats,
        )
        .unwrap_or_else(|err| {
            panic!(
                "derive_stat smoke failed for {}: {err}: {:?}",
                self.display_name, self.expr
            )
        });
        let Some(stat) = stat else {
            return false;
        };
        let stat = stat.into_owned();

        stat.check_consistency_with_type(Some(self.expr.data_type()))
            .unwrap_or_else(|err| {
                panic!(
                    "derive_stat smoke produced invalid stat for {}: {err}",
                    self.display_name
                )
            });
        true
    }
}

fn reflected_static_derive_stat_smoke_cases() -> (BTreeSet<String>, Vec<DeriveStatSmokeCase>) {
    let mut function_names = BTreeSet::new();
    let mut cases = BUILTIN_FUNCTIONS
        .funcs
        .values()
        .flat_map(|funcs| funcs.iter())
        .filter_map(|(func, id)| match &func.eval {
            FunctionEval::Scalar {
                derive_stat: Some(_),
                ..
            } => {
                function_names.insert(func.signature.name.clone());
                build_derive_stat_smoke_case(func, *id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    cases.sort_by(|left, right| left.display_name.cmp(&right.display_name));
    (function_names, cases)
}

fn build_derive_stat_smoke_case(func: &Arc<Function>, id: usize) -> Option<DeriveStatSmokeCase> {
    let signature = &func.signature;
    let mut args = Vec::with_capacity(signature.args_type.len());
    for (index, data_type) in signature.args_type.iter().enumerate() {
        if index == 0 && smoke_column_stat(data_type).is_some() {
            let name = format!("c{index}");
            let binding = column_binding(&name, index, data_type.clone());
            args.push(Expr::ColumnRef(ExprColumnRef {
                span: None,
                id: binding,
                data_type: data_type.clone(),
                display_name: name,
            }));
        } else {
            args.push(Expr::Constant(ExprConstant {
                span: None,
                scalar: smoke_scalar(data_type)?,
                data_type: data_type.clone(),
            }));
        }
    }
    let display_name = format!(
        "{}({}) -> {:?}",
        signature.name,
        signature
            .args_type
            .iter()
            .map(|data_type| format!("{data_type:?}"))
            .collect::<Vec<_>>()
            .join(", "),
        signature.return_type
    );
    let expr = Expr::FunctionCall(ExprFunctionCall {
        span: None,
        id: Box::new(FunctionID::Builtin {
            name: signature.name.clone(),
            id,
        }),
        function: func.clone(),
        generics: vec![],
        args,
        return_type: signature.return_type.clone(),
    });
    Some(DeriveStatSmokeCase {
        function_name: signature.name.clone(),
        display_name,
        expr,
    })
}

fn smoke_scalar(data_type: &DataType) -> Option<Scalar> {
    match data_type {
        DataType::Nullable(inner) => smoke_scalar(inner),
        DataType::Boolean => Some(Scalar::Boolean(true)),
        DataType::String => Some(Scalar::String("s00000000000000000020".to_string())),
        DataType::Binary => Some(Scalar::Binary(b"s00000000000000000020".to_vec())),
        DataType::Date => Some(Scalar::Date(20)),
        DataType::Timestamp => Some(Scalar::Timestamp(20)),
        DataType::TimestampTz | DataType::Interval => Some(Scalar::default_value(data_type)),
        DataType::Number(NumberDataType::UInt8) => Some(Scalar::Number(NumberScalar::UInt8(20))),
        DataType::Number(NumberDataType::UInt16) => Some(Scalar::Number(NumberScalar::UInt16(20))),
        DataType::Number(NumberDataType::UInt32) => Some(Scalar::Number(NumberScalar::UInt32(20))),
        DataType::Number(NumberDataType::UInt64) => Some(Scalar::Number(NumberScalar::UInt64(20))),
        DataType::Number(NumberDataType::Int8) => Some(Scalar::Number(NumberScalar::Int8(20))),
        DataType::Number(NumberDataType::Int16) => Some(Scalar::Number(NumberScalar::Int16(20))),
        DataType::Number(NumberDataType::Int32) => Some(Scalar::Number(NumberScalar::Int32(20))),
        DataType::Number(NumberDataType::Int64) => Some(Scalar::Number(NumberScalar::Int64(20))),
        DataType::Number(NumberDataType::Float32) => {
            Some(Scalar::Number(NumberScalar::Float32(F32::from(20.0))))
        }
        DataType::Number(NumberDataType::Float64) => {
            Some(Scalar::Number(NumberScalar::Float64(F64::from(20.0))))
        }
        _ => None,
    }
}

fn smoke_column_stat(data_type: &DataType) -> Option<ColumnStat> {
    let inner = data_type.remove_nullable();
    let (min, max) = match &inner {
        DataType::Boolean => (Datum::Bool(false), Datum::Bool(true)),
        DataType::String => (
            Datum::Bytes(b"s00000000000000000000".to_vec()),
            Datum::Bytes(b"s00000000000000000009".to_vec()),
        ),
        DataType::Binary => (
            Datum::Bytes(b"s00000000000000000000".to_vec()),
            Datum::Bytes(b"s00000000000000000009".to_vec()),
        ),
        DataType::Date | DataType::Timestamp => (Datum::Int(0), Datum::Int(9)),
        DataType::Number(NumberDataType::UInt8)
        | DataType::Number(NumberDataType::UInt16)
        | DataType::Number(NumberDataType::UInt32)
        | DataType::Number(NumberDataType::UInt64) => (Datum::UInt(0), Datum::UInt(9)),
        DataType::Number(NumberDataType::Int8)
        | DataType::Number(NumberDataType::Int16)
        | DataType::Number(NumberDataType::Int32)
        | DataType::Number(NumberDataType::Int64) => (Datum::Int(0), Datum::Int(9)),
        DataType::Number(NumberDataType::Float32) | DataType::Number(NumberDataType::Float64) => {
            (Datum::Float(F64::from(0.0)), Datum::Float(F64::from(9.0)))
        }
        _ => return None,
    };
    Some(ColumnStat {
        min,
        max,
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })
}

#[derive(Clone, Copy, Debug)]
enum SmokeValueKind {
    UInt64,
    Int64,
    Date,
    Timestamp,
    String,
}

impl SmokeValueKind {
    fn from_data_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Number(NumberDataType::UInt64) => Some(SmokeValueKind::UInt64),
            DataType::Number(NumberDataType::Int64) => Some(SmokeValueKind::Int64),
            DataType::Date => Some(SmokeValueKind::Date),
            DataType::Timestamp => Some(SmokeValueKind::Timestamp),
            DataType::String => Some(SmokeValueKind::String),
            _ => None,
        }
    }

    fn datum(self, value: i64) -> Datum {
        match self {
            SmokeValueKind::UInt64 => Datum::UInt(value.max(0) as u64),
            SmokeValueKind::Int64 => Datum::Int(value),
            SmokeValueKind::Date => Datum::Int(value),
            SmokeValueKind::Timestamp => Datum::Int(value),
            SmokeValueKind::String => Datum::Bytes(format!("s{value:020}").into_bytes()),
        }
    }

    fn scalar(self, value: i64) -> Scalar {
        match self {
            SmokeValueKind::UInt64 => Scalar::Number(NumberScalar::UInt64(value.max(0) as u64)),
            SmokeValueKind::Int64 => Scalar::Number(NumberScalar::Int64(value)),
            SmokeValueKind::Date => Scalar::Date(value as i32),
            SmokeValueKind::Timestamp => Scalar::Timestamp(value),
            SmokeValueKind::String => Scalar::String(format!("s{value:020}")),
        }
    }
}

#[derive(Clone, Debug)]
struct SmokeComparisonCase {
    func_name: String,
    data_type: DataType,
    value_kind: SmokeValueKind,
    nullable: bool,
}

fn reflected_comparison_smoke_cases() -> Vec<SmokeComparisonCase> {
    let comparison_names = ["eq", "noteq", "gt", "gte", "lt", "lte"];
    let mut cases = comparison_names
        .iter()
        .filter_map(|func_name| {
            BUILTIN_FUNCTIONS
                .funcs
                .get(*func_name)
                .map(|funcs| (*func_name, funcs))
        })
        .flat_map(|(func_name, funcs)| {
            funcs.iter().filter_map(move |(func, _)| {
                let args = &func.signature.args_type;
                if args.len() != 2 || args[0] != args[1] {
                    return None;
                }
                let value_kind = SmokeValueKind::from_data_type(&args[0])?;
                let data_type = args[0].clone();
                Some([
                    SmokeComparisonCase {
                        func_name: func_name.to_string(),
                        data_type: data_type.clone(),
                        value_kind,
                        nullable: false,
                    },
                    SmokeComparisonCase {
                        func_name: func_name.to_string(),
                        data_type: data_type.wrap_nullable(),
                        value_kind,
                        nullable: true,
                    },
                ])
            })
        })
        .flatten()
        .collect::<Vec<_>>();
    cases.sort_by(|left, right| {
        left.func_name
            .cmp(&right.func_name)
            .then_with(|| format!("{:?}", left.data_type).cmp(&format!("{:?}", right.data_type)))
            .then_with(|| left.nullable.cmp(&right.nullable))
    });
    cases.dedup_by(|left, right| {
        left.func_name == right.func_name
            && left.data_type == right.data_type
            && left.nullable == right.nullable
    });
    assert!(
        comparison_names
            .iter()
            .all(|func_name| cases.iter().any(|case| case.func_name == *func_name)),
        "smoke case reflection should cover every comparison function"
    );
    assert!(
        cases
            .iter()
            .any(|case| matches!(case.value_kind, SmokeValueKind::String)),
        "smoke case reflection should include non-numeric comparison variants"
    );
    assert!(
        cases.iter().any(|case| case.nullable),
        "smoke case reflection should include nullable variants"
    );
    cases
}

fn assert_estimator_smoke_invariants(
    estimator: &SelectivityEstimator,
    estimated_rows: f64,
    cardinality: f64,
    data_type: &DataType,
) -> std::result::Result<(), TestCaseError> {
    prop_assert!(estimated_rows.is_finite(), "estimated rows must be finite");
    prop_assert!(estimated_rows >= 0.0, "estimated rows must be non-negative");
    prop_assert!(
        estimated_rows <= cardinality,
        "estimated rows must not exceed input cardinality"
    );

    for stat in estimator.column_stats().values() {
        if let Some(expected) = stat.ndv.expected {
            prop_assert!(expected.is_finite(), "ndv must stay finite");
            prop_assert!(expected >= 0.0, "ndv must stay non-negative");
        }
        prop_assert!(
            stat.null_count.expected() <= cardinality.ceil(),
            "null count must not exceed input cardinality"
        );
        let arg_stat = stat.to_arg_stat(data_type).map_err(|err| {
            TestCaseError::fail(format!("column stat should convert to ArgStat: {err}"))
        })?;
        arg_stat
            .check_consistency_with_type(Some(data_type))
            .map_err(|err| TestCaseError::fail(format!("ArgStat invariant failed: {err}")))?;
        if let Some(histogram) = &stat.histogram {
            prop_assert!(
                histogram.num_values().is_finite(),
                "histogram row count must stay finite"
            );
            prop_assert!(
                histogram.num_values() >= 0.0,
                "histogram row count must stay non-negative"
            );
            prop_assert!(
                histogram.ndv().expected.is_some_and(f64::is_finite),
                "histogram ndv must stay finite"
            );
            prop_assert!(
                histogram.ndv().expected.is_some_and(|ndv| ndv >= 0.0),
                "histogram ndv must stay non-negative"
            );
        }
    }

    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn comparison_selectivity_smoke(
        base in 0_i64..1_000_000,
        width in 0_i64..10_000,
        ndv_offset in 0_u64..10_000,
        rows_offset in 0_u64..10_000,
        null_offset in 0_u64..10_000,
        literal_offset in 0_i64..10_000,
        case_index in 0_usize..128,
        use_histogram in any::<bool>(),
        reverse_args in any::<bool>(),
    ) {
        let cases = reflected_comparison_smoke_cases();
        let case = &cases[case_index % cases.len()];
        let min_value = base;
        let max_value = base.saturating_add(width);
        let range_len = width as u64 + 1;
        let mut ndv = ndv_offset % range_len + 1;
        let cardinality = (rows_offset % 10_000 + ndv).max(1) as f64;
        if use_histogram {
            ndv = ndv.max(3).min(cardinality as u64);
        }
        let null_count = if case.nullable {
            null_offset % (cardinality.ceil() as u64 + 1)
        } else {
            0
        };
        let literal = min_value
            .saturating_sub(width / 2)
            .saturating_add(literal_offset % (width.saturating_mul(2).saturating_add(1)));
        let min = case.value_kind.datum(min_value);
        let max = case.value_kind.datum(max_value);
        let histogram = if use_histogram {
            Some(HistogramBuilder::from_ndv(
                ndv,
                cardinality.ceil() as u64,
                Some((min.clone(), max.clone())),
                DEFAULT_HISTOGRAM_BUCKETS,
            ).expect("valid histogram for smoke type"))
        } else {
            None
        };
        let column_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
            min,
            max,
            ndv: NdvEstimate::exact(ndv as f64),
            null_count: StatCount::exact(null_count),
            histogram,
        })]);
        for stat in column_stats.values() {
            let arg_stat = stat
                .to_arg_stat(&case.data_type)
                .map_err(|err| TestCaseError::fail(format!("input column stat should convert to ArgStat: {err}")))?;
            arg_stat
                .check_consistency_with_type(Some(&case.data_type))
                .map_err(|err| TestCaseError::fail(format!("input ArgStat invariant failed: {err}")))?;
        }
        let column = column_expr("a", 0, case.data_type.clone());
        let constant = constant_expr(case.value_kind.scalar(literal));
        let expr = if reverse_args {
            comparison_expr(&case.func_name, constant, column)
        } else {
            comparison_expr(&case.func_name, column, constant)
        };

        let mut estimator = SelectivityEstimator::new(column_stats, StatCardinality::estimate(cardinality));
        let estimated_rows = estimator.apply(&[expr]).expect("selectivity estimation should not fail for valid comparison variants");
        assert_estimator_smoke_invariants(&estimator, estimated_rows, cardinality, &case.data_type)?;
    }
}
