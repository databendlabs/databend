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

use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::ColumnStat;
use databend_common_sql::optimizer::ir::ColumnStatSet;
use databend_common_sql::optimizer::ir::HistogramBuilder;
use databend_common_sql::optimizer::ir::Ndv;
use databend_common_sql::optimizer::ir::SelectivityEstimator;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::FunctionCall;
use databend_common_statistics::DEFAULT_HISTOGRAM_BUCKETS;
use databend_common_statistics::Datum;
use databend_common_statistics::F64;
use databend_common_statistics::Histogram;
use databend_common_statistics::TypedHistogram;
use databend_common_statistics::TypedHistogramBucket;
use proptest::prelude::*;

fn column_expr(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
    let column = ColumnBindingBuilder::new(
        name.to_string(),
        Symbol::new(index),
        Box::new(data_type),
        Visibility::Visible,
    )
    .build();
    ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column })
}

fn constant_expr(value: Scalar) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr { span: None, value })
}

fn comparison_expr(func_name: &str, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: func_name.to_string(),
        params: vec![],
        arguments: vec![left, right],
    })
}

#[test]
fn zero_cardinality_comparison_selectivity_is_finite() {
    let column_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(10),
        ndv: Ndv::Stat(0.0),
        null_count: 0,
        histogram: None,
    })]);
    let expr = comparison_expr(
        "gt",
        column_expr("a", 0, DataType::Number(NumberDataType::UInt64)),
        constant_expr(Scalar::Number(NumberScalar::UInt64(5))),
    );

    let mut estimator = SelectivityEstimator::new(column_stats, 0.0);
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
        ndv: Ndv::Stat(100.0),
        null_count: 0,
        histogram: Some(Histogram::Float(TypedHistogram {
            accuracy: false,
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

    let mut estimator = SelectivityEstimator::new(column_stats, 100.0);
    let estimated_rows = estimator
        .apply(&[expr])
        .expect("distorted histogram comparison should estimate");
    let stat = &estimator.column_stats()[&Symbol::new(0)];

    assert!((estimated_rows - 89.0).abs() < f64::EPSILON);
    assert_eq!(stat.min, Datum::UInt(100));
    assert_eq!(stat.max, Datum::UInt(1000));
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
        prop_assert!(stat.ndv.value().is_finite(), "ndv must stay finite");
        prop_assert!(stat.ndv.value() >= 0.0, "ndv must stay non-negative");
        prop_assert!(
            (stat.null_count as f64) <= cardinality.ceil(),
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
                histogram.num_distinct_values().is_finite(),
                "histogram ndv must stay finite"
            );
            prop_assert!(
                histogram.num_distinct_values() >= 0.0,
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
            ndv: Ndv::Stat(ndv as f64),
            null_count,
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

        let mut estimator = SelectivityEstimator::new(column_stats, cardinality);
        let estimated_rows = estimator.apply(&[expr]).expect("selectivity estimation should not fail for valid comparison variants");
        assert_estimator_smoke_invariants(&estimator, estimated_rows, cardinality, &case.data_type)?;
    }
}
