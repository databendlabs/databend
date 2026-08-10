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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::ColumnSet;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Symbol;
use databend_common_sql::optimizer::ir::ColumnStat;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::StatInfo;
use databend_common_sql::optimizer::ir::Statistics;
use databend_common_sql::optimizer::optimizers::rule::Rule;
use databend_common_sql::optimizer::optimizers::rule::RuleFilterNulls;
use databend_common_sql::optimizer::optimizers::rule::TransformResult;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::Scan;
use databend_common_statistics::Datum;

use crate::sql::planner::optimizer::test_utils::ExprBuilder;

const CARDINALITY: f64 = 100.0;

#[test]
fn filter_nulls_skips_null_safe_join_keys() -> anyhow::Result<()> {
    let join = join_with_null_ratio(true);

    let transformed = apply_filter_nulls(&join)?;

    assert!(
        matches!(transformed.child(0)?.plan(), RelOperator::Scan(_)),
        "left side of a null-safe join must not get an is_not_null filter"
    );
    assert!(
        matches!(transformed.child(1)?.plan(), RelOperator::Scan(_)),
        "right side of a null-safe join must not get an is_not_null filter"
    );

    Ok(())
}

#[test]
fn filter_nulls_still_pushes_for_regular_join_keys() -> anyhow::Result<()> {
    let join = join_with_null_ratio(false);

    let transformed = apply_filter_nulls(&join)?;

    assert!(
        has_is_not_null_filter(transformed.child(0)?, Symbol::new(0)),
        "left side of a regular join should get an is_not_null filter"
    );
    assert!(
        has_is_not_null_filter(transformed.child(1)?, Symbol::new(1)),
        "right side of a regular join should get an is_not_null filter"
    );

    Ok(())
}

fn join_with_null_ratio(is_null_equal: bool) -> SExpr {
    let mut builder = ExprBuilder::new();
    let left_key = builder.column("t1.k", 0, "k", nullable_int64(), "t1", 0);
    let right_key = builder.column("t2.k", 1, "k", nullable_int64(), "t2", 1);

    let left_scan = scan_with_stats(0, Symbol::new(0), 25);
    let right_scan = scan_with_stats(1, Symbol::new(1), 25);
    let condition = builder.join_condition(left_key, right_key, is_null_equal);

    builder.join(left_scan, right_scan, vec![condition], JoinType::Inner)
}

fn apply_filter_nulls(s_expr: &SExpr) -> Result<SExpr> {
    let rule = RuleFilterNulls::new(true);
    let mut result = TransformResult::default();
    rule.apply(s_expr, &mut result)?;

    Ok(result.results()[0].clone())
}

fn nullable_int64() -> DataType {
    DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64)))
}

fn scan_with_stats(table_index: usize, column: Symbol, null_count: u64) -> SExpr {
    SExpr::create(
        RelOperator::Scan(Scan {
            table_index,
            columns: ColumnSet::from([column]),
            ..Default::default()
        }),
        vec![],
        None,
        None,
        Some(Arc::new(StatInfo {
            cardinality: CARDINALITY,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: HashMap::from([(column, column_stat(null_count))]),
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        })),
    )
}

fn column_stat(null_count: u64) -> ColumnStat {
    ColumnStat {
        min: Datum::Int(0),
        max: Datum::Int(99),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(null_count),
        histogram: None,
    }
}

fn has_is_not_null_filter(s_expr: &SExpr, column: Symbol) -> bool {
    let RelOperator::Filter(filter) = s_expr.plan() else {
        return false;
    };

    filter
        .predicates
        .iter()
        .any(|predicate| is_is_not_null_on_column(predicate, column))
}

fn is_is_not_null_on_column(predicate: &ScalarExpr, column: Symbol) -> bool {
    let ScalarExpr::FunctionCall(func) = predicate else {
        return false;
    };

    if func.func_name != "is_not_null" || func.arguments.len() != 1 {
        return false;
    }

    matches!(
        &func.arguments[0],
        ScalarExpr::BoundColumnRef(column_ref) if column_ref.column.index == column
    )
}
