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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;

use crate::ColumnSet;
use crate::ScalarExpr;
use crate::Symbol;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::ColumnStatSet;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::EvalScalar;
use crate::plans::Operator;
use crate::plans::RelOp;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UnionAll {
    // We'll cast the output of union to the expected data type by the cast expr at runtime.
    // Left input symbol and its optional coercion expression, aligned by output position.
    pub left_outputs: Vec<(Symbol, Option<ScalarExpr>)>,
    // Right input symbol and its optional coercion expression, aligned by output position.
    pub right_outputs: Vec<(Symbol, Option<ScalarExpr>)>,
    // Recursive cte scan names
    // For example: `with recursive t as (select 1 as x union all select m.x+f.x from t as m, t as f where m.x < 3) select * from t`
    // The `cte_scan_names` are `m` and `f`
    pub cte_scan_names: Vec<String>,
    pub logical_recursive_cte_id: Option<u32>,
    pub output_indexes: Vec<Symbol>,
}

impl UnionAll {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for (idx, _) in &self.left_outputs {
            used_columns.insert(*idx);
        }
        for (idx, _) in &self.right_outputs {
            used_columns.insert(*idx);
        }
        Ok(used_columns)
    }

    fn derive_union_stats(
        &self,
        left_stat_info: Arc<StatInfo>,
        right_stat_info: Arc<StatInfo>,
    ) -> Result<Arc<StatInfo>> {
        let cardinality = left_stat_info.cardinality + right_stat_info.cardinality;

        let precise_cardinality = if let Some(left_cardinality) =
            left_stat_info.statistics.precise_cardinality
            && let Some(right_cardinality) = right_stat_info.statistics.precise_cardinality
        {
            Some(left_cardinality + right_cardinality)
        } else {
            None
        };

        let left_cardinality = left_stat_info
            .statistics
            .precise_cardinality
            .map(StatCardinality::exact)
            .unwrap_or_else(|| StatCardinality::estimate(left_stat_info.cardinality));
        let right_cardinality = right_stat_info
            .statistics
            .precise_cardinality
            .map(StatCardinality::exact)
            .unwrap_or_else(|| StatCardinality::estimate(right_stat_info.cardinality));

        debug_assert_eq!(self.left_outputs.len(), self.right_outputs.len());
        debug_assert_eq!(self.left_outputs.len(), self.output_indexes.len());

        let column_stats = self
            .left_outputs
            .iter()
            .zip(&self.right_outputs)
            .zip(self.output_indexes.iter().copied())
            .map(
                |(((left_output, left_expr), (right_output, right_expr)), output)| {
                    let left = {
                        let statistics = &left_stat_info.statistics;
                        match left_expr.as_ref() {
                            Some(expr) => {
                                EvalScalar::derive_item_stat(expr, statistics, left_cardinality)?
                            }
                            None => statistics.column_stats.get(left_output).cloned(),
                        }
                    };
                    let right = {
                        let statistics = &right_stat_info.statistics;
                        match right_expr.as_ref() {
                            Some(expr) => {
                                EvalScalar::derive_item_stat(expr, statistics, right_cardinality)?
                            }
                            None => statistics.column_stats.get(right_output).cloned(),
                        }
                    };

                    debug_assert!(
                        left_stat_info.statistics.precise_cardinality != Some(0) || left.is_none(),
                        "exactly empty UNION ALL left input must not carry column statistics"
                    );
                    debug_assert!(
                        right_stat_info.statistics.precise_cardinality != Some(0)
                            || right.is_none(),
                        "exactly empty UNION ALL right input must not carry column statistics"
                    );

                    let stat = match (left, right) {
                        (Some(left), Some(right)) => Self::merge_column_stats(left, right)?,
                        (Some(left), None)
                            if right_stat_info.statistics.precise_cardinality == Some(0) =>
                        {
                            left
                        }
                        (None, Some(right))
                            if left_stat_info.statistics.precise_cardinality == Some(0) =>
                        {
                            right
                        }
                        _ => return Ok(None),
                    };
                    Ok(Some((output, stat)))
                },
            )
            .filter_map(Result::transpose)
            .collect::<Result<ColumnStatSet>>()?;

        let left_max_cardinality = left_stat_info
            .max_cardinality
            .max(left_stat_info.cardinality);
        let right_max_cardinality = right_stat_info
            .max_cardinality
            .max(right_stat_info.cardinality);
        // UNION ALL retains every row from both branches, so branch risks are
        // additive. IEEE addition preserves infinity for unknown inputs.
        let max_cardinality = left_max_cardinality + right_max_cardinality;

        Ok(Arc::new(StatInfo {
            cardinality,
            max_cardinality: max_cardinality.max(cardinality),
            statistics: Statistics {
                precise_cardinality,
                column_stats,
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        }))
    }

    fn merge_column_stats(left: ColumnStat, right: ColumnStat) -> Result<ColumnStat> {
        let (left, right) = match (left, right) {
            (
                ColumnStat::AllNull {
                    null_count: left_null_count,
                },
                ColumnStat::AllNull {
                    null_count: right_null_count,
                },
            ) => {
                return Ok(ColumnStat::AllNull {
                    null_count: StatCount::sum(left_null_count, right_null_count),
                });
            }
            (ColumnStat::AllNull { null_count }, mut values)
            | (mut values, ColumnStat::AllNull { null_count }) => {
                values.set_null_count(StatCount::sum(values.null_count(), null_count));
                return Ok(values);
            }
            pair => pair,
        };

        let left_histogram = left.histogram();
        let right_histogram = right.histogram();

        let (Some(left_bounds), Some(right_bounds)) = (left.bounds(), right.bounds()) else {
            return Err(databend_common_exception::ErrorCode::Internal(
                "UNION ALL value-statistics merge received all-NULL statistics",
            ));
        };
        let left_ndv = left.ndv();
        let right_ndv = right.ndv();
        let left_null_count = left.null_count();
        let right_null_count = right.null_count();

        let ndv_upper = left_ndv.upper + right_ndv.upper;
        let ranges_disjoint = left_bounds.is_disjoint(&right_bounds)?;
        let ndv = if ranges_disjoint
            && let (Some(left), Some(right)) = (left_ndv.expected, right_ndv.expected)
        {
            NdvEstimate::new(left + right, ndv_upper)
        } else if left_bounds.is_numeric()
            && let (Some(left_histogram), Some(left_expected_ndv)) =
                (left_histogram, left_ndv.expected)
            && let (Some(right_histogram), Some(right_expected_ndv)) =
                (right_histogram, right_ndv.expected)
            && let Some(intersection) =
                left_histogram.estimate_join_numeric_compatible(right_histogram)?
            && let Some(intersection_ndv) = intersection.ndv.expected
        {
            let lower = left_expected_ndv.max(right_expected_ndv);
            let expected =
                (left_expected_ndv + right_expected_ndv - intersection_ndv).clamp(lower, ndv_upper);
            NdvEstimate::new(expected, ndv_upper)
        } else {
            match (left_ndv.expected, right_ndv.expected) {
                (Some(left), Some(right)) => NdvEstimate::new(left.max(right), ndv_upper),
                (Some(expected), None) | (None, Some(expected)) => {
                    NdvEstimate::new(expected, ndv_upper)
                }
                (None, None) => NdvEstimate::upper_bound(ndv_upper),
            }
        };

        let bounds = left_bounds.union(right_bounds)?;
        ColumnStat::new(
            bounds,
            ndv,
            StatCount::sum(left_null_count, right_null_count),
            // Combining histograms requires aligning bucket boundaries and
            // accounting for overlapping values. Dropping it is safer than
            // exposing either child's distribution as the union distribution.
            None,
        )
        .map_err(databend_common_exception::ErrorCode::Internal)
    }
}

impl Operator for UnionAll {
    fn rel_op(&self) -> RelOp {
        RelOp::UnionAll
    }

    fn arity(&self) -> usize {
        2
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;

        // Derive output columns
        let output_columns = self.output_indexes.iter().cloned().collect();
        // Derive outer columns
        let mut outer_columns = left_prop.outer_columns.clone();
        outer_columns = outer_columns
            .union(&right_prop.outer_columns)
            .cloned()
            .collect();

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(left_prop.used_columns.clone());
        used_columns.extend(right_prop.used_columns.clone());

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        let left_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        let right_physical_prop = rel_expr.derive_physical_prop_child(1)?;

        if left_physical_prop.distribution == Distribution::Serial
            || right_physical_prop.distribution == Distribution::Serial
        {
            return Ok(PhysicalProperty {
                distribution: Distribution::Serial,
            });
        }

        Ok(PhysicalProperty {
            distribution: Distribution::Random,
        })
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let left_stat_info = rel_expr.derive_cardinality_child(0)?;
        let right_stat_info = rel_expr.derive_cardinality_child(1)?;
        self.derive_union_stats(left_stat_info, right_stat_info)
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let required = required.clone();
        let left_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        let right_physical_prop = rel_expr.derive_physical_prop_child(1)?;
        if left_physical_prop.distribution == Distribution::Serial
            || right_physical_prop.distribution == Distribution::Serial
            || required.distribution == Distribution::Serial
        {
            Ok(RequiredProperty {
                distribution: Distribution::Serial,
            })
        } else {
            Ok(RequiredProperty {
                distribution: Distribution::Random,
            })
        }
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        // (Any, Any)
        let mut children_required = vec![vec![
            RequiredProperty {
                distribution: Distribution::Any,
            },
            RequiredProperty {
                distribution: Distribution::Any,
            },
        ]];

        // (Serial, Serial)
        children_required.push(vec![
            RequiredProperty {
                distribution: Distribution::Serial,
            },
            RequiredProperty {
                distribution: Distribution::Serial,
            },
        ]);

        Ok(children_required)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_all_sums_branch_risk_bounds() -> Result<()> {
        let union = UnionAll {
            left_outputs: vec![],
            right_outputs: vec![],
            cte_scan_names: vec![],
            logical_recursive_cte_id: None,
            output_indexes: vec![],
        };
        let stat = |cardinality, max_cardinality| {
            Arc::new(StatInfo {
                cardinality,
                max_cardinality,
                statistics: Statistics::default(),
            })
        };

        let output =
            union.derive_union_stats(stat(50_000.0, 60_000_000.0), stat(50_000.0, 60_000_000.0))?;
        assert_eq!(output.cardinality, 100_000.0);
        assert_eq!(output.max_cardinality, 120_000_000.0);

        let unknown = union.derive_union_stats(stat(1.0, f64::INFINITY), stat(1.0, 10.0))?;
        assert!(unknown.max_cardinality.is_infinite());
        Ok(())
    }
}
