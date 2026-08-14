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

use std::cmp::Ordering;
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
use crate::optimizer::ir::finite_range_ndv_upper;
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

                    match (
                        left_stat_info.statistics.precise_cardinality,
                        right_stat_info.statistics.precise_cardinality,
                    ) {
                        (Some(0), Some(0)) => return Ok(None),
                        (_, Some(0)) => return Ok(left.map(|stat| (output, stat))),
                        (Some(0), _) => return Ok(right.map(|stat| (output, stat))),
                        _ => {}
                    }

                    let (Some(left), Some(right)) = (left, right) else {
                        // TODO: Distinguish all-NULL column statistics from unknown statistics so
                        // a non-empty all-NULL branch can contribute its NULL count without
                        // discarding the other branch's value statistics.
                        return Ok(None);
                    };
                    let mut ndv = Self::merge_ndv(&left, &right)?;
                    let min = if left.min.compare(&right.min)? == Ordering::Less {
                        left.min
                    } else {
                        right.min
                    };
                    let max = if left.max.compare(&right.max)? == Ordering::Greater {
                        left.max
                    } else {
                        right.max
                    };

                    if let Some(upper) = finite_range_ndv_upper(&min, &max) {
                        ndv = ndv.reduce(upper);
                    }
                    let null_count = StatCount::sum(left.null_count, right.null_count);
                    Ok(Some((output, ColumnStat {
                        min,
                        max,
                        ndv,
                        null_count,
                        // Combining histograms requires aligning bucket boundaries and
                        // accounting for overlapping values. Dropping it is safer than
                        // exposing either child's distribution as the union distribution.
                        histogram: None,
                    })))
                },
            )
            .filter_map(Result::transpose)
            .collect::<Result<ColumnStatSet>>()?;

        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality,
                column_stats,
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        }))
    }

    fn merge_ndv(left: &ColumnStat, right: &ColumnStat) -> Result<NdvEstimate> {
        let ndv_upper = left.ndv.upper + right.ndv.upper;
        let ranges_disjoint = left.max.compare(&right.min)? == Ordering::Less
            || right.max.compare(&left.min)? == Ordering::Less;
        if ranges_disjoint
            && let (Some(left), Some(right)) = (left.ndv.expected, right.ndv.expected)
        {
            return Ok(NdvEstimate::new(left + right, ndv_upper));
        }

        if left.min.is_numeric()
            && let (Some(left), Some(left_ndv)) = (&left.histogram, left.ndv.expected)
            && let (Some(right), Some(right_ndv)) = (&right.histogram, right.ndv.expected)
            && let Some(intersection) = left.estimate_join_numeric_compatible(right)?
            && let Some(intersection_ndv) = intersection.ndv.expected
        {
            let lower = left_ndv.max(right_ndv);
            let expected = (left_ndv + right_ndv - intersection_ndv).clamp(lower, ndv_upper);
            return Ok(NdvEstimate::new(expected, ndv_upper));
        }

        Ok(match (left.ndv.expected, right.ndv.expected) {
            // Match join estimation's NDV fallback: use the larger expected
            // NDV when both sides have one, preserve the known side when only
            // one does, and become upper-only only when neither side has one.
            (Some(left), Some(right)) => NdvEstimate::new(left.max(right), ndv_upper),
            (Some(expected), None) | (None, Some(expected)) => {
                NdvEstimate::new(expected, ndv_upper)
            }
            (None, None) => NdvEstimate::upper_bound(ndv_upper),
        })
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
            // Keep newly synthesized multi-input Serial plans out of the
            // Serial-to-distributed path until mixed topologies are supported.
            return Ok(PhysicalProperty::new(Distribution::Serial));
        }

        Ok(PhysicalProperty::new(Distribution::Random))
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
