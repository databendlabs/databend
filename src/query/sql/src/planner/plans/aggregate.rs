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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_statistics::NdvEstimate;
use databend_common_statistics::StatCount;
use databend_common_statistics::TypedHistogram;
use itertools::Itertools;

use crate::ColumnSet;
use crate::ScalarExpr;
use crate::Symbol;
use crate::binder::is_grouping_id_item;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::ColumnStatSet;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatContext;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::Operator;
use crate::plans::PhysicalOperator;
use crate::plans::RelOp;
use crate::plans::ScalarItem;
use crate::plans::sort::SortItem;

const DEFAULT_AGGREGATE_RATIO: f64 = 1f64 / 3f64;
const AGGREGATE_COLUMN_CORRELATION_COEFFICIENT: f64 = 0.75_f64;

/// The maximum number of distinct keys under GROUPING SETS, ROLLUP or CUBE:
/// each key takes one bit of the `UInt32` `_grouping_id` column.
pub const MAX_GROUPING_KEYS: usize = u32::BITS as usize;

/// Information for `GROUPING SETS`.
/// See the comment of [`crate::planner::binder::aggregate::GroupingSetsInfo`].
///
/// Every key of a grouping-sets aggregate is a dedicated nullable symbol that
/// receives the injected NULLs. Aggregate arguments and sort keys never read
/// these symbols; they read their own pre-aggregate inputs.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct GroupingSets {
    /// The index of the virtual column `_grouping_id`. It's valid only if `grouping_sets` is not empty.
    pub grouping_id_index: Symbol,
    /// The keys of each grouping set.
    pub sets: Vec<Vec<Symbol>>,
}

impl GroupingSets {
    /// Whether `key` is absent from some grouping set and therefore receives
    /// injected NULLs. Keys present in every set, such as correlated columns
    /// added by decorrelation, keep their input values.
    pub fn is_rolled_up(&self, key: Symbol) -> bool {
        self.sets.iter().any(|set| !set.contains(&key))
    }

    /// The `_grouping_id` value of each set, given the keys in group item
    /// order (`_grouping_id` itself is skipped). Bit `i` is set when the
    /// `i`-th key is absent from the set, i.e. NULL in its rows. Example: for
    /// keys `[a, b]`, sets `[[a, b], [a], [b], []]` have ids `00, 10, 01, 11`.
    ///
    /// Only absent keys set a bit. The binder caps the keys of a GROUP BY at
    /// [`MAX_GROUPING_KEYS`], and keys added later by decorrelation follow them
    /// and belong to every set, so every bit fits in `_grouping_id`.
    pub fn grouping_ids(&self, group_bys: impl IntoIterator<Item = Symbol>) -> Vec<u32> {
        let group_bys = group_bys
            .into_iter()
            .filter(|index| *index != self.grouping_id_index)
            .collect::<Vec<_>>();
        self.sets
            .iter()
            .map(|set| {
                group_bys
                    .iter()
                    .enumerate()
                    .filter(|(_, key)| !set.contains(key))
                    .fold(0u32, |id, (i, _)| id | (1 << i))
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Aggregate {
    // group by scalar expressions, such as: group by col1, col2;
    pub group_items: Vec<ScalarItem>,
    // aggregate scalar expressions, such as: sum(col1), count(*);
    pub aggregate_functions: Vec<ScalarItem>,
    // True if the plan is generated from distinct, else the plan is a normal aggregate;
    pub from_distinct: bool,
    pub rank_limit: Option<(Vec<SortItem>, usize)>,

    pub grouping_sets: Option<GroupingSets>,
}

impl Aggregate {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for group_item in &self.group_items {
            used_columns.insert(group_item.index);
            group_item.scalar.collect_used_columns(&mut used_columns);
        }
        for agg in &self.aggregate_functions {
            used_columns.insert(agg.index);
            agg.scalar.collect_used_columns(&mut used_columns);
        }
        Ok(used_columns)
    }

    pub fn group_columns(&self) -> Result<ColumnSet> {
        let mut col_set = ColumnSet::new();
        for group_item in &self.group_items {
            col_set.insert(group_item.index);
            group_item.scalar.collect_used_columns(&mut col_set);
        }
        Ok(col_set)
    }

    pub fn derive_agg_stats(&self, stat_info: Arc<StatInfo>) -> Result<Arc<StatInfo>> {
        let column_stats = &stat_info.statistics.column_stats;
        if self.group_items.is_empty() {
            return Ok(Arc::new(StatInfo {
                cardinality: 1.0,
                statistics: Statistics {
                    precise_cardinality: Some(1),
                    column_stats: column_stats.clone(),
                    top_n: Default::default(),
                    count_min_sketch: Default::default(),
                },
            }));
        }

        if let Some(grouping_sets) = &self.grouping_sets {
            return Ok(Arc::new(
                self.derive_grouping_sets_stats(grouping_sets, &stat_info),
            ));
        }

        let Some(cardinality) = estimate_group_count(
            column_stats,
            self.group_items.iter().map(|item| item.index),
            stat_info.cardinality,
        ) else {
            return Ok(Arc::new(StatInfo {
                cardinality: (stat_info.cardinality * DEFAULT_AGGREGATE_RATIO).max(1.0),
                statistics: Statistics {
                    precise_cardinality: None,
                    column_stats: column_stats.clone(),
                    top_n: Default::default(),
                    count_min_sketch: Default::default(),
                },
            }));
        };

        let mut column_stats = stat_info.statistics.column_stats.clone();
        for item in self.group_items.iter() {
            let item_stat = column_stats.get_mut(&item.index).unwrap();
            if self.group_items.len() == 1 {
                item_stat.set_ndv(item_stat.ndv().reduce(cardinality));
            }
            collapse_group_key_histogram(item_stat);
        }

        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        }))
    }

    /// GROUPING SETS output the union of one aggregation per set. A key column
    /// is NULL in every row of the sets that omit it, and `_grouping_id`
    /// identifies the set of each row; the estimate keeps both visible to
    /// predicates and joins above the aggregate.
    fn derive_grouping_sets_stats(
        &self,
        grouping_sets: &GroupingSets,
        stat_info: &StatInfo,
    ) -> StatInfo {
        let input_stats = &stat_info.statistics.column_stats;
        let fallback = (stat_info.cardinality * DEFAULT_AGGREGATE_RATIO).max(1.0);
        let set_cardinalities = grouping_sets
            .sets
            .iter()
            .map(|set| {
                if set.is_empty() {
                    // The grand-total set produces one row.
                    return 1.0;
                }
                estimate_group_count(input_stats, set.iter().copied(), stat_info.cardinality)
                    .unwrap_or(fallback)
            })
            .collect::<Vec<_>>();
        let cardinality = set_cardinalities.iter().sum::<f64>();

        let mut column_stats = input_stats.clone();
        for item in self.group_items.iter() {
            if is_grouping_id_item(item, grouping_sets.grouping_id_index) {
                continue;
            }
            let Some(item_stat) = column_stats.get_mut(&item.index) else {
                continue;
            };
            let summary_rows = grouping_sets
                .sets
                .iter()
                .zip(&set_cardinalities)
                .filter(|(set, _)| !set.contains(&item.index))
                .map(|(_, rows)| *rows)
                .sum::<f64>();
            let null_count = if summary_rows > 0.0 {
                StatCount::sum(
                    item_stat.null_count(),
                    StatCount::estimate(summary_rows, cardinality),
                )
            } else {
                item_stat.null_count()
            };
            item_stat.set_null_count(null_count.reduce(cardinality));
            item_stat.set_ndv(item_stat.ndv().reduce(cardinality));
            collapse_group_key_histogram(item_stat);
        }

        let grouping_ids =
            grouping_sets.grouping_ids(self.group_items.iter().map(|item| item.index));
        if let (Some(min), Some(max)) = (grouping_ids.iter().min(), grouping_ids.iter().max()) {
            column_stats.insert(grouping_sets.grouping_id_index, ColumnStat::UInt {
                min: *min as u64,
                max: *max as u64,
                ndv: NdvEstimate::exact(grouping_ids.iter().unique().count() as f64),
                null_count: StatCount::exact(0),
                histogram: None,
            });
        }

        StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        }
    }
}

/// Estimate the number of groups over `keys`, or `None` if any key lacks an
/// expected NDV.
fn estimate_group_count(
    column_stats: &ColumnStatSet,
    keys: impl Iterator<Item = Symbol>,
    input_cardinality: f64,
) -> Option<f64> {
    let mut cardinality = 1.0;
    for (i, key) in keys.enumerate() {
        let ndv = column_stats.get(&key)?.ndv().expected?;
        let x = if i > 1 {
            ndv * AGGREGATE_COLUMN_CORRELATION_COEFFICIENT.powi((i - 1) as i32)
        } else {
            ndv
        };
        cardinality *= x.max(1.0);
    }
    Some(cardinality.min(input_cardinality))
}

fn collapse_group_key_histogram(stat: &mut ColumnStat) {
    match stat {
        ColumnStat::Int { histogram, .. } => collapse_dense_histogram(histogram),
        ColumnStat::UInt { histogram, .. } => collapse_dense_histogram(histogram),
        ColumnStat::Float { histogram, .. } => collapse_dense_histogram(histogram),
        ColumnStat::Bytes { histogram, .. } => collapse_dense_histogram(histogram),
        ColumnStat::Boolean { .. } | ColumnStat::AllNull { .. } => {}
    }
}

fn collapse_dense_histogram<T>(histogram: &mut Option<TypedHistogram<T>>) {
    let Some(histogram) = histogram else {
        return;
    };
    // When there is a high probability that eager aggregation is better, we
    // will update the histogram.
    if histogram
        .ndv()
        .expected
        .is_some_and(|ndv| histogram.num_values() >= ndv * 10.0)
    {
        histogram.collapse_counts_to_distinct();
    }
}

impl Operator for Aggregate {
    fn rel_op(&self) -> RelOp {
        RelOp::Aggregate
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        let iter = self.group_items.iter().map(|expr| &expr.scalar);
        let iter = iter.chain(self.aggregate_functions.iter().map(|expr| &expr.scalar));
        Box::new(iter)
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = ColumnSet::new();
        for group_item in self.group_items.iter() {
            output_columns.insert(group_item.index);
        }
        for agg in self.aggregate_functions.iter() {
            output_columns.insert(agg.index);
        }

        // GROUPING SETS rewrites use local producer symbols that are not necessarily
        // exposed by the child property. Treating those symbols as outer references
        // makes unrelated full outer joins fail during decorrelation.
        let outer_columns = if self.grouping_sets.is_some() {
            input_prop
                .outer_columns
                .difference(&output_columns)
                .cloned()
                .collect()
        } else {
            self.derive_outer_columns(input_prop.outer_columns.clone(), &input_prop.output_columns)
        };

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(input_prop.used_columns.clone());

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr, stat_ctx: &StatContext) -> Result<Arc<StatInfo>> {
        let stat_info = rel_expr.derive_cardinality_child(0, stat_ctx)?;
        self.derive_agg_stats(stat_info)
    }
}

/// The implementation phase of an aggregate. Logical aggregates have no phase.
///
/// ```compile_fail
/// use databend_common_sql::plans::AggregateMode;
/// let mode = AggregateMode::Initial;
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub enum AggregateMode {
    Partial,
    Final,
}

/// An executable aggregation over the shared relation description. Keeping the
/// phase outside `Aggregate` prevents logical rewrites from handling state rows.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PhysicalAggregate {
    pub mode: AggregateMode,
    pub aggregate: Aggregate,
}

impl PhysicalAggregate {
    pub fn new(aggregate: Aggregate, mode: AggregateMode) -> Self {
        Self { mode, aggregate }
    }

    pub fn get_distribution_keys(&self, before_partial: bool) -> Result<Vec<ScalarExpr>> {
        if before_partial {
            self.aggregate
                .group_items
                .iter()
                .enumerate()
                .map(|(index, item)| item.bound_column_expr(format!("_group_item_{}", index)))
                .collect()
        } else {
            Ok(vec![
                self.aggregate.group_items[0].bound_column_expr("_group_item_0".to_string())?,
            ])
        }
    }

    fn get_distribution(&self, ctx: Arc<dyn TableContext>) -> Result<Distribution> {
        let settings = ctx.get_settings();

        // Group aggregation, enforce `Hash` distribution
        let mut group_by_shuffle_mode = settings.get_group_by_shuffle_mode()?;

        // Grouping set must be merged before_merge
        if self.aggregate.grouping_sets.is_some() {
            group_by_shuffle_mode = "before_merge".to_string();
        }

        match group_by_shuffle_mode.as_str() {
            "before_partial" => Ok(Distribution::NodeToNodeHash(
                self.get_distribution_keys(true)?,
            )),
            "before_merge" => Ok(Distribution::NodeToNodeHash(
                self.get_distribution_keys(false)?,
            )),
            value => Err(ErrorCode::Internal(format!(
                "Bad settings value group_by_shuffle_mode = {:?}",
                value
            ))),
        }
    }
}

impl Operator for PhysicalAggregate {
    fn rel_op(&self) -> RelOp {
        RelOp::Aggregate
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        self.aggregate.scalar_expr_iter()
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let prop = self.aggregate.derive_relational_prop(rel_expr)?;
        if self.mode == AggregateMode::Partial {
            return Ok(prop);
        }
        // Final consumes aggregate states, not the original scalar arguments.
        // Those arguments must not become spurious correlated outer references.
        let mut prop = Arc::unwrap_or_clone(prop);
        prop.outer_columns = rel_expr
            .derive_relational_prop_child(0)?
            .outer_columns
            .clone();
        Ok(Arc::new(prop))
    }

    fn derive_stats(&self, rel_expr: &RelExpr, stat_ctx: &StatContext) -> Result<Arc<StatInfo>> {
        if self.mode == AggregateMode::Final {
            return rel_expr.derive_cardinality_child(0, stat_ctx);
        }
        self.aggregate.derive_stats(rel_expr, stat_ctx)
    }
}

impl PhysicalOperator for PhysicalAggregate {
    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        let input_physical_prop = rel_expr.derive_physical_prop_child(0)?;

        if input_physical_prop.distribution == Distribution::Serial {
            return Ok(input_physical_prop);
        }

        match self.mode {
            AggregateMode::Partial => {
                // TODO(leiysky): in current implementation we are using the `_group_by_key` produced
                // by partial aggregation as the distribution key, which is not exactly the same with
                // `Hash(group_items)`. Because of this we cannot leverage the distribution key to
                // satisfy the required hash distribution from parent node. We should fix this in the future.
                Ok(PhysicalProperty {
                    distribution: Distribution::Random,
                })
            }
            AggregateMode::Final => {
                if self.aggregate.group_items.is_empty() {
                    // Scalar aggregation
                    Ok(PhysicalProperty {
                        distribution: Distribution::Serial,
                    })
                } else {
                    // The distribution should have been derived by partial aggregation
                    Ok(PhysicalProperty {
                        distribution: input_physical_prop.distribution,
                    })
                }
            }
        }
    }

    fn compute_required_prop_child(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        let child_physical_prop = rel_expr.derive_physical_prop_child(0)?;

        if child_physical_prop.distribution == Distribution::Serial {
            return Ok(required);
        }

        match self.mode {
            AggregateMode::Partial => {
                if self.aggregate.group_items.is_empty() {
                    // Scalar aggregation
                    required.distribution = Distribution::Any;
                } else {
                    required.distribution = self.get_distribution(ctx)?;
                }
            }
            AggregateMode::Final => {
                if self.aggregate.group_items.is_empty() {
                    // Scalar aggregation
                    required.distribution = Distribution::Serial;
                } else {
                    // The distribution should have been derived by partial aggregation
                    required.distribution = Distribution::Any;
                }
            }
        }
        Ok(required)
    }

    fn compute_required_prop_children(
        &self,
        ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        let mut children_required = vec![];

        match self.mode {
            AggregateMode::Partial => {
                if self.aggregate.group_items.is_empty() {
                    // Scalar aggregation
                    children_required.push(vec![RequiredProperty {
                        distribution: Distribution::Any,
                    }]);
                    children_required.push(vec![RequiredProperty {
                        distribution: Distribution::Serial,
                    }]);
                } else {
                    children_required.push(vec![RequiredProperty {
                        distribution: self.get_distribution(ctx)?,
                    }]);
                }
            }
            AggregateMode::Final => {
                if self.aggregate.group_items.is_empty() {
                    // Scalar aggregation
                    children_required.push(vec![RequiredProperty {
                        distribution: Distribution::Serial,
                    }]);
                } else {
                    // The distribution should have been derived by partial aggregation
                    children_required.push(vec![RequiredProperty {
                        distribution: Distribution::Any,
                    }]);
                }
            }
        }
        Ok(children_required)
    }
}
