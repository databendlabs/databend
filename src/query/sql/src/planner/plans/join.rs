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
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_storage::Datum;
use databend_common_storage::Histogram;
use databend_common_storage::DEFAULT_HISTOGRAM_BUCKETS;

use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::HistogramBuilder;
use crate::optimizer::ir::NewStatistic;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::optimizer::ir::UniformSampleSet;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::ColumnSet;
use crate::IndexType;

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    /// Mark Join is a special case of join that is used to process Any subquery and correlated Exists subquery.
    /// Left Mark output build fields and marker
    /// Left Mark Join use subquery as probe(left) side, it's blocked at `mark_join_blocks`
    LeftMark,
    /// Right Mark output probe fields and marker
    /// Right Mark Join use subquery as build(right) side, it's executed by streaming.
    RightMark,
    /// Single Join is a special kind of join that is used to process correlated scalar subquery.
    LeftSingle,
    RightSingle,
    /// Asof Join special for  Speed ​​up timestamp join
    Asof,
    LeftAsof,
    RightAsof,
}

impl JoinType {
    pub fn opposite(&self) -> JoinType {
        match self {
            JoinType::Left => JoinType::Right,
            JoinType::Right => JoinType::Left,
            JoinType::LeftSingle => JoinType::RightSingle,
            JoinType::RightSingle => JoinType::LeftSingle,
            JoinType::LeftSemi => JoinType::RightSemi,
            JoinType::RightSemi => JoinType::LeftSemi,
            JoinType::LeftAnti => JoinType::RightAnti,
            JoinType::RightAnti => JoinType::LeftAnti,
            JoinType::LeftMark => JoinType::RightMark,
            JoinType::RightMark => JoinType::LeftMark,
            JoinType::RightAsof => JoinType::LeftAsof,
            JoinType::LeftAsof => JoinType::RightAsof,
            _ => self.clone(),
        }
    }

    pub fn is_outer_join(&self) -> bool {
        matches!(
            self,
            JoinType::Left
                | JoinType::Right
                | JoinType::Full
                | JoinType::LeftSingle
                | JoinType::RightSingle
                | JoinType::LeftAsof
                | JoinType::RightAsof
        )
    }

    pub fn is_mark_join(&self) -> bool {
        matches!(self, JoinType::LeftMark | JoinType::RightMark)
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            JoinType::Inner => {
                write!(f, "INNER")
            }
            JoinType::Left => {
                write!(f, "LEFT OUTER")
            }
            JoinType::Right => {
                write!(f, "RIGHT OUTER")
            }
            JoinType::Full => {
                write!(f, "FULL OUTER")
            }
            JoinType::LeftSemi => {
                write!(f, "LEFT SEMI")
            }
            JoinType::LeftAnti => {
                write!(f, "LEFT ANTI")
            }
            JoinType::RightSemi => {
                write!(f, "RIGHT SEMI")
            }
            JoinType::RightAnti => {
                write!(f, "RIGHT ANTI")
            }
            JoinType::Cross => {
                write!(f, "CROSS")
            }
            JoinType::LeftMark => {
                write!(f, "LEFT MARK")
            }
            JoinType::RightMark => {
                write!(f, "RIGHT MARK")
            }
            JoinType::LeftSingle => {
                write!(f, "LEFT SINGLE")
            }
            JoinType::RightSingle => {
                write!(f, "RIGHT SINGLE")
            }
            JoinType::Asof => {
                write!(f, "ASOF")
            }
            JoinType::LeftAsof => {
                write!(f, "LEFT ASOF")
            }
            JoinType::RightAsof => {
                write!(f, "RIGHT ASOF")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HashJoinBuildCacheInfo {
    pub cache_idx: usize,
    pub columns: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JoinEquiCondition {
    pub left: ScalarExpr,
    pub right: ScalarExpr,
    // Used for "is (not) distinct from" and mark join
    pub is_null_equal: bool,
}

impl JoinEquiCondition {
    pub fn new(left: ScalarExpr, right: ScalarExpr, is_null_equal: bool) -> Self {
        Self {
            left,
            right,
            is_null_equal,
        }
    }

    pub fn new_conditions(
        left: Vec<ScalarExpr>,
        right: Vec<ScalarExpr>,
        is_null_equal: Vec<usize>,
    ) -> Vec<JoinEquiCondition> {
        left.into_iter()
            .zip(right)
            .enumerate()
            .map(|(index, (l, r))| JoinEquiCondition::new(l, r, is_null_equal.contains(&index)))
            .collect()
    }
}

static DEFAULT_EQ_SELECTIVITY: f64 = 0.005; // 1/200

/// Join operator. We will choose hash join by default.
/// In the case that using hash join, the right child
/// is always the build side, and the left child is always
/// the probe side.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Join {
    pub equi_conditions: Vec<JoinEquiCondition>,
    pub non_equi_conditions: Vec<ScalarExpr>,
    pub join_type: JoinType,
    // marker_index is for MarkJoin only.
    pub marker_index: Option<IndexType>,
    pub from_correlated_subquery: bool,
    // if we execute distributed merge into, we need to hold the
    // hash table to get not match data from source.
    pub need_hold_hash_table: bool,
    pub is_lateral: bool,
    // When left/right single join converted to inner join, record the original join type
    // and do some special processing during runtime.
    pub single_to_inner: Option<JoinType>,
    // Cache info for ExpressionScan.
    pub build_side_cache_info: Option<HashJoinBuildCacheInfo>,
}

impl Default for Join {
    fn default() -> Self {
        Self {
            equi_conditions: Vec::new(),
            non_equi_conditions: Vec::new(),
            join_type: JoinType::Cross,
            marker_index: None,
            from_correlated_subquery: false,
            need_hold_hash_table: false,
            is_lateral: false,
            single_to_inner: None,
            build_side_cache_info: None,
        }
    }
}

impl Join {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for condition in self.equi_conditions.iter() {
            used_columns = used_columns
                .union(&condition.left.used_columns())
                .cloned()
                .collect();
            used_columns = used_columns
                .union(&condition.right.used_columns())
                .cloned()
                .collect();
        }
        for condition in self.non_equi_conditions.iter() {
            used_columns = used_columns
                .union(&condition.used_columns())
                .cloned()
                .collect();
        }
        Ok(used_columns)
    }

    fn inner_join_cardinality(
        &self,
        left_cardinality: f64,
        right_cardinality: f64,
        left_statistics: &mut Statistics,
        right_statistics: &mut Statistics,
    ) -> Result<f64> {
        let mut join_selectivity = 1f64;
        let mut left_need_update_columns_stat = vec![];
        let mut right_need_update_columns_stat = vec![];

        for condition in self.equi_conditions.iter() {
            let left_condition = &condition.left;
            let right_condition = &condition.right;

            let left_used_columns = left_condition.used_columns();
            let right_used_columns = right_condition.used_columns();

            // Unable to calculate join_selectivity
            if left_used_columns.is_empty() || right_used_columns.is_empty() {
                continue;
            }

            let mut left_columns_stat = left_used_columns
                .iter()
                .filter_map(|x| left_statistics.column_stats.get(x).map(|v| (*x, v.clone())))
                .collect::<Vec<(IndexType, ColumnStat)>>();

            if left_used_columns.len() != left_columns_stat.len() {
                // The left table contains columns with unknown statistics.
                join_selectivity *= DEFAULT_EQ_SELECTIVITY;
                continue;
            }

            let mut right_columns_stat = right_used_columns
                .iter()
                .filter_map(|x| {
                    right_statistics
                        .column_stats
                        .get(x)
                        .map(|v| (*x, v.clone()))
                })
                .collect::<Vec<(IndexType, ColumnStat)>>();

            if right_used_columns.len() != right_columns_stat.len() {
                // The right table contains columns with unknown statistics.
                join_selectivity *= DEFAULT_EQ_SELECTIVITY;
                continue;
            }

            if left_columns_stat.len() != 1 || right_columns_stat.len() != 1 {
                join_selectivity *= DEFAULT_EQ_SELECTIVITY;
                continue;
            }

            let (left_idx, left_column_stat) = left_columns_stat.pop().unwrap();
            let (right_idx, right_column_stat) = right_columns_stat.pop().unwrap();

            if !left_column_stat.min.type_comparable(&right_column_stat.min) {
                continue;
            }

            let left_interval =
                UniformSampleSet::new(left_column_stat.min.clone(), left_column_stat.max.clone());

            let right_interval =
                UniformSampleSet::new(right_column_stat.min.clone(), right_column_stat.max.clone());

            if !left_interval.has_intersection(&right_interval)? {
                join_selectivity = 0.0;
                break;
            }

            let (min, max) = left_interval.intersection(&right_interval)?;

            let has_histogram =
                left_column_stat.histogram.is_some() && right_column_stat.histogram.is_some();

            match left_column_stat.min {
                Datum::Int(_) | Datum::UInt(_) | Datum::Float(_) if has_histogram => {
                    // Calculate join selectivity from histogram
                    let left_histogram = left_column_stat.histogram.as_ref().unwrap();
                    let right_histogram = right_column_stat.histogram.as_ref().unwrap();
                    let (cardinality, ndv) =
                        evaluate_by_histogram(left_histogram, right_histogram)?;
                    join_selectivity *= cardinality / (left_cardinality * right_cardinality);

                    let new_statistic = NewStatistic {
                        min,
                        max,
                        ndv: Some(ndv),
                    };

                    left_need_update_columns_stat.push((left_idx, new_statistic.clone()));
                    right_need_update_columns_stat.push((right_idx, new_statistic));
                }
                _calculate_join_selectivity_from_ndv => {
                    let min_ndv = left_column_stat.ndv.min(right_column_stat.ndv);
                    let max_ndv = left_column_stat.ndv.max(right_column_stat.ndv);

                    if max_ndv != 0.0 {
                        join_selectivity *= 1_f64 / max_ndv;
                    }

                    let new_statistic = NewStatistic {
                        min,
                        max,
                        ndv: Some(min_ndv),
                    };
                    left_need_update_columns_stat.push((left_idx, new_statistic.clone()));
                    right_need_update_columns_stat.push((right_idx, new_statistic));
                }
            }
        }

        eprintln!("join_selectivity {}", join_selectivity);
        let mut join_cardinality = left_cardinality * right_cardinality * join_selectivity;
        join_cardinality = join_cardinality.min(left_cardinality * right_cardinality);

        for (need_update_columns_stat, column_stats) in [
            (left_need_update_columns_stat, left_statistics),
            (right_need_update_columns_stat, right_statistics),
        ] {
            for (idx, mut new_statistic) in need_update_columns_stat {
                let Some(col_stat) = column_stats.column_stats.get_mut(&idx) else {
                    continue;
                };

                if let Some(min) = new_statistic.min.as_ref() {
                    col_stat.min = min.clone();
                }

                if let Some(max) = new_statistic.max.as_ref() {
                    col_stat.max = max.clone();
                }

                if let Some(ndv) = new_statistic.ndv.as_ref() {
                    col_stat.ndv = *ndv;
                }

                if col_stat.histogram.is_some() {
                    col_stat.histogram = None;

                    let Some(min) = new_statistic.min.take() else {
                        continue;
                    };

                    let Some(max) = new_statistic.max.take() else {
                        continue;
                    };

                    let Some(ndv) = new_statistic.ndv.take() else {
                        continue;
                    };

                    if ndv as u64 <= 2 {
                        continue;
                    }

                    col_stat.histogram = Some(HistogramBuilder::from_ndv(
                        ndv as u64,
                        std::cmp::max(join_cardinality as u64, ndv as u64),
                        Some((min, max)),
                        DEFAULT_HISTOGRAM_BUCKETS,
                    )?);
                }
            }
        }

        Ok(join_cardinality)
    }

    pub fn has_null_equi_condition(&self) -> bool {
        self.equi_conditions
            .iter()
            .any(|condition| condition.is_null_equal)
    }

    pub fn derive_join_stats(
        &self,
        left_stat_info: Arc<StatInfo>,
        right_stat_info: Arc<StatInfo>,
    ) -> Result<Arc<StatInfo>> {
        let (left_cardinality, mut left_statistics) = (
            left_stat_info.cardinality,
            left_stat_info.statistics.clone(),
        );
        let (right_cardinality, mut right_statistics) = (
            right_stat_info.cardinality,
            right_stat_info.statistics.clone(),
        );
        // Evaluating join cardinality using histograms.
        // If histogram is None, will evaluate using NDV.
        let inner_join_cardinality = self.inner_join_cardinality(
            left_cardinality,
            right_cardinality,
            &mut left_statistics,
            &mut right_statistics,
        )?;
        let cardinality = match self.join_type {
            JoinType::Inner | JoinType::Asof | JoinType::Cross => inner_join_cardinality,
            JoinType::Left | JoinType::LeftAsof => {
                f64::max(left_cardinality, inner_join_cardinality)
            }
            JoinType::Right | JoinType::RightAsof => {
                f64::max(right_cardinality, inner_join_cardinality)
            }
            JoinType::Full => {
                f64::max(left_cardinality, inner_join_cardinality)
                    + f64::max(right_cardinality, inner_join_cardinality)
                    - inner_join_cardinality
            }
            JoinType::LeftSemi => f64::min(left_cardinality, inner_join_cardinality),
            JoinType::RightSemi => f64::min(right_cardinality, inner_join_cardinality),
            JoinType::LeftSingle | JoinType::RightMark | JoinType::LeftAnti => left_cardinality,
            JoinType::RightSingle | JoinType::LeftMark | JoinType::RightAnti => right_cardinality,
        };
        // Derive column statistics
        let column_stats = if cardinality == 0.0 {
            HashMap::new()
        } else {
            let mut column_stats = HashMap::new();
            column_stats.extend(left_statistics.column_stats);
            column_stats.extend(right_statistics.column_stats);
            column_stats
        };
        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
            },
        }))
    }

    pub fn replace_column(&mut self, old: IndexType, new: IndexType) -> Result<()> {
        for condition in &mut self.equi_conditions {
            condition.left.replace_column(old, new)?;
            condition.right.replace_column(old, new)?;
        }

        for condition in &mut self.non_equi_conditions {
            condition.replace_column(old, new)?;
        }

        if self.marker_index == Some(old) {
            self.marker_index = Some(new)
        }

        self.build_side_cache_info = None;

        Ok(())
    }

    pub fn has_subquery(&self) -> bool {
        self.equi_conditions
            .iter()
            .any(|condition| condition.left.has_subquery() || condition.right.has_subquery())
            || self
                .non_equi_conditions
                .iter()
                .any(|expr| expr.has_subquery())
    }
}

impl Operator for Join {
    fn rel_op(&self) -> RelOp {
        RelOp::Join
    }

    fn arity(&self) -> usize {
        2
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        let iter = self.equi_conditions.iter().map(|condition| &condition.left);
        let iter = iter.chain(
            self.equi_conditions
                .iter()
                .map(|condition| &condition.right),
        );
        let iter = iter.chain(self.non_equi_conditions.iter());
        Box::new(iter)
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;
        // Derive output columns
        let mut output_columns = left_prop.output_columns.clone();
        if let Some(mark_index) = self.marker_index {
            output_columns.insert(mark_index);
        }
        output_columns = output_columns
            .union(&right_prop.output_columns)
            .cloned()
            .collect();

        // Derive outer columns
        let mut outer_columns = left_prop.outer_columns.clone();
        outer_columns = outer_columns
            .union(&right_prop.outer_columns)
            .cloned()
            .collect();

        for condition in self.equi_conditions.iter() {
            let left_used_columns = condition.left.used_columns();
            let right_used_columns = condition.right.used_columns();
            let used_columns: ColumnSet = left_used_columns
                .union(&right_used_columns)
                .cloned()
                .collect();
            let outer = used_columns.difference(&output_columns).cloned().collect();
            outer_columns = outer_columns.union(&outer).cloned().collect();
        }
        outer_columns = outer_columns.difference(&output_columns).cloned().collect();

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
        let probe_prop = rel_expr.derive_physical_prop_child(0)?;
        let build_prop = rel_expr.derive_physical_prop_child(1)?;

        if probe_prop.distribution == Distribution::Serial
            || build_prop.distribution == Distribution::Serial
        {
            return Ok(PhysicalProperty {
                distribution: Distribution::Serial,
            });
        }

        if !matches!(self.join_type, JoinType::Inner | JoinType::Asof) {
            return Ok(PhysicalProperty {
                distribution: Distribution::Random,
            });
        }

        match (&probe_prop.distribution, &build_prop.distribution) {
            // If any side of the join is Broadcast, pass through the other side.
            (_, Distribution::Broadcast) => Ok(PhysicalProperty {
                distribution: probe_prop.distribution.clone(),
            }),

            // If both sides of the join are Hash, pass through the probe side.
            // Although the build side is also Hash, it is more efficient to
            // utilize the distribution on the probe side.
            // As soon as we support subset property, we can pass through both sides.
            (Distribution::Hash(_), Distribution::Hash(_)) => Ok(PhysicalProperty {
                distribution: probe_prop.distribution.clone(),
            }),

            // Otherwise use random distribution.
            _ => Ok(PhysicalProperty {
                distribution: Distribution::Random,
            }),
        }
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let left_stat_info = rel_expr.derive_cardinality_child(0)?;
        let right_stat_info = rel_expr.derive_cardinality_child(1)?;
        let stat_info = self.derive_join_stats(left_stat_info, right_stat_info)?;
        Ok(stat_info)
    }

    fn compute_required_prop_child(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();

        let probe_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        let build_physical_prop = rel_expr.derive_physical_prop_child(1)?;

        // if join/probe side is Serial or this is a non-equi join, we use Serial distribution
        if probe_physical_prop.distribution == Distribution::Serial
            || build_physical_prop.distribution == Distribution::Serial
            || (self.equi_conditions.is_empty() && !self.non_equi_conditions.is_empty())
        {
            // TODO(leiysky): we can enforce redistribution here
            required.distribution = Distribution::Serial;
            return Ok(required);
        }

        // Try to use broadcast join
        if !matches!(
            self.join_type,
            JoinType::Right
                | JoinType::Full
                | JoinType::RightAnti
                | JoinType::RightSemi
                | JoinType::LeftMark
                | JoinType::Asof
                | JoinType::LeftAsof
                | JoinType::RightAsof
        ) {
            let settings = ctx.get_settings();
            let left_stat_info = rel_expr.derive_cardinality_child(0)?;
            let right_stat_info = rel_expr.derive_cardinality_child(1)?;
            // The broadcast join is cheaper than the hash join when one input is at least (n − 1)× larger than the other
            // where n is the number of servers in the cluster.
            let broadcast_join_threshold = if settings.get_prefer_broadcast_join()? {
                (ctx.get_cluster().nodes.len() - 1) as f64
            } else {
                // Use a very large value to prevent broadcast join.
                1000.0
            };
            if !settings.get_enforce_shuffle_join()?
                && (right_stat_info.cardinality * broadcast_join_threshold
                    < left_stat_info.cardinality
                    || settings.get_enforce_broadcast_join()?)
            {
                if child_index == 1 {
                    required.distribution = Distribution::Broadcast;
                } else {
                    required.distribution = Distribution::Any;
                }
                return Ok(required);
            }
        }

        // Otherwise, use hash shuffle
        if child_index == 0 {
            let left_conditions = self
                .equi_conditions
                .iter()
                .map(|condition| condition.left.clone())
                .collect();
            required.distribution = Distribution::Hash(left_conditions);
        } else {
            let right_conditions = self
                .equi_conditions
                .iter()
                .map(|condition| condition.right.clone())
                .collect();
            required.distribution = Distribution::Hash(right_conditions);
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

        // For mark join with nullable eq comparison, ensure to use broadcast for subquery side
        if self.join_type.is_mark_join()
            && self.equi_conditions.len() == 1
            && self.has_null_equi_condition()
        {
            // subquery as left probe side
            if matches!(self.join_type, JoinType::LeftMark) {
                let conditions = self
                    .equi_conditions
                    .iter()
                    .map(|condition| condition.right.clone())
                    .collect();

                children_required.push(vec![
                    RequiredProperty {
                        distribution: Distribution::Broadcast,
                    },
                    RequiredProperty {
                        distribution: Distribution::Hash(conditions),
                    },
                ]);
            } else {
                // subquery as right build side
                let conditions = self
                    .equi_conditions
                    .iter()
                    .map(|condition| condition.left.clone())
                    .collect();

                children_required.push(vec![
                    RequiredProperty {
                        distribution: Distribution::Hash(conditions),
                    },
                    RequiredProperty {
                        distribution: Distribution::Broadcast,
                    },
                ]);
            }
            return Ok(children_required);
        }

        let settings = ctx.get_settings();
        if self.join_type != JoinType::Cross && !settings.get_enforce_broadcast_join()? {
            // (Hash, Hash)
            children_required.extend(self.equi_conditions.iter().map(|condition| {
                vec![
                    RequiredProperty {
                        distribution: Distribution::Hash(vec![condition.left.clone()]),
                    },
                    RequiredProperty {
                        distribution: Distribution::Hash(vec![condition.right.clone()]),
                    },
                ]
            }));
        }

        if !matches!(
            self.join_type,
            JoinType::Right
                | JoinType::Full
                | JoinType::RightAnti
                | JoinType::RightSemi
                | JoinType::LeftMark
                | JoinType::RightSingle
                | JoinType::Asof
                | JoinType::LeftAsof
                | JoinType::RightAsof
        ) && !settings.get_enforce_shuffle_join()?
        {
            // (Any, Broadcast)
            let left_distribution = Distribution::Any;
            let right_distribution = Distribution::Broadcast;
            children_required.push(vec![
                RequiredProperty {
                    distribution: left_distribution,
                },
                RequiredProperty {
                    distribution: right_distribution,
                },
            ]);
        }

        if children_required.is_empty() {
            // (Serial, Serial)
            children_required.push(vec![
                RequiredProperty {
                    distribution: Distribution::Serial,
                },
                RequiredProperty {
                    distribution: Distribution::Serial,
                },
            ]);
        }

        Ok(children_required)
    }
}

fn evaluate_by_histogram(left_hist: &Histogram, right_hist: &Histogram) -> Result<(f64, f64)> {
    let mut card = 0.0;
    let mut all_ndv = 0.0;
    for left_bucket in left_hist.buckets.iter() {
        let mut has_intersection = false;
        let left_num_rows = left_bucket.num_values();
        let left_ndv = left_bucket.num_distinct();
        let left_bucket_min = left_bucket.lower_bound().to_double()?;
        let left_bucket_max = left_bucket.upper_bound().to_double()?;
        for right_bucket in right_hist.buckets.iter() {
            let right_bucket_min = right_bucket.lower_bound().to_double()?;
            let right_bucket_max = right_bucket.upper_bound().to_double()?;
            if left_bucket_min < right_bucket_max && left_bucket_max > right_bucket_min {
                has_intersection = true;
                let right_num_rows = right_bucket.num_values();
                let right_ndv = right_bucket.num_distinct();

                // There are four cases for interleaving
                if right_bucket_min >= left_bucket_min && right_bucket_max <= left_bucket_max {
                    // 1. left bucket contains right bucket
                    // ---left_min---right_min---right_max---left_max---
                    let percentage =
                        (right_bucket_max - right_bucket_min) / (left_bucket_max - left_bucket_min);

                    let left_ndv = left_ndv * percentage;
                    let left_num_rows = left_num_rows * percentage;

                    let max_ndv = f64::max(left_ndv, right_ndv);
                    if max_ndv > 0.0 {
                        all_ndv += left_ndv.min(right_ndv);
                        card += left_num_rows * right_num_rows / max_ndv;
                    }
                } else if left_bucket_min >= right_bucket_min && left_bucket_max <= right_bucket_max
                {
                    // 2. right bucket contains left bucket
                    // ---right_min---left_min---left_max---right_max---
                    let percentage =
                        (left_bucket_max - left_bucket_min) / (right_bucket_max - right_bucket_min);

                    let right_ndv = right_ndv * percentage;
                    let right_num_rows = right_num_rows * percentage;

                    let max_ndv = f64::max(left_ndv, right_ndv);
                    if max_ndv > 0.0 {
                        all_ndv += left_ndv.min(right_ndv);
                        card += left_num_rows * right_num_rows / max_ndv;
                    }
                } else if left_bucket_min <= right_bucket_min && left_bucket_max <= right_bucket_max
                {
                    // 3. left bucket intersects with right bucket on the left
                    // ---left_min---right_min---left_max---right_max---
                    if left_bucket_max == right_bucket_min {
                        continue;
                    }
                    let left_percentage =
                        (left_bucket_max - right_bucket_min) / (left_bucket_max - left_bucket_min);
                    let right_percentage = (left_bucket_max - right_bucket_min)
                        / (right_bucket_max - right_bucket_min);

                    let left_ndv = left_ndv * left_percentage;
                    let left_num_rows = left_num_rows * left_percentage;
                    let right_ndv = right_ndv * right_percentage;
                    let right_num_rows = right_num_rows * right_percentage;

                    let max_ndv = f64::max(left_ndv, right_ndv);
                    if max_ndv > 0.0 {
                        all_ndv += left_ndv.min(right_ndv);
                        card += left_num_rows * right_num_rows / max_ndv;
                    }
                } else if left_bucket_min >= right_bucket_min && left_bucket_max >= right_bucket_max
                {
                    // 4. left bucket intersects with right bucket on the right
                    // ---right_min---left_min---right_max---left_max---
                    if right_bucket_max == left_bucket_min {
                        continue;
                    }
                    let left_percentage =
                        (right_bucket_max - left_bucket_min) / (left_bucket_max - left_bucket_min);
                    let right_percentage = (right_bucket_max - left_bucket_min)
                        / (right_bucket_max - right_bucket_min);

                    let left_ndv = left_ndv * left_percentage;
                    let left_num_rows = left_num_rows * left_percentage;
                    let right_ndv = right_ndv * right_percentage;
                    let right_num_rows = right_num_rows * right_percentage;

                    let max_ndv = f64::max(left_ndv, right_ndv);
                    if max_ndv > 0.0 {
                        all_ndv += left_ndv.min(right_ndv);
                        card += left_num_rows * right_num_rows / max_ndv;

                        // （left_num_rows * right_num_rows / 0.2） + （left_num_rows * right_num_rows / 0.3）
                    }
                }
            } else if has_intersection {
                break;
            }
        }
    }

    Ok((card, all_ndv.ceil()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_exception::Result;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_storage::Datum;
    use databend_common_storage::DEFAULT_HISTOGRAM_BUCKETS;

    use crate::optimizer::ir::ColumnStat;
    use crate::optimizer::ir::HistogramBuilder;
    use crate::optimizer::ir::Statistics;
    use crate::plans::join::DEFAULT_EQ_SELECTIVITY;
    use crate::plans::BoundColumnRef;
    use crate::plans::Join;
    use crate::plans::JoinEquiCondition;
    use crate::plans::JoinType;
    use crate::ColumnBindingBuilder;
    use crate::IndexType;
    use crate::ScalarExpr;
    use crate::Visibility;

    fn create_column_stat(min: Datum, max: Datum, ndv: f64, has_histogram: bool) -> ColumnStat {
        let histogram = if has_histogram {
            Some(
                HistogramBuilder::from_ndv(
                    ndv as u64,
                    ndv as u64 * 2,
                    Some((min.clone(), max.clone())),
                    DEFAULT_HISTOGRAM_BUCKETS,
                )
                .unwrap(),
            )
        } else {
            None
        };

        ColumnStat {
            min,
            max,
            ndv,
            histogram,
            null_count: 0,
        }
    }

    fn create_equi_condition(left_col: IndexType, right_col: IndexType) -> JoinEquiCondition {
        let left_column = ColumnBindingBuilder::new(
            String::from("column_name"),
            left_col,
            Box::new(DataType::Number(NumberDataType::Float32)),
            Visibility::Visible,
        );

        let left_bound_column_ref = BoundColumnRef {
            span: None,
            column: left_column.build(),
        };

        let right_column = ColumnBindingBuilder::new(
            String::from("column_name"),
            right_col,
            Box::new(DataType::Number(NumberDataType::Float32)),
            Visibility::Visible,
        );

        let right_bound_column_ref = BoundColumnRef {
            span: None,
            column: right_column.build(),
        };

        JoinEquiCondition::new(
            ScalarExpr::BoundColumnRef(left_bound_column_ref),
            ScalarExpr::BoundColumnRef(right_bound_column_ref),
            false,
        )
    }

    fn create_join_with_equi_conditions(conditions: Vec<(IndexType, IndexType)>) -> Join {
        Join {
            equi_conditions: conditions
                .into_iter()
                .map(|(l, r)| create_equi_condition(l, r))
                .collect(),
            non_equi_conditions: vec![],
            join_type: JoinType::Cross,
            marker_index: None,
            from_correlated_subquery: false,
            need_hold_hash_table: false,
            is_lateral: false,
            single_to_inner: None,
            build_side_cache_info: None,
        }
    }

    #[test]
    fn test_inner_join_cardinality_basic() -> Result<()> {
        let join = create_join_with_equi_conditions(vec![(1, 2)]);

        let mut left_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                1,
                create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, false),
            )]),
        };

        let mut right_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                2,
                create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, false),
            )]),
        };

        let result = join.inner_join_cardinality(
            1000.0, // left_cardinality
            2000.0, // right_cardinality
            &mut left_stats,
            &mut right_stats,
        )?;

        assert_eq!((result - 200.0).abs(), 20000.0 - 200.0); // 1000 * 2000 * (1/100) = 20000
        assert_eq!(left_stats.column_stats[&1].ndv, 100.0);
        assert_eq!(right_stats.column_stats[&2].ndv, 100.0);
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_zero_ndv() -> Result<()> {
        let join = create_join_with_equi_conditions(vec![(1, 2)]);

        let mut left_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                1,
                create_column_stat(Datum::Int(1), Datum::Int(100), 0.0, false),
            )]),
        };

        let mut right_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                2,
                create_column_stat(Datum::Int(1), Datum::Int(100), 0.0, false),
            )]),
        };

        let result =
            join.inner_join_cardinality(1000.0, 2000.0, &mut left_stats, &mut right_stats)?;

        let expected = 1000.0 * 2000.0 * DEFAULT_EQ_SELECTIVITY;
        assert_eq!(result, 2000000.0);
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_no_overlap() -> Result<()> {
        let join = create_join_with_equi_conditions(vec![(1, 2)]);

        let mut left_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                1,
                create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, false),
            )]),
        };

        let mut right_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                2,
                create_column_stat(Datum::Int(200), Datum::Int(300), 100.0, false),
            )]),
        };

        let result =
            join.inner_join_cardinality(1000.0, 2000.0, &mut left_stats, &mut right_stats)?;

        assert_eq!(result, 0.0);
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_with_histogram() -> Result<()> {
        let join = create_join_with_equi_conditions(vec![(1, 2)]);

        let mut left_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                1,
                create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, true),
            )]),
        };

        let mut right_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                2,
                create_column_stat(Datum::Int(50), Datum::Int(150), 100.0, true),
            )]),
        };

        let result =
            join.inner_join_cardinality(1000.0, 2000.0, &mut left_stats, &mut right_stats)?;

        assert!(result > 0.0);
        assert!(result < 1000.0 * 2000.0);

        assert_eq!(left_stats.column_stats[&1].min, Datum::Int(50));
        assert_eq!(left_stats.column_stats[&1].max, Datum::Int(100));
        assert_eq!(right_stats.column_stats[&2].min, Datum::Int(50));
        assert_eq!(right_stats.column_stats[&2].max, Datum::Int(100));
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_missing_stats() -> Result<()> {
        let join = create_join_with_equi_conditions(vec![(1, 2), (3, 4)]);

        let mut left_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                1,
                create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, false),
            )]),
        };

        let mut right_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([
                (
                    2,
                    create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, false),
                ),
                (
                    4,
                    create_column_stat(Datum::Int(1), Datum::Int(50), 50.0, false),
                ),
            ]),
        };

        let result =
            join.inner_join_cardinality(1000.0, 2000.0, &mut left_stats, &mut right_stats)?;

        let expected = 1000.0 * 2000.0 * (1.0 / 100.0) * DEFAULT_EQ_SELECTIVITY;
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_zero_cardinality() -> Result<()> {
        let join = create_join_with_equi_conditions(vec![(1, 2)]);

        let mut left_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                1,
                create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, false),
            )]),
        };

        let mut right_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                2,
                create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, false),
            )]),
        };

        let result = join.inner_join_cardinality(0.0, 0.0, &mut left_stats, &mut right_stats)?;

        assert_eq!(result, 0.0);
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_type_incompatible() -> Result<()> {
        let join = create_join_with_equi_conditions(vec![(1, 2)]);

        let mut left_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                1,
                create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, false),
            )]),
        };

        let mut right_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                2,
                create_column_stat(
                    Datum::Bytes("a".as_bytes().to_vec()),
                    Datum::Bytes("z".as_bytes().to_vec()),
                    26.0,
                    false,
                ),
            )]),
        };

        let result =
            join.inner_join_cardinality(1000.0, 2000.0, &mut left_stats, &mut right_stats)?;

        assert!((result - (1000.0 * 2000.0)).abs() < f64::EPSILON);
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_multiple_conditions() -> Result<()> {
        let join = create_join_with_equi_conditions(vec![(1, 2), (3, 4)]);

        let mut left_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([
                (
                    1,
                    create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, false),
                ),
                (
                    3,
                    create_column_stat(Datum::Int(1), Datum::Int(50), 50.0, false),
                ),
            ]),
        };

        let mut right_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([
                (
                    2,
                    create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, false),
                ),
                (
                    4,
                    create_column_stat(Datum::Int(1), Datum::Int(50), 50.0, false),
                ),
            ]),
        };

        let result =
            join.inner_join_cardinality(1000.0, 2000.0, &mut left_stats, &mut right_stats)?;

        let expected = 1000.0 * 2000.0 * (1.0 / 100.0) * (1.0 / 50.0);
        assert!((result - expected).abs() < f64::EPSILON);
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_stats_update() -> Result<()> {
        let join = create_join_with_equi_conditions(vec![(1, 2)]);

        let mut left_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                1,
                create_column_stat(Datum::Int(1), Datum::Int(100), 100.0, true),
            )]),
        };

        let mut right_stats = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(
                2,
                create_column_stat(Datum::Int(50), Datum::Int(150), 100.0, true),
            )]),
        };

        let _ = join.inner_join_cardinality(1000.0, 2000.0, &mut left_stats, &mut right_stats)?;

        assert_eq!(left_stats.column_stats[&1].min, Datum::Int(50));
        assert_eq!(left_stats.column_stats[&1].max, Datum::Int(100));
        assert_eq!(right_stats.column_stats[&2].min, Datum::Int(50));
        assert_eq!(right_stats.column_stats[&2].max, Datum::Int(100));
        Ok(())
    }
}
