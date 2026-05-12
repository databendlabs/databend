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
use databend_common_expression::conversion::ConversionClass;
use databend_common_expression::conversion::common_super_type_with_conversion;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::stat_distribution::StatEstimate;
use databend_common_expression::types::DataType;
use databend_common_statistics::Datum;
use databend_common_statistics::DatumKind;
use databend_common_statistics::Histogram;

use crate::ColumnSet;
use crate::Symbol;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::Side;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::optimizer::ir::UniformSampleSet;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Cross,
    Inner,
    InnerAny,
    Left,
    LeftAny,
    Right,
    RightAny,
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
            JoinType::LeftAny => JoinType::RightAny,
            JoinType::Right => JoinType::Left,
            JoinType::RightAny => JoinType::LeftAny,
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
            _ => *self,
        }
    }

    pub fn is_outer_join(&self) -> bool {
        matches!(
            self,
            JoinType::Left
                | JoinType::LeftAny
                | JoinType::Right
                | JoinType::RightAny
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

    pub fn is_any_join(&self) -> bool {
        matches!(
            self,
            JoinType::InnerAny | JoinType::LeftAny | JoinType::RightAny
        )
    }

    pub fn is_asof_join(&self) -> bool {
        matches!(
            self,
            JoinType::Asof | JoinType::LeftAsof | JoinType::RightAsof
        )
    }

    /// Joins that behave like filters (no null preserving side) so
    /// equi-join conditions can be deduplicated safely.
    pub fn is_filtering_join(&self) -> bool {
        matches!(
            self,
            JoinType::Inner
                | JoinType::InnerAny
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
        )
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            JoinType::Inner => {
                write!(f, "INNER")
            }
            JoinType::InnerAny => {
                write!(f, "INNER ANY")
            }
            JoinType::Left => {
                write!(f, "LEFT OUTER")
            }
            JoinType::LeftAny => {
                write!(f, "LEFT ANY")
            }
            JoinType::Right => {
                write!(f, "RIGHT OUTER")
            }
            JoinType::RightAny => {
                write!(f, "RIGHT ANY")
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
    pub columns: Vec<Symbol>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JoinEquiCondition {
    pub left: ScalarExpr,
    pub right: ScalarExpr,
    // Used for "is (not) distinct from" and mark join
    pub is_null_equal: bool,
}

impl Side {
    pub fn join_condition(self, cond: &JoinEquiCondition) -> &ScalarExpr {
        match self {
            Side::Left => &cond.left,
            Side::Right => &cond.right,
        }
    }
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
            .map(|(index, (left, right))| Self {
                left,
                right,
                is_null_equal: is_null_equal.contains(&index),
            })
            .collect()
    }

    fn single_columns(&self) -> Option<JoinConditionColumns> {
        Some(JoinConditionColumns {
            left: single_used_column(&self.left)?,
            right: single_used_column(&self.right)?,
        })
    }
}

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
    pub marker_index: Option<Symbol>,
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
        let mut estimator = JoinStatsEstimator {
            left_cardinality,
            right_cardinality,
            join_card: left_cardinality * right_cardinality,
            updated_columns: None,
            drop_null_join_keys: matches!(
                self.join_type,
                JoinType::Inner
                    | JoinType::InnerAny
                    | JoinType::Asof
                    | JoinType::LeftSemi
                    | JoinType::RightSemi
            ),
        };
        estimator.estimate(&self.equi_conditions, left_statistics, right_statistics)?;
        let Some(columns) = estimator.updated_columns else {
            return Ok(estimator.join_card);
        };

        NewStatistic::finish_join_histograms(left_statistics, columns.left)?;
        NewStatistic::finish_join_histograms(right_statistics, columns.right)?;
        Ok(estimator.join_card)
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
            JoinType::Inner | JoinType::InnerAny | JoinType::Asof | JoinType::Cross => {
                inner_join_cardinality
            }
            JoinType::Left | JoinType::LeftAny | JoinType::LeftAsof => {
                f64::max(left_cardinality, inner_join_cardinality)
            }
            JoinType::Right | JoinType::RightAny | JoinType::RightAsof => {
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
            left_statistics
                .column_stats
                .into_iter()
                .chain(right_statistics.column_stats)
                .map(|(col, mut stat)| {
                    stat.ndv = stat.ndv.reduce(cardinality);
                    stat.null_count = stat.null_count.reduce(cardinality);
                    (col, stat)
                })
                .collect()
        };
        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
            },
        }))
    }

    pub fn replace_column(&mut self, old: Symbol, new: Symbol) -> Result<()> {
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
            (Distribution::NodeToNodeHash(_), Distribution::NodeToNodeHash(_)) => {
                Ok(PhysicalProperty {
                    distribution: probe_prop.distribution.clone(),
                })
            }

            (Distribution::GlobalHash(_), Distribution::GlobalHash(_)) => Ok(PhysicalProperty {
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
                | JoinType::InnerAny
                | JoinType::LeftAny
                | JoinType::RightAny
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
            required.distribution = Distribution::GlobalHash(left_conditions);
        } else {
            let right_conditions = self
                .equi_conditions
                .iter()
                .map(|condition| condition.right.clone())
                .collect();
            required.distribution = Distribution::GlobalHash(right_conditions);
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
                        distribution: Distribution::GlobalHash(conditions),
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
                        distribution: Distribution::GlobalHash(conditions),
                    },
                    RequiredProperty {
                        distribution: Distribution::Broadcast,
                    },
                ]);
            }
            return Ok(children_required);
        }

        let settings = ctx.get_settings();
        if !matches!(self.join_type, JoinType::Cross) && !settings.get_enforce_broadcast_join()? {
            // (Hash, Hash) – use full equi-join key set to avoid single-column hash shuffle
            let left_keys: Vec<_> = self
                .equi_conditions
                .iter()
                .map(|condition| condition.left.clone())
                .collect();
            let right_keys: Vec<_> = self
                .equi_conditions
                .iter()
                .map(|condition| condition.right.clone())
                .collect();

            if !left_keys.is_empty() {
                children_required.push(vec![
                    RequiredProperty {
                        distribution: Distribution::GlobalHash(left_keys),
                    },
                    RequiredProperty {
                        distribution: Distribution::GlobalHash(right_keys),
                    },
                ]);
            }
        }

        if !matches!(
            self.join_type,
            JoinType::Right
                | JoinType::Full
                | JoinType::RightAnti
                | JoinType::RightSemi
                | JoinType::LeftMark
                | JoinType::RightSingle
                | JoinType::InnerAny
                | JoinType::LeftAny
                | JoinType::RightAny
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

fn single_used_column(expr: &ScalarExpr) -> Option<Symbol> {
    match expr {
        ScalarExpr::BoundColumnRef(column) => Some(column.column.index),
        ScalarExpr::CastExpr(cast) if !cast.is_try => match cast.argument.as_ref() {
            ScalarExpr::BoundColumnRef(column) => Some(column.column.index),
            _ => None,
        },
        _ => None,
    }
}

struct JoinStatsEstimator {
    left_cardinality: f64,
    right_cardinality: f64,
    join_card: f64,
    updated_columns: Option<JoinConditionColumns>,
    drop_null_join_keys: bool,
}

impl JoinStatsEstimator {
    fn estimate(
        &mut self,
        conditions: &[JoinEquiCondition],
        left_statistics: &mut Statistics,
        right_statistics: &mut Statistics,
    ) -> Result<()> {
        for condition in conditions {
            if self.join_card == 0.0 {
                break;
            }
            self.apply_condition(condition, left_statistics, right_statistics)?;
        }
        Ok(())
    }

    fn apply_condition(
        &mut self,
        condition: &JoinEquiCondition,
        left_statistics: &mut Statistics,
        right_statistics: &mut Statistics,
    ) -> Result<()> {
        let Some(columns) = condition.single_columns() else {
            return Ok(());
        };
        let (left_cardinality, right_cardinality) = if !condition.is_null_equal {
            let left_null_count = left_statistics
                .column_stats
                .get(&columns.left)
                .map(|stat| join_key_null_count_for_cardinality(stat, self.left_cardinality))
                .unwrap_or(0.0);
            let right_null_count = right_statistics
                .column_stats
                .get(&columns.right)
                .map(|stat| join_key_null_count_for_cardinality(stat, self.right_cardinality))
                .unwrap_or(0.0);
            (
                (self.left_cardinality - left_null_count).max(0.0),
                (self.right_cardinality - right_null_count).max(0.0),
            )
        } else {
            (self.left_cardinality, self.right_cardinality)
        };
        if self.drop_null_join_keys && !condition.is_null_equal {
            if let Some(stat) = left_statistics.column_stats.get_mut(&columns.left) {
                stat.null_count = StatCount::exact(0);
            }
            if let Some(stat) = right_statistics.column_stats.get_mut(&columns.right) {
                stat.null_count = StatCount::exact(0);
            }
        }

        let condition_stats = match try {
            JoinConditionEstimation {
                condition,
                left_col_stat: left_statistics.column_stats.get(&columns.left)?,
                right_col_stat: right_statistics.column_stats.get(&columns.right)?,
                left_cardinality,
                right_cardinality,
            }
        } {
            Some(estimation) => estimation.estimate()?,
            None => return Ok(()),
        };

        match condition_stats {
            JoinConditionStats::Skip => {}
            JoinConditionStats::NoOverlap => {
                self.join_card = 0.0;
            }
            JoinConditionStats::Estimated { new_stat, card } => {
                let left_stat = left_statistics.column_stats.get_mut(&columns.left).unwrap();
                let right_stat = right_statistics
                    .column_stats
                    .get_mut(&columns.right)
                    .unwrap();
                new_stat.apply(left_stat, right_stat);

                if card < self.join_card {
                    self.join_card = card;
                    self.updated_columns = Some(columns);
                }
            }
        };
        Ok(())
    }
}

fn join_key_null_count_for_cardinality(stat: &ColumnStat, cardinality: f64) -> f64 {
    // Keep at least known NDV rows for non-null values; derived filters may
    // leave a stale null_count that would otherwise be subtracted again.
    let max_null_count = (cardinality - stat.ndv.lower).max(0.0);
    stat.null_count.expected().min(max_null_count)
}

#[derive(Clone, Copy)]
struct JoinConditionColumns {
    left: Symbol,
    right: Symbol,
}

struct JoinConditionEstimation<'a> {
    condition: &'a JoinEquiCondition,
    left_col_stat: &'a ColumnStat,
    right_col_stat: &'a ColumnStat,
    left_cardinality: f64,
    right_cardinality: f64,
}

impl<'a> JoinConditionEstimation<'a> {
    fn estimate(&self) -> Result<JoinConditionStats> {
        let left_input = JoinColumnInput::from_column_stat(self.left_col_stat);
        let right_input = JoinColumnInput::from_column_stat(self.right_col_stat);
        if left_input
            .interval()
            .has_same_supported_type(&right_input.interval())
        {
            let Some(estimation) = JoinEstimate::from_inputs(
                &left_input,
                &right_input,
                self.left_cardinality,
                self.right_cardinality,
            )?
            else {
                return Ok(JoinConditionStats::NoOverlap);
            };
            return Ok(JoinConditionStats::Estimated {
                new_stat: NewStatistic::same_type(estimation.min, estimation.max, estimation.ndv),
                card: estimation.card,
            });
        }

        let Some(kind) = mixed_numeric_stat_kind(
            self.left_col_stat,
            self.right_col_stat,
            &self.condition.left.data_type()?,
            &self.condition.right.data_type()?,
        )?
        else {
            return Ok(JoinConditionStats::Skip);
        };
        let Some(left_input) = left_input.normalize_to_kind(kind) else {
            return Ok(JoinConditionStats::Skip);
        };
        let Some(right_input) = right_input.normalize_to_kind(kind) else {
            return Ok(JoinConditionStats::Skip);
        };
        let Some(estimation) = JoinEstimate::from_inputs(
            &left_input,
            &right_input,
            self.left_cardinality,
            self.right_cardinality,
        )?
        else {
            return Ok(JoinConditionStats::NoOverlap);
        };

        Ok(JoinConditionStats::Estimated {
            new_stat: NewStatistic::mixed_type(
                estimation.min,
                estimation.max,
                self.left_col_stat,
                self.right_col_stat,
                estimation.ndv,
            ),
            card: estimation.card,
        })
    }
}

enum JoinConditionStats {
    Skip,
    NoOverlap,
    Estimated { new_stat: NewStatistic, card: f64 },
}

struct JoinColumnInput<'a> {
    min: Datum,
    max: Datum,
    ndv: StatEstimate,
    histogram: Option<&'a Histogram>,
}

impl<'a> JoinColumnInput<'a> {
    fn from_column_stat(stat: &'a ColumnStat) -> Self {
        Self {
            min: stat.min.clone(),
            max: stat.max.clone(),
            ndv: stat.ndv,
            histogram: stat.histogram.as_ref(),
        }
    }

    fn normalize_to_kind(&self, kind: DatumKind) -> Option<Self> {
        let min = self.min.normalize_to_kind(kind)?;
        let max = self.max.normalize_to_kind(kind)?;
        if min.compare(&max).ok()? == std::cmp::Ordering::Greater {
            return None;
        }

        Some(Self {
            min,
            max,
            ndv: self.ndv,
            histogram: self.histogram,
        })
    }

    fn interval(&self) -> UniformSampleSet {
        UniformSampleSet::new(self.min.clone(), self.max.clone())
    }
}

struct JoinEstimate {
    min: Option<Datum>,
    max: Option<Datum>,
    card: f64,
    ndv: Option<StatEstimate>,
}

impl JoinEstimate {
    fn from_inputs(
        left: &JoinColumnInput,
        right: &JoinColumnInput,
        left_cardinality: f64,
        right_cardinality: f64,
    ) -> Result<Option<Self>> {
        let left_interval = left.interval();
        let right_interval = right.interval();
        if !left_interval.has_intersection(&right_interval)? {
            return Ok(None);
        }

        if left.min.is_numeric()
            && let (Some(left_hist), Some(right_hist)) = (left.histogram, right.histogram)
            && let Some(estimation) = left_hist.estimate_join_numeric_compatible(right_hist)?
        {
            let (min, max) = left_interval.intersection(&right_interval)?;
            return Ok(Some(Self {
                min,
                max,
                card: estimation.cardinality.expected,
                ndv: Some(estimation.ndv),
            }));
        }

        let (max_ndv, ndv) = {
            let left = left.ndv;
            let right = right.ndv;
            fn is_missing_ndv(ndv: StatEstimate) -> bool {
                ndv.lower == 0.0 && ndv.expected == ndv.upper && ndv.upper > 0.0
            }
            match (is_missing_ndv(left), is_missing_ndv(right)) {
                (false, false) => (left.expected.max(right.expected), Some(left.min(right))),
                (false, true) => (left.expected, Some(left.min(right))),
                (true, false) => (right.expected, Some(left.min(right))),
                (true, true) => {
                    if left.upper == 0.0 && right.upper == 0.0 {
                        (0.0, Some(StatEstimate::exact(0.0)))
                    } else {
                        (left_cardinality * right_cardinality, None)
                    }
                }
            }
        };

        let card = if max_ndv == 0.0 {
            0.0
        } else {
            left_cardinality * right_cardinality / max_ndv
        };
        let (min, max) = left_interval.intersection(&right_interval)?;
        Ok(Some(Self {
            min,
            max,
            card,
            ndv,
        }))
    }
}

fn mixed_numeric_stat_kind(
    left_stat: &ColumnStat,
    right_stat: &ColumnStat,
    left_type: &DataType,
    right_type: &DataType,
) -> Result<Option<DatumKind>> {
    let values = [
        &left_stat.min,
        &left_stat.max,
        &right_stat.min,
        &right_stat.max,
    ];
    if !values.iter().all(|value| value.is_numeric()) {
        return Ok(None);
    }

    let Some(conversion) = common_super_type_with_conversion(left_type, right_type) else {
        return Ok(None);
    };
    if !matches!(
        conversion.common_type.remove_nullable(),
        DataType::Number(_) | DataType::Decimal(_)
    ) {
        return Ok(None);
    }
    if matches!(
        (conversion.left, conversion.right),
        (
            ConversionClass::ValueDependent | ConversionClass::TryOnly,
            _
        ) | (
            _,
            ConversionClass::ValueDependent | ConversionClass::TryOnly
        )
    ) {
        return Ok(None);
    }

    if values.iter().any(|value| matches!(value, Datum::Float(_))) {
        return Ok(Some(DatumKind::Float));
    }
    if values.iter().all(|value| value.as_i64().is_some()) {
        return Ok(Some(DatumKind::Int));
    }
    if values.iter().all(|value| value.as_u64().is_some()) {
        return Ok(Some(DatumKind::UInt));
    }
    Ok(Some(DatumKind::Float))
}

#[derive(Debug, Clone)]
struct NewStatistic {
    left_min: Option<Datum>,
    left_max: Option<Datum>,
    right_min: Option<Datum>,
    right_max: Option<Datum>,
    ndv: Option<StatEstimate>,
}

impl NewStatistic {
    fn same_type(min: Option<Datum>, max: Option<Datum>, ndv: Option<StatEstimate>) -> Self {
        Self {
            left_min: min.clone(),
            left_max: max.clone(),
            right_min: min,
            right_max: max,
            ndv,
        }
    }

    fn mixed_type(
        min: Option<Datum>,
        max: Option<Datum>,
        left_stat: &ColumnStat,
        right_stat: &ColumnStat,
        ndv: Option<StatEstimate>,
    ) -> Self {
        let (left_min, left_max) =
            Self::normalize_bounds_to_stat_type(min.as_ref(), max.as_ref(), left_stat);
        let (right_min, right_max) =
            Self::normalize_bounds_to_stat_type(min.as_ref(), max.as_ref(), right_stat);
        Self {
            left_min,
            left_max,
            right_min,
            right_max,
            ndv,
        }
    }

    fn normalize_bounds_to_stat_type(
        min: Option<&Datum>,
        max: Option<&Datum>,
        stat: &ColumnStat,
    ) -> (Option<Datum>, Option<Datum>) {
        let Some(kind) = stat.min.kind() else {
            return (None, None);
        };
        let min = min.and_then(|value| value.lower_bound_to_kind(kind));
        let max = max.and_then(|value| value.upper_bound_to_kind(kind));
        if let (Some(min), Some(max)) = (&min, &max)
            && min
                .compare(max)
                .is_ok_and(|ordering| ordering == std::cmp::Ordering::Greater)
        {
            return (None, None);
        }
        (min, max)
    }

    fn apply(self, left_stat: &mut ColumnStat, right_stat: &mut ColumnStat) {
        if let Some(new_min) = self.left_min {
            left_stat.min = new_min;
        }
        if let Some(new_max) = self.left_max {
            left_stat.max = new_max;
        }
        if let Some(new_min) = self.right_min {
            right_stat.min = new_min;
        }
        if let Some(new_max) = self.right_max {
            right_stat.max = new_max;
        }
        if let Some(new_ndv) = self.ndv {
            left_stat.ndv = new_ndv;
            right_stat.ndv = new_ndv;
        }
    }

    fn finish_join_histograms(statistics: &mut Statistics, joined_column: Symbol) -> Result<()> {
        for (idx, stat) in statistics.column_stats.iter_mut() {
            stat.histogram = if let Some(histogram) = &stat.histogram
                && *idx == joined_column
                && stat.ndv.expected as u64 > 2
            {
                histogram.restrict_to_bounds(&stat.min, &stat.max)?
            } else {
                // Other columns' histograms are inaccurate after the join cardinality update.
                None
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_statistics::F64;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::plans::BoundColumnRef;
    use crate::plans::CastExpr;
    use crate::plans::ConstantExpr;
    use crate::plans::FunctionCall;

    fn column(index: usize, data_type: DataType) -> ScalarExpr {
        ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::new(index),
                Box::new(data_type),
                Visibility::Visible,
            )
            .build(),
        })
    }

    fn cast(expr: ScalarExpr, target_type: DataType) -> ScalarExpr {
        ScalarExpr::CastExpr(CastExpr {
            span: None,
            is_try: false,
            argument: Box::new(expr),
            target_type: Box::new(target_type),
        })
    }

    fn function_call(func_name: &str, arguments: Vec<ScalarExpr>) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: func_name.to_string(),
            params: vec![],
            arguments,
        })
    }

    #[test]
    fn test_mixed_type_stat_update_uses_original_stat_types() {
        let left_stat = ColumnStat {
            min: Datum::Int(0),
            max: Datum::Int(100),
            ndv: StatEstimate::exact(100.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };
        let right_stat = ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(10),
            ndv: StatEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };

        let stat = NewStatistic::mixed_type(
            Some(Datum::Int(1)),
            Some(Datum::Int(2)),
            &left_stat,
            &right_stat,
            Some(StatEstimate::exact(2.0)),
        );

        assert_eq!(stat.left_min, Some(Datum::Int(1)));
        assert_eq!(stat.left_max, Some(Datum::Int(2)));
        assert_eq!(stat.right_min, Some(Datum::UInt(1)));
        assert_eq!(stat.right_max, Some(Datum::UInt(2)));
    }

    #[test]
    fn test_mixed_type_stat_update_rounds_float_bounds_for_integer_stats() {
        let int_stat = ColumnStat {
            min: Datum::Int(0),
            max: Datum::Int(100),
            ndv: StatEstimate::exact(100.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };
        let float_stat = ColumnStat {
            min: Datum::Float(F64::from(1.2)),
            max: Datum::Float(F64::from(8.8)),
            ndv: StatEstimate::exact(8.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };

        let stat = NewStatistic::mixed_type(
            Some(Datum::Float(F64::from(1.2))),
            Some(Datum::Float(F64::from(8.8))),
            &int_stat,
            &float_stat,
            Some(StatEstimate::exact(8.0)),
        );

        assert_eq!(stat.left_min, Some(Datum::Int(2)));
        assert_eq!(stat.left_max, Some(Datum::Int(8)));
        assert_eq!(stat.right_min, Some(Datum::Float(F64::from(1.2))));
        assert_eq!(stat.right_max, Some(Datum::Float(F64::from(8.8))));
    }

    #[test]
    fn test_mixed_numeric_stats_normalize_to_smaller_common_stat_kind() -> Result<()> {
        let left_stat = ColumnStat {
            min: Datum::Int(0),
            max: Datum::Int(100),
            ndv: StatEstimate::exact(100.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };
        let right_stat = ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(10),
            ndv: StatEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };

        let kind = mixed_numeric_stat_kind(
            &left_stat,
            &right_stat,
            &DataType::Number(NumberDataType::Int32),
            &DataType::Number(NumberDataType::UInt8),
        )?
        .expect("mixed numeric stats should be normalized");
        let left = JoinColumnInput::from_column_stat(&left_stat)
            .normalize_to_kind(kind)
            .expect("left stats should normalize");
        let right = JoinColumnInput::from_column_stat(&right_stat)
            .normalize_to_kind(kind)
            .expect("right stats should normalize");

        assert_eq!(left.min, Datum::Int(0));
        assert_eq!(left.max, Datum::Int(100));
        assert_eq!(right.min, Datum::Int(0));
        assert_eq!(right.max, Datum::Int(10));
        Ok(())
    }

    #[test]
    fn test_inner_join_stat_clears_non_null_safe_join_key_null_count() -> Result<()> {
        let mut left_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(0), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(3),
                ndv: StatEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(2),
                ndv: StatEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut estimator = JoinStatsEstimator {
            left_cardinality: 4.0,
            right_cardinality: 3.0,
            join_card: 12.0,
            updated_columns: None,
            drop_null_join_keys: true,
        };
        let condition = JoinEquiCondition::new(
            column(0, DataType::Number(NumberDataType::Int32)),
            column(1, DataType::Number(NumberDataType::Int32)),
            false,
        );

        estimator.estimate(&[condition], &mut left_statistics, &mut right_statistics)?;

        assert_eq!(estimator.join_card, 2.0);
        assert_eq!(
            left_statistics.column_stats[&Symbol::new(0)].null_count,
            StatCount::exact(0)
        );
        assert_eq!(
            right_statistics.column_stats[&Symbol::new(1)].null_count,
            StatCount::exact(0)
        );
        Ok(())
    }

    #[test]
    fn test_inner_join_stat_ignores_derived_join_key_null_count() -> Result<()> {
        let mut left_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(0), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(3),
                ndv: StatEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(2),
                ndv: StatEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut estimator = JoinStatsEstimator {
            left_cardinality: 3.0,
            right_cardinality: 2.3333333333333335,
            join_card: 7.0,
            updated_columns: None,
            drop_null_join_keys: true,
        };
        let condition = JoinEquiCondition::new(
            column(0, DataType::Number(NumberDataType::Int32)),
            column(1, DataType::Number(NumberDataType::Int32)),
            false,
        );

        estimator.estimate(&[condition], &mut left_statistics, &mut right_statistics)?;

        assert!((estimator.join_card - 2.3333333333333335).abs() < 1e-9);
        assert_eq!(
            left_statistics.column_stats[&Symbol::new(0)].null_count,
            StatCount::exact(0)
        );
        assert_eq!(
            right_statistics.column_stats[&Symbol::new(1)].null_count,
            StatCount::exact(0)
        );
        Ok(())
    }

    #[test]
    fn test_inner_join_stat_clears_join_key_null_count_when_estimation_skips() -> Result<()> {
        let mut left_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(0), ColumnStat {
                min: Datum::Bool(false),
                max: Datum::Bool(true),
                ndv: StatEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Bool(false),
                max: Datum::Bool(true),
                ndv: StatEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut estimator = JoinStatsEstimator {
            left_cardinality: 3.0,
            right_cardinality: 3.0,
            join_card: 9.0,
            updated_columns: None,
            drop_null_join_keys: true,
        };
        let condition = JoinEquiCondition::new(
            column(0, DataType::Boolean),
            column(1, DataType::Boolean),
            false,
        );

        estimator.estimate(&[condition], &mut left_statistics, &mut right_statistics)?;

        assert_eq!(estimator.join_card, 9.0);
        assert_eq!(
            left_statistics.column_stats[&Symbol::new(0)].null_count,
            StatCount::exact(0)
        );
        assert_eq!(
            right_statistics.column_stats[&Symbol::new(1)].null_count,
            StatCount::exact(0)
        );
        Ok(())
    }

    #[test]
    fn test_inner_join_stat_preserves_null_safe_join_key_null_count() -> Result<()> {
        let mut left_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(0), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(3),
                ndv: StatEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(2),
                ndv: StatEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut estimator = JoinStatsEstimator {
            left_cardinality: 4.0,
            right_cardinality: 3.0,
            join_card: 12.0,
            updated_columns: None,
            drop_null_join_keys: true,
        };
        let condition = JoinEquiCondition::new(
            column(0, DataType::Number(NumberDataType::Int32)),
            column(1, DataType::Number(NumberDataType::Int32)),
            true,
        );

        estimator.estimate(&[condition], &mut left_statistics, &mut right_statistics)?;

        assert_eq!(
            left_statistics.column_stats[&Symbol::new(0)].null_count,
            StatCount::exact(1)
        );
        assert_eq!(
            right_statistics.column_stats[&Symbol::new(1)].null_count,
            StatCount::exact(1)
        );
        Ok(())
    }

    #[test]
    fn test_left_join_stat_excludes_join_key_nulls_from_inner_cardinality() -> Result<()> {
        let left_stat_info = Arc::new(StatInfo {
            cardinality: 4.0,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: HashMap::from([(Symbol::new(0), ColumnStat {
                    min: Datum::Int(42),
                    max: Datum::Int(44),
                    ndv: StatEstimate::exact(2.0),
                    null_count: StatCount::exact(1),
                    histogram: None,
                })]),
            },
        });
        let right_stat_info = Arc::new(StatInfo {
            cardinality: 4.0,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                    min: Datum::Int(42),
                    max: Datum::Int(45),
                    ndv: StatEstimate::exact(3.0),
                    null_count: StatCount::exact(1),
                    histogram: None,
                })]),
            },
        });
        let join = Join {
            equi_conditions: vec![JoinEquiCondition::new(
                column(0, DataType::Number(NumberDataType::Int32)),
                column(1, DataType::Number(NumberDataType::Int32)),
                false,
            )],
            join_type: JoinType::Left,
            ..Default::default()
        };

        let stat_info = join.derive_join_stats(left_stat_info, right_stat_info)?;

        assert_eq!(stat_info.cardinality, 4.0);
        assert_eq!(
            stat_info.statistics.column_stats[&Symbol::new(0)].null_count,
            StatCount::estimate(1.0, 1.0)
        );
        Ok(())
    }

    #[test]
    fn test_inner_join_stat_accepts_non_try_cast_join_key() -> Result<()> {
        let mut left_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(0), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(3),
                ndv: StatEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(2),
                ndv: StatEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut estimator = JoinStatsEstimator {
            left_cardinality: 4.0,
            right_cardinality: 3.0,
            join_card: 12.0,
            updated_columns: None,
            drop_null_join_keys: true,
        };
        let condition = JoinEquiCondition::new(
            cast(
                column(0, DataType::Number(NumberDataType::Int32)),
                DataType::Number(NumberDataType::Int64),
            ),
            column(1, DataType::Number(NumberDataType::Int32)),
            false,
        );

        estimator.estimate(&[condition], &mut left_statistics, &mut right_statistics)?;

        assert_eq!(
            left_statistics.column_stats[&Symbol::new(0)].null_count,
            StatCount::exact(0)
        );
        assert_eq!(
            right_statistics.column_stats[&Symbol::new(1)].null_count,
            StatCount::exact(0)
        );
        assert_eq!(estimator.join_card, 2.0);
        Ok(())
    }

    #[test]
    fn test_inner_join_stat_skips_function_join_key() -> Result<()> {
        let mut left_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(0), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(3),
                ndv: StatEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(2),
                ndv: StatEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
        };
        let mut estimator = JoinStatsEstimator {
            left_cardinality: 4.0,
            right_cardinality: 3.0,
            join_card: 12.0,
            updated_columns: None,
            drop_null_join_keys: true,
        };
        let condition = JoinEquiCondition::new(
            function_call("coalesce", vec![
                column(0, DataType::Number(NumberDataType::Int32)),
                ScalarExpr::ConstantExpr(ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int32(0)),
                }),
            ]),
            column(1, DataType::Number(NumberDataType::Int32)),
            false,
        );

        estimator.estimate(&[condition], &mut left_statistics, &mut right_statistics)?;

        assert_eq!(estimator.join_card, 12.0);
        assert!(estimator.updated_columns.is_none());
        assert_eq!(
            left_statistics.column_stats[&Symbol::new(0)].null_count,
            StatCount::exact(1)
        );
        assert_eq!(
            right_statistics.column_stats[&Symbol::new(1)].null_count,
            StatCount::exact(1)
        );
        Ok(())
    }
}
