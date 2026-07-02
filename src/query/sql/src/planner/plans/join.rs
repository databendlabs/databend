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

use crate::ColumnSet;
use crate::Symbol;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::JoinConditionColumns;
use crate::optimizer::ir::JoinKeyStatUpdate;
use crate::optimizer::ir::JoinStatsEstimator;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::Side;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::SpatialJoinCandidate;
use crate::plans::has_spatial_join_preconditions;
use crate::plans::is_spatial_join_shape;
use crate::plans::spatial_join_gate;

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
    FullAsof,
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
                | JoinType::FullAsof
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
            JoinType::Asof | JoinType::LeftAsof | JoinType::RightAsof | JoinType::FullAsof
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
            JoinType::FullAsof => {
                write!(f, "FULL ASOF")
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
    // Derived annotation. The canonical join condition remains
    // `non_equi_conditions`; this is finalized after logical rewrites.
    pub spatial_join: Option<Box<SpatialJoinCandidate>>,
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
            spatial_join: None,
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

    fn estimate_inner_join_cardinality(
        &self,
        left_cardinality: f64,
        right_cardinality: f64,
        left_statistics: &mut Statistics,
        right_statistics: &mut Statistics,
    ) -> Result<JoinStatsEstimator> {
        let mut estimator = JoinStatsEstimator::new(
            left_cardinality,
            right_cardinality,
            matches!(
                self.join_type,
                JoinType::Inner
                    | JoinType::InnerAny
                    | JoinType::Asof
                    | JoinType::LeftSemi
                    | JoinType::RightSemi
            ),
        );
        for condition in &self.equi_conditions {
            if estimator.join_card() == 0.0 {
                break;
            }
            let Some(columns) = condition.single_columns() else {
                continue;
            };
            estimator.apply_condition(
                columns,
                condition.left.data_type()?,
                condition.right.data_type()?,
                condition.is_null_equal,
                left_statistics,
                right_statistics,
            )?;
        }
        Ok(estimator)
    }

    fn join_cardinality(
        &self,
        left_cardinality: f64,
        right_cardinality: f64,
        inner_join_cardinality: f64,
    ) -> f64 {
        match self.join_type {
            JoinType::Inner | JoinType::InnerAny | JoinType::Asof | JoinType::Cross => {
                inner_join_cardinality
            }
            JoinType::Left | JoinType::LeftAny | JoinType::LeftAsof => {
                f64::max(left_cardinality, inner_join_cardinality)
            }
            JoinType::Right | JoinType::RightAny | JoinType::RightAsof => {
                f64::max(right_cardinality, inner_join_cardinality)
            }
            JoinType::Full | JoinType::FullAsof => {
                f64::max(left_cardinality, inner_join_cardinality)
                    + f64::max(right_cardinality, inner_join_cardinality)
                    - inner_join_cardinality
            }
            JoinType::LeftSemi => f64::min(left_cardinality, inner_join_cardinality),
            JoinType::RightSemi => f64::min(right_cardinality, inner_join_cardinality),
            JoinType::LeftSingle | JoinType::RightMark | JoinType::LeftAnti => left_cardinality,
            JoinType::RightSingle | JoinType::LeftMark | JoinType::RightAnti => right_cardinality,
        }
    }

    fn use_estimated_join_key_stats(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Inner
                | JoinType::InnerAny
                | JoinType::Asof
                | JoinType::LeftSemi
                | JoinType::RightSemi
        )
    }

    fn rebuild_join_key_histograms(&self) -> bool {
        matches!(self.join_type, JoinType::Inner)
    }

    fn keep_nullable_side_join_key_stats(
        &self,
        estimated_left_statistics: &Statistics,
        estimated_right_statistics: &Statistics,
        left_statistics: &mut Statistics,
        right_statistics: &mut Statistics,
    ) {
        match self.join_type {
            JoinType::Left | JoinType::LeftAny | JoinType::LeftAsof | JoinType::LeftSingle => {
                for columns in self
                    .equi_conditions
                    .iter()
                    .filter_map(JoinEquiCondition::single_columns)
                {
                    if let Some(mut stat) = estimated_right_statistics
                        .column_stats
                        .get(&columns.right)
                        .cloned()
                    {
                        stat.histogram = None;
                        right_statistics.column_stats.insert(columns.right, stat);
                    }
                }
            }
            JoinType::Right | JoinType::RightAny | JoinType::RightAsof | JoinType::RightSingle => {
                for columns in self
                    .equi_conditions
                    .iter()
                    .filter_map(JoinEquiCondition::single_columns)
                {
                    if let Some(mut stat) = estimated_left_statistics
                        .column_stats
                        .get(&columns.left)
                        .cloned()
                    {
                        stat.histogram = None;
                        left_statistics.column_stats.insert(columns.left, stat);
                    }
                }
            }
            _ => {}
        }
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
        let original_left_statistics = left_statistics.clone();
        let original_right_statistics = right_statistics.clone();
        let use_estimated_join_key_stats = self.use_estimated_join_key_stats();
        let rebuild_join_key_histograms = self.rebuild_join_key_histograms();
        // Evaluating join cardinality using histograms.
        // If histogram is None, will evaluate using NDV.
        let join_estimation = if use_estimated_join_key_stats {
            self.estimate_inner_join_cardinality(
                left_cardinality,
                right_cardinality,
                &mut left_statistics,
                &mut right_statistics,
            )?
        } else {
            let mut estimate_left_statistics = left_statistics.clone();
            let mut estimate_right_statistics = right_statistics.clone();
            let join_estimation = self.estimate_inner_join_cardinality(
                left_cardinality,
                right_cardinality,
                &mut estimate_left_statistics,
                &mut estimate_right_statistics,
            )?;
            self.keep_nullable_side_join_key_stats(
                &estimate_left_statistics,
                &estimate_right_statistics,
                &mut left_statistics,
                &mut right_statistics,
            );
            join_estimation
        };
        let inner_join_cardinality = join_estimation.join_card();
        let cardinality =
            self.join_cardinality(left_cardinality, right_cardinality, inner_join_cardinality);
        if let Some(columns) = join_estimation.updated_columns() {
            match self.join_type {
                JoinType::LeftSemi => {
                    JoinKeyStatUpdate::finish_semi_join_histogram(
                        &mut left_statistics,
                        &original_left_statistics,
                        columns.left,
                        cardinality,
                    )?;
                    JoinKeyStatUpdate::finish_join_histograms(
                        &mut right_statistics,
                        columns.right,
                        false,
                    )?;
                }
                JoinType::RightSemi => {
                    JoinKeyStatUpdate::finish_join_histograms(
                        &mut left_statistics,
                        columns.left,
                        false,
                    )?;
                    JoinKeyStatUpdate::finish_semi_join_histogram(
                        &mut right_statistics,
                        &original_right_statistics,
                        columns.right,
                        cardinality,
                    )?;
                }
                _ => {
                    JoinKeyStatUpdate::finish_join_histograms(
                        &mut left_statistics,
                        columns.left,
                        rebuild_join_key_histograms,
                    )?;
                    JoinKeyStatUpdate::finish_join_histograms(
                        &mut right_statistics,
                        columns.right,
                        rebuild_join_key_histograms,
                    )?;
                }
            }
        }
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
                top_n: Default::default(),
                count_min_sketch: Default::default(),
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
        self.spatial_join = None;

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

    fn spatial_join_candidate(&self, rel_expr: &RelExpr) -> Result<Option<SpatialJoinCandidate>> {
        if !has_spatial_join_preconditions(self) {
            return Ok(None);
        }

        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;
        Ok(spatial_join_gate(
            self,
            &left_prop.output_columns,
            &right_prop.output_columns,
        ))
    }

    fn can_use_distributed_spatial_join(
        &self,
        ctx: &dyn TableContext,
        rel_expr: &RelExpr,
    ) -> Result<bool> {
        Ok(ctx.get_settings().get_enable_spatial_join()?
            && !ctx.get_cluster().is_empty()
            && self.spatial_join_candidate(rel_expr)?.is_some())
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

        let spatial_join_candidate = self.spatial_join_candidate(rel_expr)?;

        match (&probe_prop.distribution, &build_prop.distribution) {
            (Distribution::Broadcast, _) if spatial_join_candidate.is_some() => {
                Ok(PhysicalProperty {
                    distribution: build_prop.distribution.clone(),
                })
            }

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

        // Spatial join (Cascades fallback path on a concrete `SExpr`): there is
        // no cost-based enumeration here, so single-select by broadcasting the
        // smaller input. This matches the physical builder, which also prefers
        // the smaller side as the R-tree build side. If either side is already
        // Serial, fall back to a Serial spatial join.
        if self.can_use_distributed_spatial_join(ctx.as_ref(), rel_expr)? {
            if probe_physical_prop.distribution == Distribution::Serial
                || build_physical_prop.distribution == Distribution::Serial
            {
                required.distribution = Distribution::Serial;
                return Ok(required);
            }

            let left_cardinality = rel_expr.derive_cardinality_child(0)?.cardinality;
            let right_cardinality = rel_expr.derive_cardinality_child(1)?.cardinality;
            let broadcast_child = if left_cardinality <= right_cardinality {
                0
            } else {
                1
            };
            required.distribution = if child_index == broadcast_child {
                Distribution::Broadcast
            } else {
                Distribution::Any
            };
            return Ok(required);
        }

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
        let settings = ctx.get_settings();
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
                | JoinType::FullAsof
        ) {
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

        // Spatial join: enumerate the broadcast alternatives and let the cost
        // model pick the cheaper build side (broadcasting the smaller input is
        // cheaper). The physical builder reads the materialized exchange to
        // decide which side actually builds the R-tree, so we do not pick a
        // build side here, and we must not peek at child physical properties
        // (they are not available during Cascades exploration).
        //
        // Use `is_spatial_join_shape` (shape-only check) instead of the full
        // `can_use_distributed_spatial_join` because `derive_relational_prop_child`
        // on an MExpr returns the current group's properties, not the child
        // group's. Final column-side validation happens after Cascades in
        // `FinalizeSpatialJoinOptimizer` which sets `join.spatial_join`; the
        // physical builder only constructs a spatial join when that field is set.
        if ctx.get_settings().get_enable_spatial_join()?
            && !ctx.get_cluster().is_empty()
            && is_spatial_join_shape(self)
        {
            return Ok(vec![
                vec![
                    RequiredProperty {
                        distribution: Distribution::Broadcast,
                    },
                    RequiredProperty {
                        distribution: Distribution::Any,
                    },
                ],
                vec![
                    RequiredProperty {
                        distribution: Distribution::Any,
                    },
                    RequiredProperty {
                        distribution: Distribution::Broadcast,
                    },
                ],
                vec![
                    RequiredProperty {
                        distribution: Distribution::Serial,
                    },
                    RequiredProperty {
                        distribution: Distribution::Serial,
                    },
                ],
            ]);
        }

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
                | JoinType::FullAsof
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

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::stat_distribution::NdvEstimate;
    use databend_common_expression::stat_distribution::StatCount;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_statistics::Datum;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::optimizer::ir::ColumnStat;
    use crate::optimizer::ir::SExpr;
    use crate::plans::BoundColumnRef;
    use crate::plans::CastExpr;
    use crate::plans::ConstantExpr;
    use crate::plans::Exchange;
    use crate::plans::FunctionCall;
    use crate::plans::Scan;

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

    fn column_set(indices: &[usize]) -> ColumnSet {
        indices.iter().copied().map(Symbol::new).collect()
    }

    fn exchanged_scan(exchange: Exchange, columns: &[usize]) -> SExpr {
        SExpr::create_unary(
            exchange,
            SExpr::create_leaf(Scan {
                columns: column_set(columns),
                ..Default::default()
            }),
        )
    }

    fn apply_stats_condition(
        estimator: &mut JoinStatsEstimator,
        condition: &JoinEquiCondition,
        left_statistics: &mut Statistics,
        right_statistics: &mut Statistics,
    ) -> Result<()> {
        let columns = condition
            .single_columns()
            .expect("test condition should be a single-column join key");
        estimator.apply_condition(
            columns,
            condition.left.data_type()?,
            condition.right.data_type()?,
            condition.is_null_equal,
            left_statistics,
            right_statistics,
        )
    }

    #[test]
    fn test_spatial_join_left_broadcast_preserves_right_distribution() -> Result<()> {
        let right_distribution =
            Distribution::GlobalHash(vec![column(2, DataType::Number(NumberDataType::Int32))]);
        let join = Join {
            non_equi_conditions: vec![function_call("st_intersects", vec![
                column(0, DataType::Geometry),
                column(1, DataType::Geometry),
            ])],
            join_type: JoinType::Inner,
            ..Default::default()
        };
        let s_expr = SExpr::create_binary(
            join,
            exchanged_scan(Exchange::Broadcast, &[0]),
            exchanged_scan(
                Exchange::GlobalHash(vec![column(2, DataType::Number(NumberDataType::Int32))]),
                &[1, 2],
            ),
        );

        let physical_prop = RelExpr::with_s_expr(&s_expr).derive_physical_prop()?;

        assert_eq!(physical_prop.distribution, right_distribution);
        Ok(())
    }

    #[test]
    fn test_spatial_join_same_side_predicate_does_not_preserve_left_broadcast() -> Result<()> {
        let join = Join {
            non_equi_conditions: vec![function_call("st_intersects", vec![
                column(0, DataType::Geometry),
                column(1, DataType::Geometry),
            ])],
            join_type: JoinType::Inner,
            ..Default::default()
        };
        let s_expr = SExpr::create_binary(
            join,
            exchanged_scan(Exchange::Broadcast, &[0, 1]),
            exchanged_scan(
                Exchange::GlobalHash(vec![column(2, DataType::Number(NumberDataType::Int32))]),
                &[2],
            ),
        );

        let physical_prop = RelExpr::with_s_expr(&s_expr).derive_physical_prop()?;

        assert_eq!(physical_prop.distribution, Distribution::Random);
        Ok(())
    }

    #[test]
    fn test_inner_join_stat_clears_non_null_safe_join_key_null_count() -> Result<()> {
        let mut left_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(0), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(3),
                ndv: NdvEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(2),
                ndv: NdvEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut estimator = JoinStatsEstimator::new(4.0, 3.0, true);
        let condition = JoinEquiCondition::new(
            column(0, DataType::Number(NumberDataType::Int32)),
            column(1, DataType::Number(NumberDataType::Int32)),
            false,
        );

        apply_stats_condition(
            &mut estimator,
            &condition,
            &mut left_statistics,
            &mut right_statistics,
        )?;

        assert_eq!(estimator.join_card(), 2.0);
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
                ndv: NdvEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(2),
                ndv: NdvEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut estimator = JoinStatsEstimator::new(3.0, 2.3333333333333335, true);
        let condition = JoinEquiCondition::new(
            column(0, DataType::Number(NumberDataType::Int32)),
            column(1, DataType::Number(NumberDataType::Int32)),
            false,
        );

        apply_stats_condition(
            &mut estimator,
            &condition,
            &mut left_statistics,
            &mut right_statistics,
        )?;

        assert!((estimator.join_card() - 2.3333333333333335).abs() < 1e-9);
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
                ndv: NdvEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Bool(false),
                max: Datum::Bool(true),
                ndv: NdvEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut estimator = JoinStatsEstimator::new(3.0, 3.0, true);
        let condition = JoinEquiCondition::new(
            column(0, DataType::Boolean),
            column(1, DataType::Boolean),
            false,
        );

        apply_stats_condition(
            &mut estimator,
            &condition,
            &mut left_statistics,
            &mut right_statistics,
        )?;

        assert_eq!(estimator.join_card(), 9.0);
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
                ndv: NdvEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(2),
                ndv: NdvEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut estimator = JoinStatsEstimator::new(4.0, 3.0, true);
        let condition = JoinEquiCondition::new(
            column(0, DataType::Number(NumberDataType::Int32)),
            column(1, DataType::Number(NumberDataType::Int32)),
            true,
        );

        apply_stats_condition(
            &mut estimator,
            &condition,
            &mut left_statistics,
            &mut right_statistics,
        )?;

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
                    ndv: NdvEstimate::exact(2.0),
                    null_count: StatCount::exact(1),
                    histogram: None,
                })]),
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        });
        let right_stat_info = Arc::new(StatInfo {
            cardinality: 4.0,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                    min: Datum::Int(42),
                    max: Datum::Int(45),
                    ndv: NdvEstimate::exact(3.0),
                    null_count: StatCount::exact(1),
                    histogram: None,
                })]),
                top_n: Default::default(),
                count_min_sketch: Default::default(),
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
                ndv: NdvEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(2),
                ndv: NdvEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut estimator = JoinStatsEstimator::new(4.0, 3.0, true);
        let condition = JoinEquiCondition::new(
            cast(
                column(0, DataType::Number(NumberDataType::Int32)),
                DataType::Number(NumberDataType::Int64),
            ),
            column(1, DataType::Number(NumberDataType::Int32)),
            false,
        );

        apply_stats_condition(
            &mut estimator,
            &condition,
            &mut left_statistics,
            &mut right_statistics,
        )?;

        assert_eq!(
            left_statistics.column_stats[&Symbol::new(0)].null_count,
            StatCount::exact(0)
        );
        assert_eq!(
            right_statistics.column_stats[&Symbol::new(1)].null_count,
            StatCount::exact(0)
        );
        assert_eq!(estimator.join_card(), 2.0);
        Ok(())
    }

    #[test]
    fn test_inner_join_stat_skips_function_join_key() -> Result<()> {
        let left_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(0), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(3),
                ndv: NdvEstimate::exact(3.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let right_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([(Symbol::new(1), ColumnStat {
                min: Datum::Int(1),
                max: Datum::Int(2),
                ndv: NdvEstimate::exact(2.0),
                null_count: StatCount::exact(1),
                histogram: None,
            })]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let estimator = JoinStatsEstimator::new(4.0, 3.0, true);
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

        assert!(condition.single_columns().is_none());

        assert_eq!(estimator.join_card(), 12.0);
        assert!(estimator.updated_columns().is_none());
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
