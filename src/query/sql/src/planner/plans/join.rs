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

use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::conversion::classify_conversion;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::ColumnSet;
use crate::Symbol;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::JoinStatsEstimator;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::Side;
use crate::optimizer::ir::StatInfo;
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

    /// Return the equality-preserving key expressions used by both statistics and execution.
    pub fn canonical_keys(&self) -> (&ScalarExpr, &ScalarExpr) {
        if let Some(right) = unwrap_integer_to_string_cast(&self.left, &self.right) {
            return (&self.left, right);
        }
        if let Some(left) = unwrap_integer_to_string_cast(&self.right, &self.left) {
            return (left, &self.right);
        }
        (&self.left, &self.right)
    }
}

/// Remove an integer-to-string round trip when the other equality key is an integer.
///
/// Mixed string/integer equality normally uses `Decimal(38, 5)` as the hash key. That coercion is
/// needed for arbitrary strings such as `"1.2"`, but it is unnecessary when the string is produced
/// directly from another integer. Both integers must fit losslessly in their normal common numeric
/// type, so formatting and parsing the value cannot change equality.
fn unwrap_integer_to_string_cast<'a>(
    integer_expr: &ScalarExpr,
    string_expr: &'a ScalarExpr,
) -> Option<&'a ScalarExpr> {
    let ScalarExpr::CastExpr(cast) = string_expr else {
        return None;
    };
    if cast.is_try || !matches!(cast.target_type.remove_nullable(), DataType::String) {
        return None;
    }

    let integer_type = integer_expr.data_type();
    let DataType::Number(integer_type) = integer_type.remove_nullable() else {
        return None;
    };
    if !integer_type.is_integer() {
        return None;
    }

    let source_type = cast.argument.data_type();
    let DataType::Number(source_type) = source_type.remove_nullable() else {
        return None;
    };
    if !source_type.is_integer() {
        return None;
    }

    let integer_type = DataType::Number(integer_type);
    let source_type = DataType::Number(source_type);
    let common_type = common_super_type(
        integer_type.clone(),
        source_type.clone(),
        &BUILTIN_FUNCTIONS.default_cast_rules,
    );
    let Some(common_type @ DataType::Number(_)) = common_type else {
        return None;
    };
    let preserves_equality = classify_conversion(&integer_type, &common_type)
        .is_safe_for_equality_inference()
        && classify_conversion(&source_type, &common_type).is_safe_for_equality_inference();
    preserves_equality.then_some(cast.argument.as_ref())
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
        for condition in &self.equi_conditions {
            condition.left.collect_used_columns(&mut used_columns);
            condition.right.collect_used_columns(&mut used_columns);
        }
        for condition in &self.non_equi_conditions {
            condition.collect_used_columns(&mut used_columns);
        }
        Ok(used_columns)
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
        let mut estimator =
            JoinStatsEstimator::new(self.join_type, left_stat_info, right_stat_info);
        estimator.evaluate_join(self)?;
        estimator.finish()
    }

    pub fn replace_column(&mut self, old: Symbol, new: Symbol) -> Result<()> {
        self.replace_columns(|column| Ok(if column == old { new } else { column }))
    }

    pub fn replace_columns<F>(&mut self, mut replace: F) -> Result<()>
    where F: FnMut(Symbol) -> Result<Symbol> {
        for condition in &mut self.equi_conditions {
            condition.left.replace_columns(&mut replace)?;
            condition.right.replace_columns(&mut replace)?;
        }

        for condition in &mut self.non_equi_conditions {
            condition.replace_columns(&mut replace)?;
        }

        if let Some(marker_index) = &mut self.marker_index {
            *marker_index = replace(*marker_index)?;
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
        output_columns.extend(right_prop.output_columns.iter().copied());

        // Derive outer columns
        let mut outer_columns = left_prop.outer_columns.clone();
        outer_columns.extend(right_prop.outer_columns.iter().copied());

        for condition in &self.equi_conditions {
            condition.left.collect_used_columns(&mut outer_columns);
            condition.right.collect_used_columns(&mut outer_columns);
        }
        outer_columns.retain(|column| !output_columns.contains(column));

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(left_prop.used_columns.iter().copied());
        used_columns.extend(right_prop.used_columns.iter().copied());

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
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::optimizer::ir::SExpr;
    use crate::plans::BoundColumnRef;
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

    fn function_call(
        func_name: &str,
        arguments: Vec<ScalarExpr>,
        return_type: DataType,
    ) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: func_name.to_string(),
            params: vec![],
            arguments,
            return_type: Box::new(return_type),
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

    #[test]
    fn test_spatial_join_left_broadcast_preserves_right_distribution() -> Result<()> {
        let right_distribution =
            Distribution::GlobalHash(vec![column(2, DataType::Number(NumberDataType::Int32))]);
        let join = Join {
            non_equi_conditions: vec![function_call(
                "st_intersects",
                vec![column(0, DataType::Geometry), column(1, DataType::Geometry)],
                DataType::Boolean,
            )],
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
            non_equi_conditions: vec![function_call(
                "st_intersects",
                vec![column(0, DataType::Geometry), column(1, DataType::Geometry)],
                DataType::Boolean,
            )],
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
}
