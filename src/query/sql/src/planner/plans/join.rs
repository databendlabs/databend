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

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::conversion::classify_conversion;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::ColumnSet;
use crate::Symbol;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::ColumnStatSet;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::JoinConditionColumns;
use crate::optimizer::ir::JoinStats;
use crate::optimizer::ir::JoinStatsEstimator;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::Side;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::EvalScalar;
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

    fn join_column(self, columns: JoinConditionColumns) -> Symbol {
        match self {
            Side::Left => columns.left,
            Side::Right => columns.right,
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

fn direct_column(expr: &ScalarExpr) -> Option<Symbol> {
    match expr {
        ScalarExpr::BoundColumnRef(column) => Some(column.column.index),
        _ => None,
    }
}

// This whitelist is only for propagating NULL rejection to output source columns.
// It must not be used to write expression bounds, NDV, or histograms back to them.
fn null_rejected_column(expr: &ScalarExpr) -> Option<Symbol> {
    match expr {
        ScalarExpr::BoundColumnRef(column) => Some(column.column.index),
        ScalarExpr::CastExpr(cast) if !cast.is_try => {
            let source_type = cast.argument.data_type();
            if classify_conversion(source_type.as_ref(), cast.target_type.as_ref())
                .is_lossless_injective()
            {
                direct_column(&cast.argument)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn join_condition_stat<'a>(
    expr: &ScalarExpr,
    input_statistics: &'a Statistics,
    cardinality: StatCardinality,
) -> Result<Option<Cow<'a, ColumnStat>>> {
    if let Some(column) = direct_column(expr) {
        return Ok(input_statistics
            .column_stats
            .get(&column)
            .map(Cow::Borrowed));
    }

    Ok(EvalScalar::derive_item_stat(expr, input_statistics, cardinality)?.map(Cow::Owned))
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

#[derive(Clone, Copy)]
enum JoinKeyStatOutput {
    Input,
    Estimated,
    EstimatedWithoutHistograms,
}

struct JoinSideColumnStats {
    side: Side,
    join_keys: ColumnStatSet,
    non_keys: ColumnStatSet,
    input_join_stats: ColumnStatSet,
}

impl JoinSideColumnStats {
    fn split(side: Side, mut column_stats: ColumnStatSet, join_keys: &ColumnSet) -> Self {
        let join_keys = join_keys
            .iter()
            .filter_map(|column| column_stats.remove(column).map(|stat| (*column, stat)))
            .collect();
        Self {
            side,
            join_keys,
            non_keys: column_stats,
            input_join_stats: HashMap::new(),
        }
    }

    fn join_key_output(&self, join_type: JoinType) -> JoinKeyStatOutput {
        match (join_type, self.side) {
            (
                JoinType::Inner
                | JoinType::InnerAny
                | JoinType::Asof
                | JoinType::LeftSemi
                | JoinType::RightSemi,
                _,
            ) => JoinKeyStatOutput::Estimated,
            (JoinType::Left | JoinType::LeftAny | JoinType::LeftSingle, Side::Right)
            | (JoinType::Right | JoinType::RightAny | JoinType::RightSingle, Side::Left)
            | (JoinType::LeftAsof, Side::Left)
            | (JoinType::RightAsof, Side::Right) => JoinKeyStatOutput::EstimatedWithoutHistograms,
            _ => JoinKeyStatOutput::Input,
        }
    }

    fn take_join_keys_for_estimation(&mut self, join_type: JoinType) -> ColumnStatSet {
        if matches!(self.join_key_output(join_type), JoinKeyStatOutput::Input) {
            return self.join_keys.clone();
        }

        if matches!(
            (join_type, self.side),
            (JoinType::LeftSemi, Side::Left) | (JoinType::RightSemi, Side::Right)
        ) {
            self.input_join_stats = self.join_keys.clone();
        }
        std::mem::take(&mut self.join_keys)
    }

    fn set_estimated_join_keys(&mut self, join_type: JoinType, mut estimated: ColumnStatSet) {
        match self.join_key_output(join_type) {
            JoinKeyStatOutput::Input => {}
            JoinKeyStatOutput::Estimated => self.join_keys = estimated,
            JoinKeyStatOutput::EstimatedWithoutHistograms => {
                for stat in estimated.values_mut() {
                    stat.clear_histogram();
                }
                self.join_keys = estimated;
            }
        }
    }

    fn clear_null_count(&mut self, estimated_join_keys: &mut ColumnStatSet, column: Symbol) {
        let stat = estimated_join_keys
            .get_mut(&column)
            .or_else(|| self.join_keys.get_mut(&column))
            .or_else(|| self.non_keys.get_mut(&column));
        if let Some(stat) = stat {
            stat.set_null_count(StatCount::exact(0));
        }
    }

    fn propagate_non_key_stats(
        mut self,
        input_cardinality: f64,
        output_cardinality: f64,
        matched_rows: f64,
    ) -> Self {
        if input_cardinality <= 0.0 || output_cardinality <= 0.0 {
            return self;
        }

        // Selection changes NDV, while join fanout only duplicates surviving
        // values. NULL counts are row counts, so selection and fanout combine
        // into the total output/input row scale under the independence model.
        let survival_rate = (matched_rows / input_cardinality).clamp(0.0, 1.0);
        let row_scale = output_cardinality / input_cardinality;
        for stat in self.non_keys.values_mut() {
            match stat {
                ColumnStat::Boolean {
                    ndv, null_count, ..
                } => {
                    let input_non_null =
                        (input_cardinality - null_count.expected()).clamp(0.0, input_cardinality);
                    *ndv = ndv.reduce_by_selectivity(input_non_null, survival_rate);
                    *null_count = Self::scale_count(*null_count, row_scale, output_cardinality);
                }
                ColumnStat::Int {
                    ndv, null_count, ..
                }
                | ColumnStat::UInt {
                    ndv, null_count, ..
                }
                | ColumnStat::Float {
                    ndv, null_count, ..
                }
                | ColumnStat::Bytes {
                    ndv, null_count, ..
                } => {
                    let input_non_null =
                        (input_cardinality - null_count.expected()).clamp(0.0, input_cardinality);
                    *ndv = ndv.reduce_by_selectivity(input_non_null, survival_rate);
                    *null_count = Self::scale_count(*null_count, row_scale, output_cardinality);
                    stat.clear_histogram();
                }
                ColumnStat::AllNull { null_count } => {
                    *null_count = Self::scale_count(*null_count, row_scale, output_cardinality);
                }
            }
        }
        self
    }

    fn into_column_stats(self) -> ColumnStatSet {
        self.join_keys.into_iter().chain(self.non_keys).collect()
    }

    fn cap_counts(mut self, cardinality: f64) -> Self {
        for stat in self
            .join_keys
            .values_mut()
            .chain(self.non_keys.values_mut())
        {
            match stat {
                ColumnStat::Boolean {
                    ndv, null_count, ..
                }
                | ColumnStat::Int {
                    ndv, null_count, ..
                }
                | ColumnStat::UInt {
                    ndv, null_count, ..
                }
                | ColumnStat::Float {
                    ndv, null_count, ..
                }
                | ColumnStat::Bytes {
                    ndv, null_count, ..
                } => {
                    *ndv = ndv.reduce(cardinality);
                    *null_count = null_count.reduce(cardinality);
                }
                ColumnStat::AllNull { null_count } => {
                    *null_count = null_count.reduce(cardinality);
                }
            }
        }
        self
    }

    fn apply_outer_null_extension(
        mut self,
        join_type: JoinType,
        output_cardinality: f64,
        stats: &JoinStats,
    ) -> Self {
        let null_extension_rows = match (join_type, self.side) {
            (JoinType::Left | JoinType::LeftAny | JoinType::LeftSingle, Side::Right) => {
                stats.left.unmatched_rows()
            }
            (JoinType::Right | JoinType::RightAny | JoinType::RightSingle, Side::Left) => {
                stats.right.unmatched_rows()
            }
            (JoinType::LeftAsof, Side::Left) => stats.right.unmatched_rows(),
            (JoinType::RightAsof, Side::Right) => stats.left.unmatched_rows(),
            (JoinType::Full | JoinType::FullAsof, Side::Left) => stats.right.unmatched_rows(),
            (JoinType::Full | JoinType::FullAsof, Side::Right) => stats.left.unmatched_rows(),
            _ => 0.0,
        };
        if null_extension_rows <= 0.0 {
            return self;
        }

        for stat in self
            .join_keys
            .values_mut()
            .chain(self.non_keys.values_mut())
        {
            if null_extension_rows >= output_cardinality {
                *stat = ColumnStat::AllNull {
                    null_count: StatCount::estimate(output_cardinality, output_cardinality),
                };
                continue;
            }

            let null_count = stat.null_count();
            stat.set_null_count(StatCount::estimate(
                (null_count.expected() + null_extension_rows).min(output_cardinality),
                (null_count.upper() + null_extension_rows).min(output_cardinality),
            ));
        }
        self
    }

    fn clear_inconsistent_histograms(mut self, output_cardinality: f64) -> Self {
        for stat in self
            .join_keys
            .values_mut()
            .chain(self.non_keys.values_mut())
        {
            let Some(histogram_rows) = stat.histogram().map(|histogram| histogram.num_values())
            else {
                continue;
            };
            let expected_non_null_rows =
                (output_cardinality - stat.null_count().expected()).max(0.0);
            let tolerance = output_cardinality.max(1.0) * 1e-9;
            if (histogram_rows - expected_non_null_rows).abs() > tolerance {
                stat.clear_histogram();
            }
        }
        self
    }

    fn finish_histograms(
        mut self,
        join_type: JoinType,
        cardinality: f64,
        stats: &JoinStats,
    ) -> Result<Self> {
        let Some(columns) = stats.updated_columns else {
            return Ok(self);
        };

        for stat in self.non_keys.values_mut() {
            stat.clear_histogram();
        }

        let joined_column = self.side.join_column(columns);
        let keeps_semi_join_histogram = matches!(
            (join_type, self.side),
            (JoinType::LeftSemi, Side::Left) | (JoinType::RightSemi, Side::Right)
        );
        let keep_join_histogram = keeps_semi_join_histogram || join_type == JoinType::Inner;
        for (column, stat) in self.join_keys.iter_mut() {
            if !keep_join_histogram || *column != joined_column {
                // Other columns' histograms are inaccurate after the join cardinality update.
                stat.clear_histogram();
            }
        }

        if keeps_semi_join_histogram && let Some(stat) = self.join_keys.get_mut(&joined_column) {
            let side_stats = match self.side {
                Side::Left => &stats.left,
                Side::Right => &stats.right,
            };
            if let Some(histogram) = &side_stats.matched_histogram {
                stat.set_histogram(Some(histogram.clone()))
                    .map_err(ErrorCode::Internal)?;
            } else if !matches!(stat, ColumnStat::AllNull { .. })
                && let Some(input_stat) = self.input_join_stats.get(&joined_column)
            {
                stat.replace_histogram_from(input_stat, cardinality)
                    .map_err(ErrorCode::Internal)?;
            }
        }
        Ok(self)
    }

    fn into_output_column_stats(
        self,
        join_type: JoinType,
        input_cardinality: f64,
        output_cardinality: f64,
        stats: &JoinStats,
    ) -> Result<ColumnStatSet> {
        let side_stats = match self.side {
            Side::Left => &stats.left,
            Side::Right => &stats.right,
        };
        let side_matched_rows = side_stats.matched_rows;
        let (surviving_input_rows, value_output_rows) = match (join_type, self.side) {
            (JoinType::Inner | JoinType::InnerAny | JoinType::Asof, _) => {
                (side_matched_rows, output_cardinality)
            }
            (JoinType::Cross, _) => (input_cardinality, output_cardinality),
            (JoinType::Left | JoinType::LeftAny | JoinType::LeftSingle, Side::Left)
            | (JoinType::Right | JoinType::RightAny | JoinType::RightSingle, Side::Right) => {
                (input_cardinality, output_cardinality)
            }
            (JoinType::Left, Side::Right) | (JoinType::Right, Side::Left) => {
                (side_matched_rows, stats.cardinality)
            }
            (JoinType::LeftAny | JoinType::LeftSingle, Side::Right) => {
                (side_matched_rows, stats.left.matched_rows)
            }
            (JoinType::RightAny | JoinType::RightSingle, Side::Left) => {
                (side_matched_rows, stats.right.matched_rows)
            }
            (JoinType::Full, Side::Left) => (
                input_cardinality,
                stats.cardinality + stats.left.unmatched_rows(),
            ),
            (JoinType::Full, Side::Right) => (
                input_cardinality,
                stats.cardinality + stats.right.unmatched_rows(),
            ),
            (JoinType::LeftSemi, Side::Left) | (JoinType::RightSemi, Side::Right) => {
                (side_matched_rows, output_cardinality)
            }
            (JoinType::LeftAnti, Side::Left) | (JoinType::RightAnti, Side::Right) => {
                (output_cardinality, output_cardinality)
            }
            (JoinType::LeftAsof, Side::Right) | (JoinType::RightAsof, Side::Left) => {
                (input_cardinality, output_cardinality)
            }
            (JoinType::LeftAsof, Side::Left) => (side_matched_rows, stats.right.matched_rows),
            (JoinType::RightAsof, Side::Right) => (side_matched_rows, stats.left.matched_rows),
            (JoinType::FullAsof, Side::Left) => (
                input_cardinality,
                stats.right.matched_rows + stats.left.unmatched_rows(),
            ),
            (JoinType::FullAsof, Side::Right) => (input_cardinality, input_cardinality),
            _ => (0.0, 0.0),
        };
        let output = self
            .propagate_non_key_stats(input_cardinality, value_output_rows, surviving_input_rows)
            .cap_counts(output_cardinality)
            .apply_outer_null_extension(join_type, output_cardinality, stats)
            .finish_histograms(join_type, output_cardinality, stats)?
            .clear_inconsistent_histograms(output_cardinality);
        Ok(output.into_column_stats())
    }

    fn scale_count(count: StatCount, factor: f64, upper: f64) -> StatCount {
        if count == StatCount::exact(0) {
            return count;
        }
        StatCount::estimate(
            (count.expected() * factor).min(upper),
            (count.upper() * factor).min(upper),
        )
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

    fn join_key_columns(&self, side: Side) -> ColumnSet {
        self.equi_conditions
            .iter()
            .filter_map(|condition| {
                let (left, right) = condition.canonical_keys();
                Some(JoinConditionColumns {
                    left: direct_column(left)?,
                    right: direct_column(right)?,
                })
            })
            .map(|columns| side.join_column(columns))
            .collect()
    }

    fn estimate_inner_join_key_stats(
        &self,
        left_cardinality: f64,
        right_cardinality: f64,
        left_input_statistics: &Statistics,
        right_input_statistics: &Statistics,
        left: &mut JoinSideColumnStats,
        right: &mut JoinSideColumnStats,
    ) -> Result<JoinStats> {
        let mut left_join_keys = left.take_join_keys_for_estimation(self.join_type);
        let mut right_join_keys = right.take_join_keys_for_estimation(self.join_type);
        let drop_null_join_keys = matches!(
            self.join_type,
            JoinType::Inner
                | JoinType::InnerAny
                | JoinType::Asof
                | JoinType::LeftSemi
                | JoinType::RightSemi
        );
        let mut estimator =
            JoinStatsEstimator::new(left_cardinality, right_cardinality, drop_null_join_keys);
        let left_stat_cardinality = left_input_statistics
            .precise_cardinality
            .map(StatCardinality::exact)
            .unwrap_or_else(|| StatCardinality::estimate(left_cardinality));
        let right_stat_cardinality = right_input_statistics
            .precise_cardinality
            .map(StatCardinality::exact)
            .unwrap_or_else(|| StatCardinality::estimate(right_cardinality));
        for condition in &self.equi_conditions {
            if estimator.has_no_matches() {
                break;
            }
            let (left_condition, right_condition) = condition.canonical_keys();
            let output_columns = match (
                direct_column(left_condition),
                direct_column(right_condition),
            ) {
                (Some(left), Some(right)) => Some(JoinConditionColumns { left, right }),
                _ => None,
            };
            if drop_null_join_keys && !condition.is_null_equal {
                if let Some(column) = null_rejected_column(left_condition) {
                    left.clear_null_count(&mut left_join_keys, column);
                }
                if let Some(column) = null_rejected_column(right_condition) {
                    right.clear_null_count(&mut right_join_keys, column);
                }
            }
            let left_condition_stat =
                join_condition_stat(left_condition, left_input_statistics, left_stat_cardinality)?;
            let right_condition_stat = join_condition_stat(
                right_condition,
                right_input_statistics,
                right_stat_cardinality,
            )?;
            let (Some(left_condition_stat), Some(right_condition_stat)) =
                (&left_condition_stat, &right_condition_stat)
            else {
                estimator.apply_missing_condition_statistics(
                    left_condition_stat.as_deref(),
                    right_condition_stat.as_deref(),
                    condition.is_null_equal,
                );
                continue;
            };
            estimator.apply_condition(
                output_columns,
                left_condition.data_type().as_ref(),
                right_condition.data_type().as_ref(),
                left_condition_stat.as_ref(),
                right_condition_stat.as_ref(),
                condition.is_null_equal,
                &mut left_join_keys,
                &mut right_join_keys,
            )?;
        }
        left.set_estimated_join_keys(self.join_type, left_join_keys);
        right.set_estimated_join_keys(self.join_type, right_join_keys);
        Ok(estimator.finish())
    }

    fn join_cardinality(
        &self,
        left_cardinality: f64,
        right_cardinality: f64,
        stats: &JoinStats,
    ) -> f64 {
        let inner_join_cardinality = stats.cardinality;
        match self.join_type {
            JoinType::Inner | JoinType::Cross => inner_join_cardinality,
            JoinType::InnerAny => stats
                .ndv
                .and_then(|ndv| ndv.expected)
                .unwrap_or_else(|| stats.left.matched_rows.min(stats.right.matched_rows)),
            // ASOF plans have swapped logical children: the right child is the
            // original probe side and contributes at most one output per row.
            JoinType::Asof => stats.right.matched_rows,
            JoinType::Left => inner_join_cardinality + stats.left.unmatched_rows(),
            JoinType::Right => inner_join_cardinality + stats.right.unmatched_rows(),
            JoinType::Full => {
                inner_join_cardinality + stats.left.unmatched_rows() + stats.right.unmatched_rows()
            }
            JoinType::LeftAny => left_cardinality,
            JoinType::RightAny => right_cardinality,
            JoinType::LeftAsof => right_cardinality,
            JoinType::RightAsof => left_cardinality,
            JoinType::FullAsof => right_cardinality + stats.left.unmatched_rows(),
            JoinType::LeftSemi => stats.left.matched_rows,
            JoinType::RightSemi => stats.right.matched_rows,
            JoinType::LeftAnti => estimate_anti_join_cardinality(
                left_cardinality,
                stats.left.matched_rows,
                stats.left.estimated_matched_rows,
            ),
            JoinType::RightAnti => estimate_anti_join_cardinality(
                right_cardinality,
                stats.right.matched_rows,
                stats.right.estimated_matched_rows,
            ),
            JoinType::LeftSingle | JoinType::RightMark => left_cardinality,
            JoinType::RightSingle | JoinType::LeftMark => right_cardinality,
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
        let left_cardinality = left_stat_info.cardinality;
        let right_cardinality = right_stat_info.cardinality;
        let left_max_cardinality = left_stat_info.max_cardinality.max(left_cardinality);
        let right_max_cardinality = right_stat_info.max_cardinality.max(right_cardinality);
        let mut left_column_stats = JoinSideColumnStats::split(
            Side::Left,
            left_stat_info.statistics.column_stats.clone(),
            &self.join_key_columns(Side::Left),
        );
        let mut right_column_stats = JoinSideColumnStats::split(
            Side::Right,
            right_stat_info.statistics.column_stats.clone(),
            &self.join_key_columns(Side::Right),
        );

        // Evaluating join cardinality using histograms.
        // If histogram is None, will evaluate using NDV.
        let join_stats = self.estimate_inner_join_key_stats(
            left_cardinality,
            right_cardinality,
            &left_stat_info.statistics,
            &right_stat_info.statistics,
            &mut left_column_stats,
            &mut right_column_stats,
        )?;
        let cardinality = self.join_cardinality(left_cardinality, right_cardinality, &join_stats);

        // Derive column statistics
        let column_stats = if cardinality == 0.0 {
            HashMap::new()
        } else {
            left_column_stats
                .into_output_column_stats(
                    self.join_type,
                    left_cardinality,
                    cardinality,
                    &join_stats,
                )?
                .into_iter()
                .chain(right_column_stats.into_output_column_stats(
                    self.join_type,
                    right_cardinality,
                    cardinality,
                    &join_stats,
                )?)
                .collect()
        };
        Ok(Arc::new(StatInfo {
            cardinality,
            // Preserve the largest known source risk behind either input.
            // This intentionally does not model many-to-many output fan-out;
            // max_cardinality is a scoped source-risk heuristic, not a strict
            // upper bound on join output rows.
            max_cardinality: left_max_cardinality
                .max(right_max_cardinality)
                .max(cardinality),
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        }))
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

fn estimate_anti_join_cardinality(
    input_cardinality: f64,
    matched_rows: f64,
    estimated_matched_rows: Option<f64>,
) -> f64 {
    if input_cardinality <= 0.0 {
        return 0.0;
    }

    let matched_rows = matched_rows.clamp(0.0, input_cardinality);
    let estimated_matched_rows = estimated_matched_rows
        .unwrap_or(0.0)
        .clamp(0.0, matched_rows);
    // Bucket overlap cannot prove dense value-set coverage. Only cap the
    // uncertain portion when it would push the total match rate above 90%.
    const MAX_ANTI_JOIN_ESTIMATED_OVERLAP: f64 = 0.9;
    let confirmed_matched_rows = matched_rows - estimated_matched_rows;
    let estimated_match_budget =
        (input_cardinality * MAX_ANTI_JOIN_ESTIMATED_OVERLAP - confirmed_matched_rows).max(0.0);
    let adjusted_matched_rows =
        confirmed_matched_rows + estimated_matched_rows.min(estimated_match_budget);
    input_cardinality - adjusted_matched_rows
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
            let build_stat_info = rel_expr.stat_info_child_group(1)?;
            let max_broadcast_build_rows = settings.get_max_broadcast_join_build_rows()?;
            let cluster_nodes = ctx.get_cluster().nodes.len();
            // The broadcast join is cheaper than the hash join when one input is at least (n − 1)× larger than the other
            // where n is the number of servers in the cluster.
            let broadcast_join_threshold = if settings.get_prefer_broadcast_join()? {
                cluster_nodes.saturating_sub(1) as f64
            } else {
                // Use a very large value to prevent broadcast join.
                1000.0
            };
            if !settings.get_enforce_shuffle_join()?
                && (settings.get_enforce_broadcast_join()?
                    || (broadcast_build_allowed(
                        false,
                        cluster_nodes,
                        self.join_type,
                        &build_stat_info,
                        max_broadcast_build_rows,
                    ) && right_stat_info.cardinality * broadcast_join_threshold
                        < left_stat_info.cardinality))
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
        rel_expr: &RelExpr,
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

        let enforce_broadcast_join = settings.get_enforce_broadcast_join()?;
        let build_stat_info = rel_expr.stat_info_child_group(1)?;
        let max_broadcast_build_rows = settings.get_max_broadcast_join_build_rows()?;
        let cluster_nodes = ctx.get_cluster().nodes.len();
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
            && broadcast_build_allowed(
                enforce_broadcast_join,
                cluster_nodes,
                self.join_type,
                build_stat_info.as_ref(),
                max_broadcast_build_rows,
            )
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

fn broadcast_build_allowed(
    enforce_broadcast: bool,
    cluster_nodes: usize,
    join_type: JoinType,
    stat_info: &StatInfo,
    max_build_rows: u64,
) -> bool {
    enforce_broadcast
        || cluster_nodes <= 1
        || matches!(join_type, JoinType::Cross)
        || is_safe_broadcast_build(stat_info, max_build_rows)
}

fn is_safe_broadcast_build(stat_info: &StatInfo, max_build_rows: u64) -> bool {
    let cardinality = stat_info.cardinality;
    let risk_cardinality = stat_info.max_cardinality;

    // This scoped guard compares the expected build output with its largest
    // known source. Unknown source sizes retain the pre-existing distribution
    // choice; output-expansion risks such as many-to-many fan-out are outside
    // this heuristic and require a separate model.
    if !risk_cardinality.is_finite() {
        return true;
    }

    let max_cardinality = risk_cardinality.max(cardinality);
    cardinality.is_finite()
        && cardinality >= 0.0
        && risk_cardinality >= 0.0
        && (max_build_rows == 0 || max_cardinality <= max_build_rows as f64)
        && if cardinality == 0.0 {
            max_cardinality == 0.0
        } else {
            !stat_info.cardinality_is_severely_underestimated()
        }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::stat_distribution::NdvEstimate;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_statistics::TypedHistogram;
    use databend_common_statistics::TypedHistogramBucket;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::optimizer::ir::SExpr;
    use crate::plans::BoundColumnRef;
    use crate::plans::CastExpr;
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

    fn int_column_stat(min: i64, max: i64, ndv: f64) -> ColumnStat {
        ColumnStat::Int {
            min,
            max,
            ndv: NdvEstimate::exact(ndv),
            null_count: StatCount::exact(0),
            histogram: None,
        }
    }

    fn stat_info(cardinality: f64, column: Symbol, stat: ColumnStat) -> Arc<StatInfo> {
        Arc::new(StatInfo {
            cardinality,
            max_cardinality: cardinality,
            statistics: Statistics {
                precise_cardinality: Some(cardinality as u64),
                column_stats: HashMap::from([(column, stat)]),
                ..Default::default()
            },
        })
    }

    #[test]
    fn test_anti_join_only_scales_estimated_matched_rows() {
        assert_eq!(estimate_anti_join_cardinality(100.0, 100.0, None), 0.0);
        assert_eq!(
            estimate_anti_join_cardinality(100.0, 100.0, Some(100.0)),
            10.0
        );
        assert_eq!(
            estimate_anti_join_cardinality(100.0, 60.0, Some(50.0)),
            40.0
        );
        assert_eq!(
            estimate_anti_join_cardinality(100.0, 95.0, Some(95.0)),
            10.0
        );
        assert_eq!(estimate_anti_join_cardinality(100.0, 0.0, Some(0.0)), 100.0);
    }

    #[test]
    fn test_canonical_integer_string_keys_drive_join_stats() -> Result<()> {
        let left_key = column(0, DataType::Number(NumberDataType::Int64));
        let right_source = column(1, DataType::Number(NumberDataType::Int32));
        let right_key = ScalarExpr::CastExpr(CastExpr {
            span: None,
            is_try: false,
            argument: Box::new(right_source),
            target_type: Box::new(DataType::String),
        });
        let join = Join {
            join_type: JoinType::Inner,
            equi_conditions: vec![JoinEquiCondition::new(left_key, right_key, false)],
            ..Default::default()
        };
        let left = stat_info(4.0, Symbol::new(0), int_column_stat(1, 4, 4.0));
        let right = stat_info(3.0, Symbol::new(1), int_column_stat(0, 1, 2.0));

        let stats = join.derive_join_stats(left, right)?;

        assert_eq!(stats.cardinality, 3.0);
        Ok(())
    }

    #[test]
    fn test_finish_semi_join_histogram_skips_all_null_join_key() -> Result<()> {
        let left_stat = ColumnStat::Int {
            min: 1,
            max: 1,
            ndv: NdvEstimate::exact(1.0),
            null_count: StatCount::exact(1),
            histogram: Some(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(1, 1, 1.0, 1.0)],
                avg_spacing: None,
            }),
        };
        let right_stat = ColumnStat::Int {
            min: 2,
            max: 2,
            ndv: NdvEstimate::exact(1.0),
            null_count: StatCount::exact(1),
            histogram: None,
        };
        let mut left_stats = HashMap::from([(Symbol::new(0), left_stat.clone())]);
        let mut right_stats = HashMap::from([(Symbol::new(1), right_stat.clone())]);
        let mut estimator = JoinStatsEstimator::new(2.0, 2.0, false);

        estimator.apply_condition(
            Some(JoinConditionColumns {
                left: Symbol::new(0),
                right: Symbol::new(1),
            }),
            &DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            &DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            &left_stat,
            &right_stat,
            true,
            &mut left_stats,
            &mut right_stats,
        )?;
        let stats = estimator.finish();

        let output = JoinSideColumnStats {
            side: Side::Left,
            join_keys: left_stats,
            non_keys: HashMap::new(),
            input_join_stats: HashMap::from([(Symbol::new(0), left_stat)]),
        }
        .finish_histograms(JoinType::LeftSemi, 1.0, &stats)?;

        assert_eq!(output.join_keys[&Symbol::new(0)], ColumnStat::AllNull {
            null_count: StatCount::estimate(1.0, 1.0),
        });
        Ok(())
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

    fn estimated_stat(cardinality: f64, max_cardinality: f64) -> Arc<StatInfo> {
        Arc::new(StatInfo {
            cardinality,
            max_cardinality,
            statistics: Statistics::default(),
        })
    }

    #[test]
    fn test_broadcast_build_guard_enforces_risk_bound() {
        const DEFAULT_MAX_BUILD_ROWS: u64 = 100_000_000;

        assert!(is_safe_broadcast_build(
            &estimated_stat(1_000.0, 100_000.0),
            DEFAULT_MAX_BUILD_ROWS,
        ));
        assert!(!is_safe_broadcast_build(
            &estimated_stat(10.0, 200_000_000.0),
            DEFAULT_MAX_BUILD_ROWS,
        ));
        assert!(!is_safe_broadcast_build(
            &estimated_stat(10.0, 100_000.0),
            DEFAULT_MAX_BUILD_ROWS,
        ));
        assert!(is_safe_broadcast_build(
            &estimated_stat(200_000_000.0, 200_000_000.0),
            0,
        ));
        assert!(!is_safe_broadcast_build(
            &estimated_stat(0.0, 1.0),
            DEFAULT_MAX_BUILD_ROWS,
        ));
        assert!(is_safe_broadcast_build(
            &StatInfo::default(),
            DEFAULT_MAX_BUILD_ROWS,
        ));
        assert!(is_safe_broadcast_build(
            &estimated_stat(1_000.0, f64::INFINITY),
            DEFAULT_MAX_BUILD_ROWS,
        ));
    }

    #[test]
    fn test_explicit_broadcast_overrides_automatic_guard() {
        let unsafe_build = estimated_stat(10.0, 200_000_000.0);

        assert!(broadcast_build_allowed(
            true,
            3,
            JoinType::Inner,
            &unsafe_build,
            100_000_000,
        ));
        assert!(broadcast_build_allowed(
            false,
            1,
            JoinType::Inner,
            &unsafe_build,
            100_000_000,
        ));
        assert!(broadcast_build_allowed(
            false,
            3,
            JoinType::Cross,
            &unsafe_build,
            100_000_000,
        ));
        assert!(!broadcast_build_allowed(
            false,
            3,
            JoinType::Inner,
            &unsafe_build,
            100_000_000,
        ));
    }
}
