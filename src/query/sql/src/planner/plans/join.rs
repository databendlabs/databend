// Copyright 2022 Datafuse Labs.
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

use common_catalog::table_context::TableContext;
use common_exception::Result;

use crate::optimizer::ColumnSet;
use crate::optimizer::ColumnStat;
use crate::optimizer::Datum;
use crate::optimizer::Distribution;
use crate::optimizer::Histogram;
use crate::optimizer::InterleavedBucket;
use crate::optimizer::NewStatistic;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::optimizer::UniformSampleSet;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::IndexType;

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    Cross,
    /// Mark Join is a special case of join that is used to process Any subquery and correlated Exists subquery.
    /// Left Mark Join use subquery as probe side, it's blocked at `mark_join_blocks`
    LeftMark,
    /// Right Mark Join use subquery as build side, it's executed by streaming.
    RightMark,
    /// Single Join is a special kind of join that is used to process correlated scalar subquery.
    Single,
}

impl JoinType {
    pub fn opposite(&self) -> JoinType {
        match self {
            JoinType::Left => JoinType::Right,
            JoinType::Right => JoinType::Left,
            JoinType::LeftSemi => JoinType::RightSemi,
            JoinType::RightSemi => JoinType::LeftSemi,
            JoinType::LeftAnti => JoinType::RightAnti,
            JoinType::RightAnti => JoinType::LeftAnti,
            JoinType::LeftMark => JoinType::RightMark,
            JoinType::RightMark => JoinType::LeftMark,
            _ => self.clone(),
        }
    }

    pub fn is_outer_join(&self) -> bool {
        matches!(self, JoinType::Left | JoinType::Right | JoinType::Full)
    }

    pub fn is_mark_join(&self) -> bool {
        matches!(self, JoinType::LeftMark | JoinType::RightMark)
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
            JoinType::Single => {
                write!(f, "SINGLE")
            }
        }
    }
}

/// Join operator. We will choose hash join by default.
/// In the case that using hash join, the right child
/// is always the build side, and the left child is always
/// the probe side.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Join {
    pub left_conditions: Vec<ScalarExpr>,
    pub right_conditions: Vec<ScalarExpr>,
    pub non_equi_conditions: Vec<ScalarExpr>,
    pub join_type: JoinType,
    // marker_index is for MarkJoin only.
    pub marker_index: Option<IndexType>,
    pub from_correlated_subquery: bool,
    // It means that join has a corresponding runtime filter
    pub contain_runtime_filter: bool,
}

impl Default for Join {
    fn default() -> Self {
        Self {
            left_conditions: Default::default(),
            right_conditions: Default::default(),
            non_equi_conditions: Default::default(),
            join_type: JoinType::Cross,
            marker_index: Default::default(),
            from_correlated_subquery: Default::default(),
            contain_runtime_filter: false,
        }
    }
}

impl Join {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for cond in self
            .left_conditions
            .iter()
            .chain(self.right_conditions.iter())
            .chain(self.non_equi_conditions.iter())
        {
            used_columns = used_columns.union(&cond.used_columns()).cloned().collect();
        }
        Ok(used_columns)
    }

    fn inner_join_cardinality(
        &self,
        left_cardinality: &mut f64,
        right_cardinality: &mut f64,
        left_statistics: &mut Statistics,
        right_statistics: &mut Statistics,
    ) -> Result<f64> {
        let mut join_card = *left_cardinality * *right_cardinality;
        for (left_condition, right_condition) in self
            .left_conditions
            .iter()
            .zip(self.right_conditions.iter())
        {
            if join_card == 0 as f64 {
                break;
            }
            // Currently don't consider the case such as: `t1.a + t1.b = t2.a`
            if left_condition.used_columns().len() != 1 || right_condition.used_columns().len() != 1
            {
                continue;
            }
            let left_col_stat = left_statistics
                .column_stats
                .get(left_condition.used_columns().iter().next().unwrap());
            let right_col_stat = right_statistics
                .column_stats
                .get(right_condition.used_columns().iter().next().unwrap());
            match (left_col_stat, right_col_stat) {
                (Some(left_col_stat), Some(right_col_stat)) => {
                    if !left_col_stat.min.type_comparable(&right_col_stat.min) {
                        continue;
                    }
                    let left_interval =
                        UniformSampleSet::new(left_col_stat.min.clone(), left_col_stat.max.clone());
                    let right_interval = UniformSampleSet::new(
                        right_col_stat.min.clone(),
                        right_col_stat.max.clone(),
                    );
                    if !left_interval.has_intersection(&right_interval)? {
                        join_card = 0.0;
                        continue;
                    }

                    // Update column min and max value
                    let mut new_ndv = None;
                    let (new_min, new_max) = left_interval.intersection(&right_interval)?;

                    if let Datum::Bytes(_) | Datum::Bool(_) = left_col_stat.min {
                        let card = evaluate_by_ndv(
                            left_col_stat,
                            right_col_stat,
                            *left_cardinality,
                            *right_cardinality,
                            &mut new_ndv,
                        );
                        if card < join_card {
                            join_card = card;
                        }
                        update_statistic(
                            left_statistics,
                            right_statistics,
                            left_condition,
                            right_condition,
                            NewStatistic {
                                min: new_min,
                                max: new_max,
                                ndv: new_ndv,
                            },
                        );
                        continue;
                    }
                    let card = match (&left_col_stat.histogram, &right_col_stat.histogram) {
                        (Some(left_hist), Some(right_hist)) => {
                            // Evaluate join cardinality by histogram.
                            evaluate_by_histogram(left_hist, right_hist, &mut new_ndv)?
                        }
                        _ => evaluate_by_ndv(
                            left_col_stat,
                            right_col_stat,
                            *left_cardinality,
                            *right_cardinality,
                            &mut new_ndv,
                        ),
                    };
                    if card < join_card {
                        join_card = card;
                    }
                    update_statistic(
                        left_statistics,
                        right_statistics,
                        left_condition,
                        right_condition,
                        NewStatistic {
                            min: new_min,
                            max: new_max,
                            ndv: new_ndv,
                        },
                    );
                }
                _ => continue,
            }
        }

        Ok(join_card)
    }
}

impl Operator for Join {
    fn rel_op(&self) -> RelOp {
        RelOp::Join
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<RelationalProperty> {
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
        for cond in self
            .left_conditions
            .iter()
            .chain(self.right_conditions.iter())
        {
            let used_columns = cond.used_columns();
            let outer = used_columns.difference(&output_columns).cloned().collect();
            outer_columns = outer_columns.union(&outer).cloned().collect();
        }
        outer_columns = outer_columns.difference(&output_columns).cloned().collect();

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(left_prop.used_columns);
        used_columns.extend(right_prop.used_columns);

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
        })
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        let probe_prop = rel_expr.derive_physical_prop_child(0)?;
        let build_prop = rel_expr.derive_physical_prop_child(1)?;

        match (&probe_prop.distribution, &build_prop.distribution) {
            // If the distribution of probe side is Random, we will pass through
            // the distribution of build side.
            (Distribution::Random, _) => Ok(PhysicalProperty {
                distribution: build_prop.distribution.clone(),
            }),
            // Otherwise pass through probe side.
            _ => Ok(PhysicalProperty {
                distribution: probe_prop.distribution.clone(),
            }),
        }
    }

    fn derive_cardinality(&self, rel_expr: &RelExpr) -> Result<StatInfo> {
        let left_stat_info = rel_expr.derive_cardinality_child(0)?;
        let right_stat_info = rel_expr.derive_cardinality_child(1)?;
        let (mut left_cardinality, mut left_statistics) =
            (left_stat_info.cardinality, left_stat_info.statistics);
        let (mut right_cardinality, mut right_statistics) =
            (right_stat_info.cardinality, right_stat_info.statistics);
        // Evaluating join cardinality using histograms.
        // If histogram is None, will evaluate using NDV.
        let inner_join_cardinality = self.inner_join_cardinality(
            &mut left_cardinality,
            &mut right_cardinality,
            &mut left_statistics,
            &mut right_statistics,
        )?;
        let cardinality = match self.join_type {
            JoinType::Inner | JoinType::Cross => inner_join_cardinality,
            JoinType::Left => f64::max(left_cardinality, inner_join_cardinality),
            JoinType::Right => f64::max(right_cardinality, inner_join_cardinality),
            JoinType::Full => {
                f64::max(left_cardinality, inner_join_cardinality)
                    + f64::max(right_cardinality, inner_join_cardinality)
                    - inner_join_cardinality
            }
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark | JoinType::Single => {
                left_cardinality
            }
            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => right_cardinality,
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
        Ok(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
            },
        })
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

        // if join/probe side is Serial or join key is empty, we use Serial distribution
        if probe_physical_prop.distribution == Distribution::Serial
            || build_physical_prop.distribution == Distribution::Serial
        {
            // TODO(leiysky): we can enforce redistribution here
            required.distribution = Distribution::Serial;
        } else if ctx.get_settings().get_prefer_broadcast_join()?
            && !matches!(
                self.join_type,
                JoinType::Right
                    | JoinType::Full
                    | JoinType::RightAnti
                    | JoinType::RightSemi
                    | JoinType::RightMark
            )
        {
            required.distribution = Distribution::Broadcast;
        } else if child_index == 0 {
            required.distribution = Distribution::Hash(self.left_conditions.clone());
        } else {
            required.distribution = Distribution::Hash(self.right_conditions.clone());
        }

        Ok(required)
    }
}

fn evaluate_by_histogram(
    left_hist: &Histogram,
    right_hist: &Histogram,
    new_ndv: &mut Option<f64>,
) -> Result<f64> {
    let mut interleaved_buckets = vec![];
    let mut all_ndv = 0.0;
    for (idx, (left_bucket, right_bucket)) in left_hist
        .buckets
        .iter()
        .zip(right_hist.buckets.iter())
        .enumerate()
    {
        if idx == 0 {
            continue;
        }

        let left_bucket_min = left_hist.buckets[idx - 1].upper_bound().to_double()?;
        let left_bucket_max = left_bucket.upper_bound().to_double()?;
        let right_bucket_min = right_hist.buckets[idx - 1].upper_bound().to_double()?;
        let right_bucket_max = right_bucket.upper_bound().to_double()?;
        if left_bucket_min <= right_bucket_max && left_bucket_max >= right_bucket_min {
            // There are four cases for interleaving
            // 1. left bucket contains right bucket
            // ---left_min---right_min---right_max---left_max---
            if right_bucket_min >= left_bucket_min && right_bucket_max <= left_bucket_max {
                let percentage =
                    (right_bucket_max - right_bucket_min) / (left_bucket_max - left_bucket_min);
                interleaved_buckets.push(InterleavedBucket {
                    left_ndv: left_bucket.num_distinct() * percentage,
                    right_ndv: right_bucket.num_distinct(),
                    left_num_rows: left_bucket.num_values() * percentage,
                    right_num_rows: right_bucket.num_values(),
                    max_val: right_bucket_max,
                })
            } else if left_bucket_min >= right_bucket_min && left_bucket_max <= right_bucket_max {
                // 2. right bucket contains left bucket
                // ---right_min---left_min---left_max---right_max---
                let percentage =
                    (left_bucket_max - left_bucket_min) / (right_bucket_max - right_bucket_min);
                interleaved_buckets.push(InterleavedBucket {
                    left_ndv: left_bucket.num_distinct(),
                    right_ndv: right_bucket.num_distinct() * percentage,
                    left_num_rows: left_bucket.num_values(),
                    right_num_rows: right_bucket.num_values() * percentage,
                    max_val: left_bucket_max,
                })
            } else if left_bucket_min <= right_bucket_min && left_bucket_max <= right_bucket_max {
                // 3. left bucket intersects with right bucket on the left
                // ---left_min---right_min---left_max---right_max---
                if left_bucket_max == right_bucket_min {
                    interleaved_buckets.push(InterleavedBucket {
                        left_ndv: 1.0,
                        right_ndv: 1.0,
                        left_num_rows: left_bucket.num_values() / left_bucket.num_distinct(),
                        right_num_rows: right_bucket.num_values() / right_bucket.num_distinct(),
                        max_val: left_bucket_max,
                    });
                    continue;
                }
                let left_percentage =
                    (left_bucket_max - right_bucket_min) / (left_bucket_max - left_bucket_min);
                let right_percentage =
                    (left_bucket_max - right_bucket_min) / (right_bucket_max - right_bucket_min);
                interleaved_buckets.push(InterleavedBucket {
                    left_ndv: left_bucket.num_distinct() * left_percentage,
                    right_ndv: right_bucket.num_distinct() * right_percentage,
                    left_num_rows: left_bucket.num_values() * left_percentage,
                    right_num_rows: right_bucket.num_values() * right_percentage,
                    max_val: left_bucket_max,
                })
            } else if left_bucket_min >= right_bucket_min && left_bucket_max >= right_bucket_max {
                // 4. left bucket intersects with right bucket on the right
                // ---right_min---left_min---right_max---left_max---
                if right_bucket_max == left_bucket_min {
                    interleaved_buckets.push(InterleavedBucket {
                        left_ndv: 1.0,
                        right_ndv: 1.0,
                        left_num_rows: left_bucket.num_values() / left_bucket.num_distinct(),
                        right_num_rows: right_bucket.num_values() / right_bucket.num_distinct(),
                        max_val: right_bucket_max,
                    });
                    continue;
                }
                let left_percentage =
                    (right_bucket_max - left_bucket_min) / (left_bucket_max - left_bucket_min);
                let right_percentage =
                    (right_bucket_max - left_bucket_min) / (right_bucket_max - right_bucket_min);
                interleaved_buckets.push(InterleavedBucket {
                    left_ndv: left_bucket.num_distinct() * left_percentage,
                    right_ndv: right_bucket.num_distinct() * right_percentage,
                    left_num_rows: left_bucket.num_values() * left_percentage,
                    right_num_rows: right_bucket.num_values() * right_percentage,
                    max_val: right_bucket_max,
                })
            }
        }
    }
    let mut card = 0.0;
    for (idx, bucket) in interleaved_buckets.iter().enumerate() {
        if idx == 0 || bucket.max_val != interleaved_buckets[idx - 1].max_val {
            all_ndv += bucket.left_ndv.min(bucket.right_ndv);
        }
        let max_ndv = f64::max(bucket.left_ndv, bucket.right_ndv);
        if max_ndv == 0.0 {
            continue;
        }
        card += bucket.left_num_rows * bucket.right_num_rows / max_ndv;
    }
    *new_ndv = Some(all_ndv);
    Ok(card)
}

fn evaluate_by_ndv(
    left_stat: &ColumnStat,
    right_stat: &ColumnStat,
    left_cardinality: f64,
    right_cardinality: f64,
    new_ndv: &mut Option<f64>,
) -> f64 {
    // Update column ndv
    *new_ndv = Some(left_stat.ndv.min(right_stat.ndv));

    let max_ndv = f64::max(left_stat.ndv, right_stat.ndv);
    if max_ndv == 0.0 {
        0.0
    } else {
        left_cardinality * right_cardinality / max_ndv
    }
}

fn update_statistic(
    left_statistics: &mut Statistics,
    right_statistics: &mut Statistics,
    left_condition: &ScalarExpr,
    right_condition: &ScalarExpr,
    new_stat: NewStatistic,
) {
    let left_col_stat = left_statistics
        .column_stats
        .get_mut(left_condition.used_columns().iter().next().unwrap())
        .unwrap();
    let right_col_stat = right_statistics
        .column_stats
        .get_mut(right_condition.used_columns().iter().next().unwrap())
        .unwrap();
    if let Some(new_min) = new_stat.min {
        left_col_stat.min = new_min.clone();
        right_col_stat.min = new_min;
    }
    if let Some(new_max) = new_stat.max {
        left_col_stat.max = new_max.clone();
        right_col_stat.max = new_max;
    }
    if let Some(new_ndv) = new_stat.ndv {
        left_col_stat.ndv = new_ndv;
        right_col_stat.ndv = new_ndv;
    }
}
