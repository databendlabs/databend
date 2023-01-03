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

use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::Result;

use super::ScalarExpr;
use crate::optimizer::ColumnSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::Scalar;
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
    pub left_conditions: Vec<Scalar>,
    pub right_conditions: Vec<Scalar>,
    pub non_equi_conditions: Vec<Scalar>,
    pub join_type: JoinType,
    // marker_index is for MarkJoin only.
    pub marker_index: Option<IndexType>,
    pub from_correlated_subquery: bool,
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
}

impl Operator for Join {
    fn rel_op(&self) -> RelOp {
        RelOp::Join
    }

    fn derive_relational_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;

        // Derive output columns
        let mut output_columns = left_prop.output_columns;
        if let Some(mark_index) = self.marker_index {
            output_columns.insert(mark_index);
        }
        output_columns = output_columns
            .union(&right_prop.output_columns)
            .cloned()
            .collect();

        // Derive outer columns
        let mut outer_columns = left_prop.outer_columns;
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

        // Derive cardinality. We can not estimate the cardinality of inner join until we have
        // distribution information of join keys, so we set it to the maximum value.
        let cardinality = match self.join_type {
            // Todo(xudong): after `distinct_count` of `ColumnStat` is ready, we can estimate more precisely
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                f64::max(left_prop.cardinality, right_prop.cardinality)
            }
            JoinType::Cross => left_prop.cardinality * right_prop.cardinality,

            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark | JoinType::Single => {
                left_prop.cardinality
            }

            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                right_prop.cardinality
            }
        };

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(left_prop.used_columns);
        used_columns.extend(right_prop.used_columns);

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: Default::default(),
                is_accurate: false,
            },
        })
    }

    fn derive_physical_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<PhysicalProperty> {
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

    fn compute_required_prop_child<'a>(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr<'a>,
        child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();

        let probe_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        let build_physical_prop = rel_expr.derive_physical_prop_child(1)?;

        if probe_physical_prop.distribution == Distribution::Serial
            || build_physical_prop.distribution == Distribution::Serial
        {
            // TODO(leiysky): we can enforce redistribution here
            required.distribution = Distribution::Serial;
        } else if ctx.get_settings().get_prefer_broadcast_join()?
            && !matches!(self.join_type, JoinType::Right | JoinType::Full)
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
