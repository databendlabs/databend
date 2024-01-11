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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

pub struct RuleSemiToInnerJoin {
    id: RuleID,
    patterns: Vec<SExpr>,
    _metadata: MetadataRef,
}

impl RuleSemiToInnerJoin {
    pub fn new(_metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::SemiToInnerJoin,
            patterns: vec![SExpr::create_binary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Join,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_leaf(Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ))),
                Arc::new(SExpr::create_leaf(Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ))),
            )],
            _metadata,
        }
    }
}

impl Rule for RuleSemiToInnerJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let mut join: Join = s_expr.plan().clone().try_into()?;
        if !matches!(join.join_type, JoinType::LeftSemi | JoinType::RightSemi) {
            return Ok(());
        }

        let conditions = if join.join_type == JoinType::LeftSemi {
            join.right_conditions.clone()
        } else {
            join.left_conditions.clone()
        };

        if conditions.is_empty() {
            return Ok(());
        }

        let mut condition_cols = HashSet::with_capacity(conditions.len());
        for condition in conditions.iter() {
            add_column_idx(condition, &mut condition_cols);
        }

        let child = if join.join_type == JoinType::LeftSemi {
            s_expr.child(1)?
        } else {
            s_expr.child(0)?
        };

        // Traverse child to find join keys in group by keys
        let mut group_by_keys = HashSet::new();
        find_group_by_keys(child, &mut group_by_keys)?;
        if condition_cols
            .iter()
            .all(|condition| group_by_keys.contains(condition))
        {
            join.join_type = JoinType::Inner;
            let mut join_expr = SExpr::create_binary(
                Arc::new(join.into()),
                Arc::new(s_expr.child(0)?.clone()),
                Arc::new(s_expr.child(1)?.clone()),
            );
            join_expr.set_applied_rule(&self.id);
            state.add_result(join_expr);
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}

fn find_group_by_keys(child: &SExpr, group_by_keys: &mut HashSet<IndexType>) -> Result<()> {
    match child.plan() {
        RelOperator::EvalScalar(_) | RelOperator::Filter(_) | RelOperator::Window(_) => {
            find_group_by_keys(child.child(0)?, group_by_keys)?;
        }
        RelOperator::Aggregate(agg) => {
            for item in agg.group_items.iter() {
                if let ScalarExpr::BoundColumnRef(c) = &item.scalar {
                    group_by_keys.insert(c.column.index);
                }
            }
        }
        RelOperator::Sort(_)
        | RelOperator::Limit(_)
        | RelOperator::Exchange(_)
        | RelOperator::AddRowNumber(_)
        | RelOperator::UnionAll(_)
        | RelOperator::DummyTableScan(_)
        | RelOperator::ProjectSet(_)
        | RelOperator::MaterializedCte(_)
        | RelOperator::ConstantTableScan(_)
        | RelOperator::Udf(_)
        | RelOperator::Scan(_)
        | RelOperator::CteScan(_)
        | RelOperator::Join(_)
        | RelOperator::Pattern(_) => {}
    }
    Ok(())
}

fn add_column_idx(condition: &ScalarExpr, condition_cols: &mut HashSet<IndexType>) {
    match condition {
        ScalarExpr::BoundColumnRef(c) => {
            condition_cols.insert(c.column.index);
        }
        ScalarExpr::CastExpr(expr) => {
            add_column_idx(&expr.argument, condition_cols);
        }
        _ => {}
    }
}
