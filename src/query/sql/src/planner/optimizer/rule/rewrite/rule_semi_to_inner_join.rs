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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::IndexType;
use crate::ScalarExpr;

pub struct RuleSemiToInnerJoin {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleSemiToInnerJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::SemiToInnerJoin,
            // Join
            // |  \
            // *   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
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
            join.equi_conditions
                .iter()
                .map(|condition| condition.right.clone())
                .collect::<Vec<_>>()
        } else {
            join.equi_conditions
                .iter()
                .map(|condition| condition.left.clone())
                .collect::<Vec<_>>()
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
        let mut group_by_keys = HashMap::new();
        find_group_by_keys(child, &mut group_by_keys)?;

        // If condition are all group by keys and not nullable
        // we can rewrite semi join to inner join
        // inner join will ignore null values but semi join will keep them
        // this happens in Q38
        if condition_cols.iter().all(|condition| {
            if let Some(t) = group_by_keys.get(condition) {
                !t.is_nullable_or_null()
            } else {
                false
            }
        }) {
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

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

fn find_group_by_keys(
    child: &SExpr,
    group_by_keys: &mut HashMap<IndexType, Box<DataType>>,
) -> Result<()> {
    match child.plan() {
        RelOperator::EvalScalar(_) | RelOperator::Filter(_) | RelOperator::Window(_) => {
            find_group_by_keys(child.child(0)?, group_by_keys)?;
        }
        RelOperator::Aggregate(agg) => {
            for item in agg.group_items.iter() {
                if let ScalarExpr::BoundColumnRef(c) = &item.scalar {
                    group_by_keys.insert(c.column.index, c.column.data_type.clone());
                }
            }
        }
        RelOperator::Sort(_)
        | RelOperator::Limit(_)
        | RelOperator::Exchange(_)
        | RelOperator::UnionAll(_)
        | RelOperator::DummyTableScan(_)
        | RelOperator::ProjectSet(_)
        | RelOperator::ConstantTableScan(_)
        | RelOperator::ExpressionScan(_)
        | RelOperator::CacheScan(_)
        | RelOperator::Udf(_)
        | RelOperator::Scan(_)
        | RelOperator::AsyncFunction(_)
        | RelOperator::Join(_)
        | RelOperator::RecursiveCteScan(_)
        | RelOperator::Mutation(_)
        | RelOperator::MutationSource(_)
        | RelOperator::CompactBlock(_) => {}
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
