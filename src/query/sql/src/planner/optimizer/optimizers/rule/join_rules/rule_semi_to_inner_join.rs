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
use databend_common_expression::types::DataType;

use crate::ScalarExpr;
use crate::Symbol;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;

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

        if join.equi_conditions.is_empty() {
            return Ok(());
        }

        let build_join_keys = join.equi_conditions.iter().map(|condition| {
            if join.join_type == JoinType::LeftSemi {
                &condition.right
            } else {
                &condition.left
            }
        });

        let mut build_join_key_cols = HashSet::with_capacity(join.equi_conditions.len());
        for key in build_join_keys {
            if let Some(column) = join_key_column(key) {
                build_join_key_cols.insert(column);
            }
        }

        let child = if join.join_type == JoinType::LeftSemi {
            s_expr.child(1)?
        } else {
            s_expr.child(0)?
        };

        // If group by keys are all condition keys and not nullable
        // we can rewrite semi join to inner join
        // inner join will ignore null values but semi join will keep them
        // this happens in Q38
        let mut has_group_by_keys = false;
        let mut can_rewrite = true;
        visit_group_by_keys(child, &mut |key, data_type| {
            has_group_by_keys = true;
            if !build_join_key_cols.contains(&key) || data_type.is_nullable_or_null() {
                can_rewrite = false;
            }
        })?;

        if has_group_by_keys && can_rewrite {
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

fn visit_group_by_keys(child: &SExpr, visitor: &mut impl FnMut(Symbol, DataType)) -> Result<()> {
    match child.plan() {
        RelOperator::EvalScalar(_)
        | RelOperator::Filter(_)
        | RelOperator::Window(_)
        | RelOperator::WindowGroup(_) => {
            visit_group_by_keys(child.child(0)?, visitor)?;
        }
        RelOperator::Aggregate(agg) => {
            for item in agg.group_items.iter() {
                visitor(item.index, item.scalar.data_type()?);
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
        | RelOperator::MaterializedCTE(_)
        | RelOperator::MaterializedCTERef(_)
        | RelOperator::CompactBlock(_)
        | RelOperator::Sequence(_) => {}
    }
    Ok(())
}

fn join_key_column(condition: &ScalarExpr) -> Option<Symbol> {
    match condition {
        ScalarExpr::BoundColumnRef(c) => Some(c.column.index),
        ScalarExpr::CastExpr(expr) if !expr.is_try => join_key_column(&expr.argument),
        _ => None,
    }
}

impl Default for RuleSemiToInnerJoin {
    fn default() -> Self {
        Self::new()
    }
}
