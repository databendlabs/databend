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

use std::sync::Arc;

use databend_common_exception::Result;

use crate::binder::JoinPredicate;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::ColumnSet;

/// Push `Left/Right Semi|Anti` join closer to the base table that participates
/// in the predicate so that fewer rows stay in the join tree.
pub struct RulePushdownAntiJoin {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushdownAntiJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownAntiJoin,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
        }
    }

    fn try_push_down(&self, left: &SExpr, right: &SExpr, join: Join) -> Result<Option<SExpr>> {
        let right_rel_expr = RelExpr::with_s_expr(right);

        if let Some(inner_join) = extract_inner_join(left)? {
            let inner_join_rel_expr = RelExpr::with_s_expr(&inner_join);
            let inner_join_left_prop = inner_join_rel_expr.derive_relational_prop_child(0)?;
            let inner_join_right_prop = inner_join_rel_expr.derive_relational_prop_child(1)?;

            let equi_conditions = join
                .equi_conditions
                .iter()
                .map(|condition| {
                    JoinPredicate::new(
                        &condition.left,
                        &inner_join_left_prop,
                        &inner_join_right_prop,
                    )
                })
                .collect::<Vec<_>>();

            if equi_conditions.iter().all(left_predicate) {
                let right_prop = right_rel_expr.derive_relational_prop()?;
                let mut union_output_columns = ColumnSet::new();
                union_output_columns.extend(right_prop.output_columns.clone());
                union_output_columns.extend(inner_join_left_prop.output_columns.clone());

                if join
                    .non_equi_conditions
                    .iter()
                    .all(|x| x.used_columns().is_subset(&union_output_columns))
                {
                    let new_inner_join = inner_join.replace_children([
                        Arc::new(SExpr::create_binary(
                            RelOperator::Join(join.clone()),
                            inner_join.child(0)?.clone(),
                            right.clone(),
                        )),
                        Arc::new(inner_join.child(1)?.clone()),
                    ]);

                    return replace_inner_join(left, new_inner_join);
                }
            } else if equi_conditions.iter().all(right_predicate) {
                let right_prop = right_rel_expr.derive_relational_prop()?;
                let mut union_output_columns = ColumnSet::new();
                union_output_columns.extend(right_prop.output_columns.clone());
                union_output_columns.extend(inner_join_right_prop.output_columns.clone());

                if join
                    .non_equi_conditions
                    .iter()
                    .all(|x| x.used_columns().is_subset(&union_output_columns))
                {
                    let new_inner_join = inner_join.replace_children([
                        Arc::new(inner_join.child(0)?.clone()),
                        Arc::new(SExpr::create_binary(
                            RelOperator::Join(join.clone()),
                            inner_join.child(1)?.clone(),
                            right.clone(),
                        )),
                    ]);

                    return replace_inner_join(left, new_inner_join);
                }
            }
        }

        Ok(None)
    }
}

impl Rule for RulePushdownAntiJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let join: Join = s_expr.plan().clone().try_into()?;

        if matches!(join.join_type, JoinType::LeftAnti | JoinType::LeftSemi) {
            if let Some(mut result) =
                self.try_push_down(s_expr.child(0)?, s_expr.child(1)?, join)?
            {
                result.set_applied_rule(&self.id);
                state.add_result(result);
            }
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushdownAntiJoin {
    fn default() -> Self {
        Self::new()
    }
}

fn replace_inner_join(expr: &SExpr, new_join: SExpr) -> Result<Option<SExpr>> {
    match expr.plan() {
        RelOperator::Join(join) if join.join_type == JoinType::Inner => Ok(Some(new_join)),
        RelOperator::Filter(_) => match replace_inner_join(expr.child(0)?, new_join)? {
            None => Ok(None),
            Some(new_child) => Ok(Some(expr.replace_children([Arc::new(new_child)]))),
        },
        _ => Ok(None),
    }
}

fn extract_inner_join(expr: &SExpr) -> Result<Option<SExpr>> {
    match expr.plan() {
        RelOperator::Join(join) if join.join_type == JoinType::Inner => Ok(Some(expr.clone())),
        RelOperator::Filter(_) => extract_inner_join(expr.child(0)?),
        _ => Ok(None),
    }
}

fn left_predicate(tuple: &JoinPredicate) -> bool {
    matches!(&tuple, JoinPredicate::Left(_))
}

fn right_predicate(tuple: &JoinPredicate) -> bool {
    matches!(&tuple, JoinPredicate::Right(_))
}
