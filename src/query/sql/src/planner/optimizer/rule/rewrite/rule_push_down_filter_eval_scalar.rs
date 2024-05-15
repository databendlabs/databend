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

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::walk_expr_mut;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;

pub struct RulePushDownFilterEvalScalar {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownFilterEvalScalar {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterEvalScalar,
            // Filter
            //  \
            //   EvalScalar
            //    \
            //     *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::Leaf],
                }],
            }],
        }
    }

    // Replace predicate with children scalar items
    fn replace_predicate(predicate: &ScalarExpr, items: &[ScalarItem]) -> Result<ScalarExpr> {
        struct PredicateVisitor<'a> {
            items: &'a [ScalarItem],
        }
        impl<'a> VisitorMut<'a> for PredicateVisitor<'a> {
            fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
                if let ScalarExpr::BoundColumnRef(column) = expr {
                    for item in self.items {
                        if item.index == column.column.index {
                            *expr = item.scalar.clone();
                            return Ok(());
                        }
                    }
                    return Ok(());
                };
                walk_expr_mut(self, expr)
            }
        }

        let mut visitor = PredicateVisitor { items };
        let mut predicate = predicate.clone();
        visitor.visit(&mut predicate)?;
        Ok(predicate.clone())
    }
}

impl Rule for RulePushDownFilterEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let eval_scalar: EvalScalar = s_expr.child(0)?.plan().clone().try_into()?;
        let scalar_rel_expr = RelExpr::with_s_expr(s_expr);
        let eval_scalar_prop = scalar_rel_expr.derive_relational_prop_child(0)?;

        let mut remaining_predicates = vec![];
        let mut pushed_down_predicates = vec![];

        for pred in filter.predicates.iter() {
            if pred
                .used_columns()
                .is_subset(&eval_scalar_prop.output_columns)
            {
                // Replace `BoundColumnRef` with the column expression introduced in `EvalScalar`.
                let rewritten_predicate = Self::replace_predicate(pred, &eval_scalar.items)?;
                pushed_down_predicates.push(rewritten_predicate);
            } else {
                remaining_predicates.push(pred.clone());
            }
        }

        let mut result = s_expr.child(0)?.child(0)?.clone();

        if !pushed_down_predicates.is_empty() {
            let pushed_down_filter = Filter {
                predicates: pushed_down_predicates,
            };
            result = SExpr::create_unary(Arc::new(pushed_down_filter.into()), Arc::new(result));
        }

        result = SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(result));

        if !remaining_predicates.is_empty() {
            let remaining_filter = Filter {
                predicates: remaining_predicates,
            };
            result = SExpr::create_unary(Arc::new(remaining_filter.into()), Arc::new(result));
            result.set_applied_rule(&self.id);
        }

        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
