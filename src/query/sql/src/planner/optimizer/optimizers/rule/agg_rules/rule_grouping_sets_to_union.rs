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
use databend_common_expression::Scalar;

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::walk_expr_mut;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::RelOp;
use crate::plans::UnionAll;
use crate::plans::VisitorMut;
use crate::IndexType;
use crate::ScalarExpr;

// TODO
const ID: RuleID = RuleID::RuleGroupingSetsToUnion;
// Split `Grouping Sets` into `Union All` of `Group by`
// Eg:
// select number % 10 AS a, number % 3 AS b, number % 4 AS c
// from numbers(100000000)
// group by grouping sets((a,b),(a,c));

// INTO:

// select number % 10 AS a, number % 3 AS b, number % 4 AS c
// from numbers(100000000)
// group by a,b
// union all
// select number % 10 AS a, number % 3 AS b, number % 4 AS c
// from numbers(100000000)
// group by a,c
//
pub struct RuleGroupingSetsToUnion {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleGroupingSetsToUnion {
    pub fn new() -> Self {
        Self {
            id: ID,
            //  Aggregate
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::Leaf],
                }],
            }],
        }
    }
}

// Must go before `RuleSplitAggregate`
impl Rule for RuleGroupingSetsToUnion {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let eval_scalar: EvalScalar = s_expr.plan().clone().try_into()?;
        let agg: Aggregate = s_expr.child(0)?.plan().clone().try_into()?;
        if agg.mode != AggregateMode::Initial {
            return Ok(());
        }

        let agg_input = s_expr.child(0)?.child(0)?;

        if let Some(grouping_sets) = &agg.grouping_sets {
            if grouping_sets.sets.len() > 1 {
                let mut children = Vec::with_capacity(grouping_sets.sets.len());
                for set in &grouping_sets.sets {
                    let mut eval_scalar = eval_scalar.clone();
                    let mut agg = agg.clone();
                    agg.grouping_sets = None;

                    let null_group_ids: Vec<IndexType> = agg
                        .group_items
                        .iter()
                        .map(|i| i.index)
                        .filter(|index| !set.contains(&index))
                        .clone()
                        .collect();

                    agg.group_items.retain(|x| set.contains(&x.index));

                    let group_ids: Vec<IndexType> =
                        agg.group_items.iter().map(|i| i.index).collect();

                    let mut none_match_visitor = ReplaceColumnBeBullVisitor {
                        indexes: null_group_ids,
                        kind: SetKind::Null,
                    };

                    let mut match_visitor = ReplaceColumnBeBullVisitor {
                        indexes: group_ids,
                        kind: SetKind::Nullable,
                    };

                    // Replace the index to be null in aggregate functions
                    for func in agg.aggregate_functions.iter_mut() {
                        if let ScalarExpr::AggregateFunction(func) = &mut func.scalar {
                            for arg in func.args.iter_mut() {
                                none_match_visitor.visit(arg)?;
                            }

                            for arg in func.sort_descs.iter_mut() {
                                none_match_visitor.visit(&mut arg.expr)?;
                            }
                        }
                    }

                    for scalar in eval_scalar.items.iter_mut() {
                        none_match_visitor.visit(&mut scalar.scalar)?;
                        match_visitor.visit(&mut scalar.scalar)?;
                    }

                    let agg_plan = SExpr::create_unary(agg, agg_input.clone());
                    let eval_plan = SExpr::create_unary(eval_scalar, agg_plan);
                    children.push(eval_plan);
                }

                // fold children into result
                let mut result = children.first().unwrap().clone();
                for other in children.into_iter().skip(1) {
                    let union_plan = UnionAll {
                        left_outputs: eval_scalar.items.iter().map(|x| (x.index, None)).collect(),
                        right_outputs: eval_scalar.items.iter().map(|x| (x.index, None)).collect(),
                        cte_scan_names: vec![],
                        output_indexes: eval_scalar.items.iter().map(|x| x.index).collect(),
                    };
                    result = SExpr::create_binary(Arc::new(union_plan.into()), result, other);
                }
                state.add_result(result);
                return Ok(());
            }
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RuleGroupingSetsToUnion {
    fn default() -> Self {
        Self::new()
    }
}

enum SetKind {
    Null,
    Nullable,
}

struct ReplaceColumnBeBullVisitor {
    indexes: Vec<IndexType>,
    kind: SetKind,
}

impl VisitorMut<'_> for ReplaceColumnBeBullVisitor {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        let old = expr.clone();

        if let ScalarExpr::BoundColumnRef(col) = expr {
            if self.indexes.contains(&col.column.index) {
                match self.kind {
                    SetKind::Null => {
                        *expr = ScalarExpr::TypedConstantExpr(
                            ConstantExpr {
                                value: Scalar::Null,
                                span: col.span,
                            },
                            col.column.data_type.wrap_nullable(),
                        );
                    }
                    SetKind::Nullable => {
                        *expr = ScalarExpr::CastExpr(CastExpr {
                            argument: Box::new(old),
                            is_try: true,
                            target_type: Box::new(col.column.data_type.wrap_nullable()),
                            span: col.span,
                        });
                    }
                }
            }
            return Ok(());
        }
        walk_expr_mut(self, expr)
    }
}
