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

use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::NumberScalar;

use crate::IndexType;
use crate::ScalarExpr;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::MaterializedCTE;
use crate::plans::MaterializedCTERef;
use crate::plans::RelOp;
use crate::plans::Sequence;
use crate::plans::UnionAll;
use crate::plans::VisitorMut;
use crate::plans::walk_expr_mut;

const ID: RuleID = RuleID::GroupingSetsToUnion;
// Split `Grouping Sets` into `Union All` of `Group by`
// Eg:
// select number % 10 AS a, number % 3 AS b, number % 4 AS c
// from numbers(100000000)
// group by grouping sets((a,b),(a,c));

// INTO:

// select number % 10 AS a, number % 3 AS b, null AS c
// from numbers(100000000)
// group by a,b
// union all
// select number % 10 AS a, null AS b, number % 4 AS c
// from numbers(100000000)
// group by a,c
//
pub struct RuleGroupingSetsToUnion {
    id: RuleID,
    matchers: Vec<Matcher>,
    ctx: Arc<OptimizerContext>,
}

impl RuleGroupingSetsToUnion {
    pub fn new(ctx: Arc<OptimizerContext>) -> Self {
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
            ctx,
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
        let agg_input_columns: Vec<IndexType> = RelExpr::with_s_expr(agg_input)
            .derive_relational_prop()?
            .output_columns
            .iter()
            .cloned()
            .collect();

        let column_mapping = agg_input_columns
            .iter()
            .map(|index| (*index, *index))
            .collect();

        if let Some(grouping_sets) = &agg.grouping_sets {
            if !grouping_sets.sets.is_empty() {
                let mut children = Vec::with_capacity(grouping_sets.sets.len());

                let mut hasher = DefaultHasher::new();
                agg.grouping_sets.hash(&mut hasher);
                let hash = hasher.finish();
                let temp_cte_name = format!("cte_groupingsets_{hash}");

                let channel_size = self
                    .ctx
                    .get_table_ctx()
                    .get_settings()
                    .get_grouping_sets_channel_size()
                    .unwrap_or(2);

                let cte_materialized_sexpr = SExpr::create_unary(
                    MaterializedCTE::new(temp_cte_name.clone(), None, Some(channel_size as usize)),
                    agg_input.clone(),
                );

                let cte_consumer = SExpr::create_leaf(MaterializedCTERef {
                    cte_name: temp_cte_name,
                    output_columns: agg_input_columns.clone(),
                    def: agg_input.clone(),
                    column_mapping,
                });

                let mask = (1 << grouping_sets.dup_group_items.len()) - 1;
                let group_bys = agg
                    .group_items
                    .iter()
                    .map(|i| {
                        agg_input_columns
                            .iter()
                            .position(|t| *t == i.index)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();

                for set in &grouping_sets.sets {
                    let mut id = 0;

                    // For element in `group_bys`,
                    // if it is in current grouping set: set 0, else: set 1. (1 represents it will be NULL in grouping)
                    // Example: GROUP BY GROUPING SETS ((a, b), (a), (b), ())
                    // group_bys: [a, b]
                    // grouping_sets: [[0, 1], [0], [1], []]
                    // grouping_ids: 00, 01, 10, 11

                    for g in set {
                        let i = group_bys.iter().position(|t| *t == *g).unwrap();
                        id |= 1 << i;
                    }
                    let grouping_id = !id & mask;

                    let mut eval_scalar = eval_scalar.clone();
                    let mut agg = agg.clone();
                    agg.grouping_sets = None;

                    let null_group_ids: Vec<IndexType> = agg
                        .group_items
                        .iter()
                        .map(|i| i.index)
                        .filter(|index| !set.contains(index))
                        .clone()
                        .collect();

                    agg.group_items.retain(|x| set.contains(&x.index));
                    let group_ids: Vec<IndexType> =
                        agg.group_items.iter().map(|i| i.index).collect();

                    let mut visitor = ReplaceColumnForGroupingSetsVisitor {
                        group_indexes: group_ids,
                        exclude_group_indexes: null_group_ids,
                        grouping_id_index: grouping_sets.grouping_id_index,
                        grouping_id_value: grouping_id,
                    };

                    for scalar in eval_scalar.items.iter_mut() {
                        visitor.visit(&mut scalar.scalar)?;
                    }

                    let agg_plan = SExpr::create_unary(agg, cte_consumer.clone());
                    let eval_plan = SExpr::create_unary(eval_scalar, agg_plan);
                    children.push(eval_plan);
                }

                // fold children into result
                let mut result = children.first().unwrap().clone();
                for other in children.into_iter().skip(1) {
                    let left_outputs: Vec<(IndexType, Option<ScalarExpr>)> =
                        eval_scalar.items.iter().map(|x| (x.index, None)).collect();
                    let right_outputs = left_outputs.clone();

                    let union_plan = UnionAll {
                        left_outputs,
                        right_outputs,
                        cte_scan_names: vec![],
                        output_indexes: eval_scalar.items.iter().map(|x| x.index).collect(),
                    };
                    result = SExpr::create_binary(Arc::new(union_plan.into()), result, other);
                }
                result = SExpr::create_binary(Sequence, cte_materialized_sexpr, result);
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

struct ReplaceColumnForGroupingSetsVisitor {
    group_indexes: Vec<IndexType>,
    exclude_group_indexes: Vec<IndexType>,
    grouping_id_index: IndexType,
    grouping_id_value: u32,
}

impl VisitorMut<'_> for ReplaceColumnForGroupingSetsVisitor {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        let old = expr.clone();

        if let ScalarExpr::BoundColumnRef(col) = expr {
            if self.group_indexes.contains(&col.column.index) {
                *expr = ScalarExpr::CastExpr(CastExpr {
                    argument: Box::new(old),
                    is_try: true,
                    target_type: Box::new(col.column.data_type.wrap_nullable()),
                    span: col.span,
                });
            } else if self.exclude_group_indexes.contains(&col.column.index) {
                *expr = ScalarExpr::TypedConstantExpr(
                    ConstantExpr {
                        value: Scalar::Null,
                        span: col.span,
                    },
                    col.column.data_type.wrap_nullable(),
                );
            } else if self.grouping_id_index == col.column.index {
                *expr = ScalarExpr::ConstantExpr(ConstantExpr {
                    value: Scalar::Number(NumberScalar::UInt32(self.grouping_id_value)),
                    span: col.span,
                });
            }
            return Ok(());
        }
        walk_expr_mut(self, expr)
    }
}
