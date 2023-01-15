// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Literal;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::ConstantExpr;
use crate::plans::DummyTableScan;
use crate::plans::EvalScalar;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::Scalar;
use crate::ScalarExpr;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;

/// Rewrite `COUNT_DISTINCT(*)` aggregator to GROUP BY.
pub struct RuleFoldCountAggregate {
    id: RuleID,
    pattern: SExpr,
}

impl RuleFoldCountAggregate {
    pub fn new() -> Self {
        Self {
            id: RuleID::FoldCountAggregate,
            //  Aggregate
            //  \
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Aggregate,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ),
            ),
        }
    }
}

impl Rule for RuleFoldCountAggregate {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let agg: Aggregate = s_expr.plan().clone().try_into()?;

        if agg.mode == AggregateMode::Final || agg.mode == AggregateMode::Partial {
            return Ok(());
        }

        let rel_expr = RelExpr::with_s_expr(s_expr);
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        let is_simple_distinct = agg.group_items.is_empty()
            && agg.aggregate_functions.len() == 1 && agg.aggregate_functions.iter().all(|agg| match &agg.scalar {
                Scalar::AggregateFunction(agg_func) => {
                    agg_func.func_name == "count" && agg_func.distinct
                }
                _ => false,
            });
        
        if !is_simple_distinct {
            return Ok(());
        }
        
        let mut scalar = &agg.aggregate_functions[0];
        match scalar.scalar {
            Scalar::AggregateFunction(agg_func) => {
                // agg_func.args
               let sub_query = Scalar::SubqueryExpr(SubqueryExpr {
                    typ: SubqueryType::Scalar,
                    subquery: todo!(),
                    child_expr: None,
                    compare_op: None,
                    output_column: todo!(),
                    projection_index: todo!(),
                    data_type: todo!(),
                    allow_multi_rows: todo!(),
                    outer_columns: todo!(),
                })
            }
            _ => unreachable!()
        }
        let dummy_table_scan = DummyTableScan;
        state.add_result(SExpr::create_unary(
            eval_scalar.into(),
            SExpr::create_leaf(dummy_table_scan.into()),
        ));
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
