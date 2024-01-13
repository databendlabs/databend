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
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;

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
use crate::plans::ScalarExpr;

/// Fold simple `COUNT(*)` aggregate with statistics information.
pub struct RuleFoldCountAggregate {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RuleFoldCountAggregate {
    pub fn new() -> Self {
        Self {
            id: RuleID::FoldCountAggregate,
            //  Aggregate
            //  \
            //   *
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Aggregate,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_leaf(Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ))),
            )],
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
        let input_stat_info = rel_expr.derive_cardinality_child(0)?;

        let is_simple_count = agg.group_items.is_empty()
            && agg.aggregate_functions.iter().all(|agg| match &agg.scalar {
                ScalarExpr::AggregateFunction(agg_func) => {
                    agg_func.func_name == "count" && !agg_func.distinct
                }
                _ => false,
            });

        if let (true, column_stats, Some(table_card)) = (
            is_simple_count,
            &input_stat_info.statistics.column_stats,
            &input_stat_info.statistics.precise_cardinality,
        ) {
            let mut scalars = agg.aggregate_functions;
            for item in scalars.iter_mut() {
                if let ScalarExpr::AggregateFunction(agg_func) = item.scalar.clone() {
                    if agg_func.args.is_empty() {
                        item.scalar = ScalarExpr::ConstantExpr(ConstantExpr {
                            span: item.scalar.span(),
                            value: Scalar::Number(NumberScalar::UInt64(*table_card)),
                        });
                    } else if let ScalarExpr::BoundColumnRef(col) = &agg_func.args[0] {
                        if let Some(card) = column_stats.get(&col.column.index) {
                            item.scalar = ScalarExpr::ConstantExpr(ConstantExpr {
                                span: item.scalar.span(),
                                value: Scalar::Number(NumberScalar::UInt64(
                                    table_card - card.null_count,
                                )),
                            });
                        } else {
                            return Ok(());
                        }
                    } else {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }
            }
            let eval_scalar = EvalScalar { items: scalars };
            let dummy_table_scan = DummyTableScan;
            state.add_result(SExpr::create_unary(
                Arc::new(eval_scalar.into()),
                Arc::new(SExpr::create_leaf(Arc::new(dummy_table_scan.into()))),
            ));
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
