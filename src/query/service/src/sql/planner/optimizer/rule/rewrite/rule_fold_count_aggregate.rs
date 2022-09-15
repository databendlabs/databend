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

use common_datavalues::DataValue;
use common_exception::Result;

use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::RuleID;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Aggregate;
use crate::sql::plans::AggregateMode;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::DummyTableScan;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;
use crate::sql::ScalarExpr;

/// Fold simple `COUNT(*)` aggregate with statistics information.
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

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        let agg: Aggregate = s_expr.plan().clone().try_into()?;

        if agg.mode == AggregateMode::Final || agg.mode == AggregateMode::Partial {
            return Ok(());
        }

        let rel_expr = RelExpr::with_s_expr(s_expr);
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        let is_simple_count = agg.group_items.is_empty()
            && agg.aggregate_functions.iter().all(|agg| match &agg.scalar {
                Scalar::AggregateFunction(agg_func) => {
                    agg_func.func_name == "count" && agg_func.args.is_empty() && !agg_func.distinct
                }
                _ => false,
            });

        if let (true, Some(card)) = (is_simple_count, input_prop.precise_cardinality) {
            let mut scalars = agg.aggregate_functions;
            for item in scalars.iter_mut() {
                item.scalar = Scalar::ConstantExpr(ConstantExpr {
                    value: DataValue::UInt64(card),
                    data_type: Box::new(item.scalar.data_type()),
                });
            }
            let eval_scalar = EvalScalar { items: scalars };
            let dummy_table_scan = DummyTableScan;
            state.add_result(SExpr::create_unary(
                eval_scalar.into(),
                SExpr::create_leaf(dummy_table_scan.into()),
            ));
        }

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
