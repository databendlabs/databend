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

use common_datavalues::DataTypeImpl::Null;
use common_datavalues::DataTypeImpl::Nullable;
use common_datavalues::DataValue;
use common_exception::Result;

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

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let agg: Aggregate = s_expr.plan().clone().try_into()?;

        if agg.mode == AggregateMode::Final || agg.mode == AggregateMode::Partial {
            return Ok(());
        }

        let rel_expr = RelExpr::with_s_expr(s_expr);
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        let is_simple_count = agg.group_items.is_empty()
            && agg.aggregate_functions.iter().all(|agg| match &agg.scalar {
                Scalar::AggregateFunction(agg_func) => {
                    agg_func.func_name == "count"
                        && (agg_func.args.is_empty()
                            || !matches!(agg_func.args[0].data_type(), Nullable(_) | Null(_)))
                        && !agg_func.distinct
                }
                _ => false,
            });

        let simple_nullable_count = agg.group_items.is_empty()
            && agg.aggregate_functions.iter().all(|agg| match &agg.scalar {
                Scalar::AggregateFunction(agg_func) => {
                    agg_func.func_name == "count"
                        && (agg_func.args.is_empty()
                            || matches!(agg_func.args[0].data_type(), Nullable(_)))
                        && !agg_func.distinct
                }
                _ => false,
            });

        if let (true, Some(card)) = (is_simple_count, input_prop.statistics.precise_cardinality) {
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
        } else if let (true, true, column_stats, Some(table_card)) = (
            simple_nullable_count,
            input_prop.statistics.is_accurate,
            input_prop.statistics.column_stats,
            input_prop.statistics.precise_cardinality,
        ) {
            let mut scalars = agg.aggregate_functions;
            for item in scalars.iter_mut() {
                if let Scalar::AggregateFunction(agg_func) = item.scalar.clone() {
                    let col_set = agg_func.args[0].used_columns();
                    for index in col_set {
                        let col_stat = column_stats.get(&index);
                        if let Some(card) = col_stat {
                            item.scalar = Scalar::ConstantExpr(ConstantExpr {
                                value: DataValue::UInt64(table_card - card.null_count),
                                data_type: Box::new(item.scalar.data_type()),
                            });
                        } else {
                            return Ok(());
                        }
                    }
                }
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
