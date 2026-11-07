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
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_statistics::StatCount;

use crate::MetadataRef;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::StatContext;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Aggregate;
use crate::plans::ConstantExpr;
use crate::plans::DummyTableScan;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::Visitor;

/// Fold simple `COUNT(*)` aggregate with statistics information.
pub struct RuleFoldCountAggregate {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
    stat_context: StatContext,
}

impl RuleFoldCountAggregate {
    pub fn new(metadata: MetadataRef, stat_context: StatContext) -> Self {
        Self {
            id: RuleID::FoldCountAggregate,
            metadata,
            stat_context,
            //  Aggregate
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Aggregate,
                children: vec![Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RuleFoldCountAggregate {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let agg: Aggregate = s_expr.plan().clone().try_into()?;

        let rel_expr = RelExpr::with_s_expr(s_expr);
        let input_stat_info = rel_expr.derive_cardinality_child(0, &self.stat_context)?;

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
                let ScalarExpr::AggregateFunction(agg_func) = item.scalar.clone() else {
                    return Ok(());
                };
                if agg_func.args.is_empty() {
                    item.scalar = ScalarExpr::ConstantExpr(ConstantExpr {
                        span: item.scalar.span(),
                        value: Scalar::Number(NumberScalar::UInt64(*table_card)),
                    });
                } else if let ScalarExpr::BoundColumnRef(col) = &agg_func.args[0]
                    && let Some(card) = column_stats.get(&col.column.index)
                    && let StatCount::Exact(null_count) = card.null_count()
                    && let Some(card) = table_card.checked_sub(null_count)
                {
                    item.scalar = ScalarExpr::ConstantExpr(ConstantExpr {
                        span: item.scalar.span(),
                        value: Scalar::Number(NumberScalar::UInt64(card)),
                    });
                } else {
                    return Ok(());
                }
            }
            if contains_non_deterministic_function(s_expr.child(0)?)? {
                // Statistics are derived with the current query's function context, so a count
                // computed from them over e.g. `if(to_second(now()) % 2 = 0, 1, NULL)` is valid
                // only for this query.
                self.metadata.write().set_holds_query_specific_value();
            }
            let eval_scalar = EvalScalar { items: scalars };
            // Collect source table indexes from the original plan for cache invalidation
            let source_table_indexes = DummyTableScan::collect_source_tables(s_expr);
            let dummy_table_scan = DummyTableScan::with_source_tables(source_table_indexes);
            state.add_result(SExpr::create_unary(
                Arc::new(eval_scalar.into()),
                Arc::new(SExpr::create_leaf(Arc::new(dummy_table_scan.into()))),
            ));
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

/// Whether any operator in `s_expr` evaluates a non-deterministic function such as `now()`.
fn contains_non_deterministic_function(s_expr: &SExpr) -> Result<bool> {
    struct Finder {
        found: bool,
    }

    impl<'a> Visitor<'a> for Finder {
        fn visit_function_call(&mut self, func: &'a FunctionCall) -> Result<()> {
            if BUILTIN_FUNCTIONS
                .get_property(&func.func_name)
                .is_some_and(|property| property.non_deterministic)
            {
                self.found = true;
                return Ok(());
            }
            for arg in &func.arguments {
                self.visit(arg)?;
            }
            Ok(())
        }
    }

    let mut finder = Finder { found: false };
    for scalar in s_expr.plan().scalar_expr_iter() {
        finder.visit(scalar)?;
        if finder.found {
            return Ok(true);
        }
    }
    for child in s_expr.children() {
        if contains_non_deterministic_function(child)? {
            return Ok(true);
        }
    }
    Ok(false)
}
