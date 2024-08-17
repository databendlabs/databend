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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::ConstantExpr;
use crate::plans::DummyTableScan;
use crate::plans::EvalScalar;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::MetadataRef;

// Replace aggregate function with scalar from table's accurate stats function
pub struct RuleStatsAggregateOptimizer {
    metadata: MetadataRef,
    ctx: Arc<dyn TableContext>,
}

impl RuleStatsAggregateOptimizer {
    pub fn new(ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        RuleStatsAggregateOptimizer { metadata, ctx }
    }

    #[async_recursion::async_recursion(#[recursive::recursive])]
    pub async fn run(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut children = Vec::with_capacity(s_expr.arity());
        for child in s_expr.children() {
            let child = self.run(child).await?;
            children.push(Arc::new(child));
        }
        let s_expr = s_expr.replace_children(children);
        if let RelOperator::Aggregate(_) = s_expr.plan.as_ref() {
            self.normalize_aggregate(&s_expr).await
        } else {
            Ok(s_expr)
        }
    }

    async fn normalize_aggregate(&self, s_expr: &SExpr) -> Result<SExpr> {
        let agg: Aggregate = s_expr.plan().clone().try_into()?;
        if s_expr.arity() != 1 || agg.grouping_sets.is_some() || !agg.group_items.is_empty() {
            return Ok(s_expr.clone());
        }

        // agg --> eval scalar --> scan
        let child = s_expr.child(0)?;
        if child.arity() != 1 || child.plan.as_ref().rel_op() != RelOp::EvalScalar {
            return Ok(s_expr.clone());
        }

        let child = child.child(0)?;
        if child.arity() != 0 {
            return Ok(s_expr.clone());
        }

        if let RelOperator::Scan(scan) = child.plan.as_ref() {
            if scan.prewhere.is_none() && scan.push_down_predicates.is_none() {
                let table = self.metadata.read().table(scan.table_index).table();
                let schema = table.schema();

                let mut results = Vec::with_capacity(agg.aggregate_functions.len());

                let mut column_ids = Vec::with_capacity(agg.aggregate_functions.len());
                let mut agg_functions = Vec::with_capacity(agg.aggregate_functions.len());

                for item in agg.aggregate_functions.iter() {
                    if let ScalarExpr::AggregateFunction(function) = &item.scalar {
                        if ["min", "max"].contains(&function.func_name.as_str())
                            && function.args.len() == 1
                            && !function.distinct
                        {
                            if let ScalarExpr::BoundColumnRef(b) = &function.args[0] {
                                if let Ok(col_id) =
                                    schema.column_id_of(b.column.column_name.as_str())
                                {
                                    column_ids.push(col_id);
                                    agg_functions.push(function.func_name.clone());
                                }
                            }
                        }
                    }
                }

                if column_ids.len() != agg.aggregate_functions.len() {
                    return Ok(s_expr.clone());
                }

                if let Some(stats) = table
                    .accurate_columns_ranges(self.ctx.clone(), &column_ids)
                    .await?
                {
                    for (i, (id, name)) in column_ids.iter().zip(agg_functions.iter()).enumerate() {
                        let index = agg.aggregate_functions[i].index;
                        if let Some(stat) = stats.get(id) {
                            if name.eq_ignore_ascii_case("min") && !stat.min.may_be_truncated {
                                results.push(ScalarItem {
                                    index,

                                    scalar: ScalarExpr::ConstantExpr(ConstantExpr {
                                        value: stat.min.value.clone(),
                                        span: None,
                                    }),
                                });
                            } else if !stat.max.may_be_truncated {
                                results.push(ScalarItem {
                                    index,
                                    scalar: ScalarExpr::ConstantExpr(ConstantExpr {
                                        value: stat.max.value.clone(),
                                        span: None,
                                    }),
                                });
                            }
                        }
                    }
                }
                if results.len() != agg.aggregate_functions.len() {
                    return Ok(s_expr.clone());
                }

                let eval_scalar = EvalScalar { items: results };
                let leaf = SExpr::create_leaf(Arc::new(DummyTableScan.into()));
                return Ok(SExpr::create_unary(
                    Arc::new(eval_scalar.into()),
                    Arc::new(leaf),
                ));
            }
        }
        Ok(s_expr.clone())
    }
}
