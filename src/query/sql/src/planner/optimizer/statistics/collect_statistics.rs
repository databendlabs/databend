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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::types::F64;
use databend_common_expression::types::NumberScalar;

use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::Statistics;

// The CollectStatisticsOptimizer will collect statistics for each leaf node in SExpr.
pub struct CollectStatisticsOptimizer {
    table_ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
}

impl CollectStatisticsOptimizer {
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        CollectStatisticsOptimizer {
            table_ctx: opt_ctx.get_table_ctx(),
            metadata: opt_ctx.get_metadata(),
        }
    }

    pub async fn optimize_async(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.collect(s_expr).await
    }

    #[async_recursion::async_recursion(#[recursive::recursive])]
    pub async fn collect(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        match s_expr.plan.as_ref() {
            RelOperator::Scan(scan) => {
                let table = self.metadata.read().table(scan.table_index).clone();
                let table = table.table();
                let columns = self
                    .metadata
                    .read()
                    .columns_by_table_index(scan.table_index);

                let column_statistics_provider = table
                    .column_statistics_provider(self.table_ctx.clone())
                    .await?;
                let table_stats = table
                    .table_statistics(self.table_ctx.clone(), true, scan.change_type.clone())
                    .await?;

                let mut column_stats = HashMap::new();
                let mut histograms = HashMap::new();
                for column in columns.iter() {
                    if let ColumnEntry::BaseTableColumn(BaseTableColumn {
                        column_index,
                        column_id,
                        virtual_expr,
                        ..
                    }) = column
                    {
                        if virtual_expr.is_none() {
                            let col_stat = column_statistics_provider
                                .column_statistics(*column_id as ColumnId);
                            column_stats.insert(*column_index, col_stat.cloned());
                            let histogram =
                                column_statistics_provider.histogram(*column_id as ColumnId);
                            histograms.insert(*column_index, histogram);
                        }
                    }
                }

                let mut scan = scan.clone();
                scan.statistics = Arc::new(Statistics {
                    table_stats,
                    column_stats,
                    histograms,
                });
                let mut s_expr = s_expr.replace_plan(Arc::new(RelOperator::Scan(scan.clone())));
                if let Some(sample) = &scan.sample {
                    // Only process row-level sampling in optimizer phase.
                    if let Some(row_level) = &sample.row_level {
                        if let Some(stats) = &table_stats
                            && let Some(probability) =
                                row_level.sample_probability(stats.num_rows)?
                        {
                            let rand_expr = ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "rand".to_string(),
                                params: vec![],
                                arguments: vec![],
                            });
                            let filter = ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "lte".to_string(),
                                params: vec![],
                                arguments: vec![
                                    rand_expr,
                                    ScalarExpr::ConstantExpr(ConstantExpr {
                                        span: None,
                                        value: Scalar::Number(NumberScalar::Float64(F64::from(
                                            probability,
                                        ))),
                                    }),
                                ],
                            });
                            s_expr = SExpr::create_unary(
                                Arc::new(
                                    Filter {
                                        predicates: vec![filter],
                                    }
                                    .into(),
                                ),
                                Arc::new(s_expr),
                            );
                        }
                    }
                }
                Ok(s_expr)
            }
            RelOperator::MaterializedCTERef(cte_ref) => {
                let def_with_stats = self.collect(&cte_ref.def).await?;
                let mut new_cte_ref = cte_ref.clone();
                new_cte_ref.def = def_with_stats;

                Ok(s_expr.replace_plan(Arc::new(RelOperator::MaterializedCTERef(new_cte_ref))))
            }
            _ => {
                let mut children = Vec::with_capacity(s_expr.arity());
                for child in s_expr.children() {
                    let child = Box::pin(self.collect(child)).await?;
                    children.push(Arc::new(child));
                }
                Ok(s_expr.replace_children(children))
            }
        }
    }
}

#[async_trait::async_trait]
impl Optimizer for CollectStatisticsOptimizer {
    fn name(&self) -> String {
        "CollectStatisticsOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_async(s_expr).await
    }
}
