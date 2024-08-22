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

use databend_common_ast::ast::SampleLevel;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::F64;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use log::info;

use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::optimizer::StatInfo;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::Statistics;
use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

// The CollectStatisticsOptimizer will collect statistics for each leaf node in SExpr.
pub struct CollectStatisticsOptimizer {
    table_ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    cte_statistics: HashMap<IndexType, Arc<StatInfo>>,
}

impl CollectStatisticsOptimizer {
    pub fn new(table_ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        CollectStatisticsOptimizer {
            table_ctx,
            metadata,
            cte_statistics: HashMap::new(),
        }
    }

    pub async fn run(mut self, s_expr: &SExpr) -> Result<SExpr> {
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
                        path_indices,
                        leaf_index,
                        virtual_computed_expr,
                        ..
                    }) = column
                    {
                        if path_indices.is_none() && virtual_computed_expr.is_none() {
                            if let Some(col_id) = *leaf_index {
                                let col_stat = column_statistics_provider
                                    .column_statistics(col_id as ColumnId);
                                if col_stat.is_none() {
                                    info!("column {} doesn't have global statistics", col_id);
                                }
                                column_stats.insert(*column_index, col_stat.cloned());
                                let histogram =
                                    column_statistics_provider.histogram(col_id as ColumnId);
                                histograms.insert(*column_index, histogram);
                            }
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
                    match sample.sample_level {
                        SampleLevel::ROW => {
                            if let Some(stats) = &table_stats
                                && let Some(probability) = sample.sample_probability(stats.num_rows)
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
                                            value: Scalar::Number(NumberScalar::Float64(
                                                F64::from(probability),
                                            )),
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
                        SampleLevel::BLOCK => {}
                    }
                }
                Ok(s_expr)
            }
            RelOperator::MaterializedCte(materialized_cte) => {
                // Collect the common table expression statistics first.
                let left = Box::pin(self.collect(s_expr.child(0)?)).await?;
                let cte_stat_info = RelExpr::with_s_expr(&left).derive_cardinality_child(0)?;
                self.cte_statistics
                    .insert(materialized_cte.cte_idx, cte_stat_info);
                let right = Box::pin(self.collect(s_expr.child(1)?)).await?;
                Ok(s_expr.replace_children(vec![Arc::new(left), Arc::new(right)]))
            }
            RelOperator::CteScan(cte_scan) => {
                let stat_info = self
                    .cte_statistics
                    .get(&cte_scan.cte_idx.0)
                    .unwrap()
                    .clone();
                let mut cte_scan = cte_scan.clone();
                cte_scan.stat = stat_info;
                Ok(s_expr.replace_plan(Arc::new(RelOperator::CteScan(cte_scan))))
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
