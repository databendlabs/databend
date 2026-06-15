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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::types::F64;
use databend_common_expression::types::NumberScalar;
use databend_common_statistics::Histogram;
use databend_storages_common_table_meta::meta::LegacyHistogram;

use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Symbol;
use crate::TableEntry;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::Scan;
use crate::plans::Statistics;

// The CollectStatisticsOptimizer will collect statistics for each leaf node in SExpr.
pub struct CollectStatisticsOptimizer {
    table_ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    enable_trace: bool,
}

impl CollectStatisticsOptimizer {
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        CollectStatisticsOptimizer {
            table_ctx: opt_ctx.get_table_ctx(),
            metadata: opt_ctx.get_metadata(),
            enable_trace: log::log_enabled!(log::Level::Debug),
        }
    }

    async fn optimize_async(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut statistics_trace = StatisticsTrace::default();
        let s_expr = self.collect(s_expr, &mut statistics_trace).await?;
        if self.enable_trace && !statistics_trace.is_empty() {
            let StatisticsTrace {
                table_stats,
                column_stats,
                scan_mappings,
            } = statistics_trace;
            log::debug!(
                table_stats:serde,
                column_stats:serde,
                scan_mappings:serde; "optimizer statistics trace");
        }
        Ok(s_expr)
    }

    #[async_recursion::async_recursion(#[recursive::recursive])]
    async fn collect(
        &mut self,
        s_expr: &SExpr,
        statistics_trace: &mut StatisticsTrace,
    ) -> Result<SExpr> {
        match s_expr.plan.as_ref() {
            RelOperator::Scan(scan) => {
                let (table_entry, columns) = {
                    let metadata = self.metadata.read();
                    (
                        metadata.table(scan.table_index).clone(),
                        metadata.columns_by_table_index(scan.table_index),
                    )
                };
                let table = table_entry.table();

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

                if self.enable_trace {
                    statistics_trace.record_scan(
                        scan,
                        &table_entry,
                        &columns,
                        &table_stats,
                        &column_stats,
                        &histograms,
                    );
                }

                let mut scan = scan.clone();
                scan.statistics = Arc::new(Statistics {
                    table_stats,
                    column_stats,
                    histograms,
                });
                let mut s_expr = s_expr.replace_plan(scan.clone());
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
                let def_with_stats = self.collect(&cte_ref.def, statistics_trace).await?;
                let mut new_cte_ref = cte_ref.clone();
                new_cte_ref.def = def_with_stats;

                Ok(s_expr.replace_plan(new_cte_ref))
            }
            _ => {
                let mut children = Vec::with_capacity(s_expr.arity());
                for child in s_expr.children() {
                    let child = Box::pin(self.collect(child, statistics_trace)).await?;
                    children.push(Arc::new(child));
                }
                Ok(s_expr.replace_children(children))
            }
        }
    }
}

#[derive(Default)]
struct StatisticsTrace {
    table_stats: BTreeMap<usize, TableStatistics>,
    column_stats: ColumnStatisticsMap,
    scan_mappings: Vec<ScanStatisticsMapping>,
}

#[derive(serde::Serialize)]
struct ScanStatisticsMapping {
    scan_id: usize,
    table_index: usize,
    catalog: String,
    database: String,
    table: String,
    change_type: Option<String>,
}

#[derive(Default)]
struct ColumnStatisticsMap(
    BTreeMap<(usize, String), (Option<BasicColumnStatistics>, Option<LegacyHistogram>)>,
);

impl serde::Serialize for ColumnStatisticsMap {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        use serde::ser::SerializeSeq;

        #[derive(serde::Serialize)]
        struct HistogramStatisticsMapEntry<'a> {
            table_index: usize,
            column_name: &'a str,
            statistics: &'a Option<BasicColumnStatistics>,
            histogram: &'a Option<LegacyHistogram>,
        }

        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for ((table_index, column_name), (statistics, histogram)) in &self.0 {
            seq.serialize_element(&HistogramStatisticsMapEntry {
                table_index: *table_index,
                column_name,
                statistics,
                histogram,
            })?;
        }
        seq.end()
    }
}

impl StatisticsTrace {
    fn is_empty(&self) -> bool {
        self.table_stats.is_empty() && self.scan_mappings.is_empty()
    }

    fn record_scan(
        &mut self,
        scan: &Scan,
        table_entry: &TableEntry,
        columns: &[ColumnEntry],
        table_stats: &Option<TableStatistics>,
        column_stats: &HashMap<Symbol, Option<BasicColumnStatistics>>,
        histograms: &HashMap<Symbol, Option<Histogram>>,
    ) {
        if let Some(table_stats) = table_stats {
            self.table_stats
                .entry(scan.table_index)
                .or_insert_with(|| *table_stats);
        }

        for column in columns {
            let ColumnEntry::BaseTableColumn(BaseTableColumn {
                column_index,
                column_name,
                virtual_expr: None,
                ..
            }) = column
            else {
                continue;
            };

            let column_stat = column_stats.get(column_index).and_then(Option::as_ref);
            let histogram = histograms.get(column_index).and_then(Option::as_ref);
            if column_stat.is_none() && histogram.is_none() {
                continue;
            }
            let (existing_stat, existing_histogram) = self
                .column_stats
                .0
                .entry((scan.table_index, column_name.clone()))
                .or_default();
            if existing_stat.is_none() {
                *existing_stat = column_stat.cloned();
            }
            if existing_histogram.is_none() {
                *existing_histogram = histogram.map(LegacyHistogram::from);
            }
        }

        self.scan_mappings.push(ScanStatisticsMapping {
            scan_id: scan.scan_id,
            table_index: scan.table_index,
            catalog: table_entry.catalog().to_string(),
            database: table_entry.database().to_string(),
            table: table_entry.name().to_string(),
            change_type: scan
                .change_type
                .as_ref()
                .map(|change_type| format!("{change_type:?}")),
        });
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
