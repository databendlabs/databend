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
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F64;
use databend_common_expression::types::NumberScalar;
use databend_common_meta_app::schema::SecurityPolicyColumnMap;
use databend_common_statistics::Histogram;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_users::UserApiProvider;
use databend_enterprise_row_access_policy_feature::get_row_access_policy_handler;
use databend_storages_common_table_meta::meta::LegacyHistogram;
use educe::Educe;
use serde::Serialize;

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
use crate::plans::Operator;
use crate::plans::RelOperator;
use crate::plans::Scan;
use crate::plans::Statistics;
use crate::plans::UDFCall;
use crate::plans::Visitor;

#[derive(Clone, Default)]
pub struct StatisticsTraceCollector {
    trace: Arc<Mutex<Option<serde_json::Value>>>,
}

impl StatisticsTraceCollector {
    pub fn take(self) -> Option<serde_json::Value> {
        self.trace.lock().unwrap().take()
    }
}

// The CollectStatisticsOptimizer will collect statistics for each leaf node in SExpr.
pub struct CollectStatisticsOptimizer {
    table_ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    enable_trace: bool,
    trace_collector: Option<StatisticsTraceCollector>,
}

impl CollectStatisticsOptimizer {
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        CollectStatisticsOptimizer {
            table_ctx: opt_ctx.get_table_ctx(),
            metadata: opt_ctx.get_metadata(),
            enable_trace: log::log_enabled!(log::Level::Debug),
            trace_collector: None,
        }
    }

    pub fn with_trace_collector(mut self, trace_collector: StatisticsTraceCollector) -> Self {
        self.trace_collector = Some(trace_collector);
        self
    }

    async fn optimize_async(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut collector = TraceCollector::default();
        let s_expr = self.collect(s_expr, &mut collector).await?;
        if self.should_collect_trace() {
            self.finalize_trace(&s_expr, &mut collector)?;
            if self.enable_trace {
                let TraceCollector {
                    views,
                    udfs,
                    table_stats,
                    column_stats,
                    scan_mappings,
                } = &collector;
                log::debug!(
                    views:serde,
                    table_stats:serde,
                    column_stats:serde,
                    scan_mappings:serde,
                    udfs:serde; "optimizer statistics trace");
            }
            if let Some(trace_collector) = &self.trace_collector {
                *trace_collector.trace.lock().unwrap() = Some(serde_json::to_value(collector)?);
            }
        }
        Ok(s_expr)
    }

    fn should_collect_trace(&self) -> bool {
        self.enable_trace || self.trace_collector.is_some()
    }

    fn finalize_trace(&self, s_expr: &SExpr, collector: &mut TraceCollector) -> Result<()> {
        collector.record_views(&self.metadata);
        collector.record_udfs(s_expr)?;
        Ok(())
    }

    #[async_recursion::async_recursion(#[recursive::recursive])]
    async fn collect(&mut self, s_expr: &SExpr, collector: &mut TraceCollector) -> Result<SExpr> {
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
                let mut top_n = HashMap::new();
                let mut count_min_sketch = HashMap::new();
                let collect_frequency_stats = scan.change_type.is_none();
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
                            if collect_frequency_stats
                                && let Some(column_top_n) =
                                    column_statistics_provider.top_n(*column_id as ColumnId)
                            {
                                top_n.insert(*column_index, column_top_n);
                            }
                            if collect_frequency_stats
                                && let Some(column_count_min_sketch) = column_statistics_provider
                                    .count_min_sketch(*column_id as ColumnId)
                            {
                                count_min_sketch.insert(*column_index, column_count_min_sketch);
                            }
                        }
                    }
                }

                if self.should_collect_trace() {
                    let row_access_policy = self
                        .record_replay_row_access_policy(
                            table.schema().fields(),
                            table
                                .get_table_info()
                                .meta
                                .row_access_policy_columns_ids
                                .as_ref(),
                        )
                        .await?;
                    collector.record_scan(
                        scan,
                        &table_entry,
                        &columns,
                        table.schema().fields(),
                        &table_stats,
                        &column_stats,
                        &histograms,
                        row_access_policy,
                    );
                }

                let mut scan = scan.clone();
                scan.statistics = Arc::new(Statistics {
                    table_stats,
                    column_stats,
                    histograms,
                    top_n,
                    count_min_sketch,
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
                let def_with_stats = self.collect(&cte_ref.def, collector).await?;
                let mut new_cte_ref = cte_ref.clone();
                new_cte_ref.def = def_with_stats;

                Ok(s_expr.replace_plan(new_cte_ref))
            }
            _ => {
                let mut children = Vec::with_capacity(s_expr.arity());
                for child in s_expr.children() {
                    let child = Box::pin(self.collect(child, collector)).await?;
                    children.push(Arc::new(child));
                }
                Ok(s_expr.replace_children(children))
            }
        }
    }

    async fn record_replay_row_access_policy(
        &self,
        fields: &[TableField],
        policy: Option<&SecurityPolicyColumnMap>,
    ) -> Result<Option<ReplayRowAccessPolicy>> {
        let Some(policy) = policy else {
            return Ok(None);
        };

        let columns = policy
            .columns_ids
            .iter()
            .map(|column_id| {
                fields
                    .iter()
                    .find(|field| field.column_id == *column_id)
                    .map(|field| field.name().to_string())
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "unable to collect row access policy column id {}. Valid fields: {:?}",
                            column_id,
                            fields.iter().map(|field| field.name()).collect::<Vec<_>>()
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let tenant = self.table_ctx.get_tenant();
        let policy_id = policy.policy_id;
        let seq_v = get_row_access_policy_handler()
            .get_row_access_policy_by_id(meta_api, &tenant, policy_id)
            .await?;
        let meta = seq_v.data;

        Ok(Some(ReplayRowAccessPolicy {
            policy_id,
            columns,
            args: meta.args,
            body: meta.body,
        }))
    }
}

#[derive(Default, Serialize)]
struct TraceCollector {
    views: ViewSet,
    udfs: UdfSet,
    table_stats: BTreeMap<usize, ReplayTable>,
    column_stats: ColumnStatisticsMap,
    scan_mappings: Vec<ScanStatisticsMapping>,
}

#[derive(Educe, serde::Serialize)]
#[educe(PartialEq, Eq, PartialOrd, Ord)]
struct ReplayView {
    catalog: String,
    database: String,
    view: String,
    #[educe(PartialEq(ignore), PartialOrd(ignore))]
    query: String,
}

#[derive(Educe, serde::Serialize)]
#[educe(PartialEq, Eq, PartialOrd, Ord)]
struct ReplayUdf {
    name: String,
    #[educe(PartialEq(ignore), PartialOrd(ignore))]
    arg_types: Vec<DataType>,
    #[educe(PartialEq(ignore), PartialOrd(ignore))]
    return_type: DataType,
}

#[derive(serde::Serialize)]
struct ReplayTable {
    statistics: Option<TableStatistics>,
    fields: Vec<TableField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_access_policy: Option<ReplayRowAccessPolicy>,
}

#[derive(serde::Serialize)]
struct ReplayRowAccessPolicy {
    policy_id: u64,
    columns: Vec<String>,
    args: Vec<(String, String)>,
    body: String,
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
struct ColumnStatisticsMap(BTreeMap<(usize, String), ColumnStatisticsEntry>);

struct ColumnStatisticsEntry {
    statistics: Option<BasicColumnStatistics>,
    histogram: Option<LegacyHistogram>,
}

#[derive(Default)]
struct ViewSet(BTreeSet<ReplayView>);

#[derive(Default)]
struct UdfSet(BTreeSet<ReplayUdf>);

impl Serialize for ViewSet {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        use serde::ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for view in &self.0 {
            seq.serialize_element(view)?;
        }
        seq.end()
    }
}

impl Serialize for UdfSet {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        use serde::ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for udf in &self.0 {
            seq.serialize_element(udf)?;
        }
        seq.end()
    }
}

impl Serialize for ColumnStatisticsMap {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        use serde::ser::SerializeSeq;

        #[derive(serde::Serialize)]
        struct ColumnStatisticsMapEntry<'a> {
            table_index: usize,
            column_name: &'a str,
            statistics: &'a Option<BasicColumnStatistics>,
            histogram: &'a Option<LegacyHistogram>,
        }

        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for ((table_index, column_name), entry) in &self.0 {
            seq.serialize_element(&ColumnStatisticsMapEntry {
                table_index: *table_index,
                column_name,
                statistics: &entry.statistics,
                histogram: &entry.histogram,
            })?;
        }
        seq.end()
    }
}

impl TraceCollector {
    fn record_views(&mut self, metadata: &MetadataRef) {
        let metadata = metadata.read();
        for table_entry in metadata.tables() {
            let table = table_entry.table();
            if table.engine() != VIEW_ENGINE {
                continue;
            }
            let Some(query) = table.get_table_info().meta.options.get(QUERY) else {
                continue;
            };

            self.views.0.insert(ReplayView {
                catalog: table_entry.catalog().to_string(),
                database: table_entry.database().to_string(),
                view: table_entry.name().to_string(),
                query: query.clone(),
            });
        }
    }

    fn record_udfs(&mut self, s_expr: &SExpr) -> Result<()> {
        for scalar in s_expr.plan().scalar_expr_iter() {
            self.udfs.record_scalar(scalar)?;
        }

        for child in s_expr.children() {
            self.record_udfs(child)?;
        }

        Ok(())
    }

    fn record_scan(
        &mut self,
        scan: &Scan,
        table_entry: &TableEntry,
        columns: &[ColumnEntry],
        table_fields: &[TableField],
        table_stats: &Option<TableStatistics>,
        column_stats: &HashMap<Symbol, Option<BasicColumnStatistics>>,
        histograms: &HashMap<Symbol, Option<Histogram>>,
        row_access_policy: Option<ReplayRowAccessPolicy>,
    ) {
        self.table_stats
            .entry(scan.table_index)
            .or_insert_with(|| ReplayTable {
                statistics: *table_stats,
                fields: table_fields.to_vec(),
                row_access_policy,
            });

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
            let entry = self
                .column_stats
                .0
                .entry((scan.table_index, column_name.clone()))
                .or_insert_with(|| ColumnStatisticsEntry {
                    statistics: None,
                    histogram: None,
                });
            if entry.statistics.is_none() {
                entry.statistics = column_stat.cloned();
            }
            if entry.histogram.is_none() {
                entry.histogram = histogram.map(LegacyHistogram::from);
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

impl UdfSet {
    fn record_scalar(&mut self, scalar: &ScalarExpr) -> Result<()> {
        let mut visitor = UdfTraceVisitor { udfs: self };
        visitor.visit(scalar)
    }
}

struct UdfTraceVisitor<'a> {
    udfs: &'a mut UdfSet,
}

impl<'a> Visitor<'a> for UdfTraceVisitor<'_> {
    fn visit_udf_call(&mut self, udf: &'a UDFCall) -> Result<()> {
        for argument in &udf.arguments {
            self.visit(argument)?;
        }

        self.udfs.0.insert(ReplayUdf {
            name: udf.name.clone(),
            arg_types: udf.arg_types.clone(),
            return_type: (*udf.return_type).clone(),
        });
        Ok(())
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
