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

use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_sql::BindContext;
use databend_common_sql::Planner;
use databend_common_sql::plans::AnalyzeTablePlan;
use databend_common_sql::plans::Plan;
use databend_common_statistics::DEFAULT_HISTOGRAM_BUCKETS;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::HistogramInfoSink;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;
use log::info;

use crate::interpreters::Interpreter;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct AnalyzeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: AnalyzeTablePlan,
}

impl AnalyzeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AnalyzeTablePlan) -> Result<Self> {
        Ok(AnalyzeTableInterpreter { ctx, plan })
    }

    async fn plan_sql(
        &self,
        sql: String,
        force_disable_distributed_optimization: bool,
    ) -> Result<(PhysicalPlan, BindContext)> {
        let mut planner = Planner::new(self.ctx.clone());
        let extras = planner.parse_sql(&sql)?;
        let plan = planner
            .plan_stmt(&extras.statement, force_disable_distributed_optimization)
            .await?;

        let (select_plan, bind_context) = match &plan {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => {
                let mut builder =
                    PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                (
                    builder.build(s_expr, bind_context.column_set()).await?,
                    (**bind_context).clone(),
                )
            }
            _ => unreachable!(),
        };
        Ok((select_plan, bind_context))
    }
}

#[async_trait::async_trait]
impl Interpreter for AnalyzeTableInterpreter {
    fn name(&self) -> &str {
        "AnalyzeTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        // check mutability
        table.check_mutable()?;

        // Only fuse table can apply analyze
        let table = match FuseTable::try_from_table(table.as_ref()) {
            Ok(t) => t,
            Err(_) => return Ok(PipelineBuildResult::create()),
        };

        let Some(snapshot) = table.read_table_snapshot().await? else {
            return Ok(PipelineBuildResult::create());
        };

        let mut build_res = PipelineBuildResult::create();
        // After profiling, computing histogram is heavy and the bottleneck is window function(90%).
        // It's possible to OOM if the table is too large and spilling isn't enabled.
        // We add a setting `enable_analyze_histogram` to control whether to compute histogram(default is closed).
        let mut histogram_info_receivers = HashMap::new();
        let quote = self
            .ctx
            .get_settings()
            .get_sql_dialect()?
            .default_ident_quote();
        if self.ctx.get_settings().get_enable_analyze_histogram()?
            && self.ctx.get_settings().get_enable_table_snapshot_stats()?
        {
            let histogram_sqls = table
                    .schema()
                    .fields()
                    .iter()
                    .filter(|f| RangeIndex::supported_type(&f.data_type().into()))
                    .map(|f| {
                        let col_name = format!("{quote}{}{quote}", f.name);
                        (
                            format!(
                                "SELECT quantile, \
                                    COUNT(DISTINCT {col_name}) AS ndv, \
                                    MAX({col_name}) AS max_value, \
                                    MIN({col_name}) AS min_value, \
                                    COUNT() as count \
                                FROM ( \
                                    SELECT {col_name}, NTILE({}) OVER (ORDER BY {col_name}) AS quantile \
                                    FROM {}.{} WHERE {col_name} IS DISTINCT FROM NULL \
                                ) \
                                GROUP BY quantile ORDER BY quantile",
                                DEFAULT_HISTOGRAM_BUCKETS, plan.database, plan.table,
                            ),
                            f.column_id(),
                        )
                    })
                    .collect::<Vec<_>>();
            for (sql, col_id) in histogram_sqls.into_iter() {
                info!("Analyze histogram via sql: {sql}");
                let (histogram_plan, bind_context) = self.plan_sql(sql, true).await?;
                let mut histogram_build_res = build_query_pipeline(
                    &QueryContext::create_from(self.ctx.as_ref()),
                    &bind_context.columns,
                    &histogram_plan,
                    false,
                )
                .await?;
                let (tx, rx) = async_channel::unbounded();
                histogram_build_res.main_pipeline.add_sink(|input_port| {
                    Ok(ProcessorPtr::create(HistogramInfoSink::create(
                        Some(tx.clone()),
                        input_port.clone(),
                    )))
                })?;

                build_res
                    .sources_pipelines
                    .push(histogram_build_res.main_pipeline.finalize(None));
                build_res
                    .sources_pipelines
                    .extend(histogram_build_res.sources_pipelines);
                histogram_info_receivers.insert(col_id, rx);
            }
        }
        table.do_analyze(
            self.ctx.clone(),
            snapshot,
            &mut build_res.main_pipeline,
            histogram_info_receivers,
            self.plan.no_scan,
        )?;
        Ok(build_res)
    }
}
