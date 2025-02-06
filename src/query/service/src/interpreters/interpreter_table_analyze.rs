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

use chrono::Utc;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::executor::physical_plans::AggregateExpand;
use databend_common_sql::executor::physical_plans::AggregateFinal;
use databend_common_sql::executor::physical_plans::AggregatePartial;
use databend_common_sql::executor::physical_plans::EvalScalar;
use databend_common_sql::executor::physical_plans::Filter;
use databend_common_sql::executor::physical_plans::Sort;
use databend_common_sql::executor::physical_plans::Window;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::AnalyzeTablePlan;
use databend_common_sql::plans::Plan;
use databend_common_sql::BindContext;
use databend_common_sql::Planner;
use databend_common_storage::DEFAULT_HISTOGRAM_BUCKETS;
use databend_common_storages_factory::NavigationPoint;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::operations::HistogramInfoSink;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;
use itertools::Itertools;
use log::info;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline;
use crate::schedulers::build_query_pipeline_without_render_result_set;
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

    async fn plan_sql(&self, sql: String) -> Result<(PhysicalPlan, BindContext)> {
        let mut planner = Planner::new(self.ctx.clone());
        let (plan, _) = planner.plan_sql(&sql).await?;
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

        let r = table.read_table_snapshot().await;
        let snapshot_opt = match r {
            Err(e) => return Err(e),
            Ok(v) => v,
        };

        if let Some(snapshot) = snapshot_opt {
            // plan sql
            let _table_info = table.get_table_info();

            let table_statistics = table
                .read_table_snapshot_statistics(Some(&snapshot))
                .await?;

            let (is_full, temporal_str) = if let Some(table_statistics) = &table_statistics {
                let is_full = match table
                    .navigate_to_point(
                        &NavigationPoint::SnapshotID(
                            table_statistics.snapshot_id.simple().to_string(),
                        ),
                        self.ctx.clone().get_abort_checker(),
                    )
                    .await
                {
                    Ok(t) => !t
                        .read_table_snapshot()
                        .await
                        .is_ok_and(|s| s.is_some_and(|s| s.prev_table_seq.is_some())),
                    Err(_) => true,
                };

                let temporal_str = if is_full {
                    format!("AT (snapshot => '{}')", snapshot.snapshot_id.simple())
                } else {
                    // analyze only need to collect the added blocks.
                    let table_alias = format!("_change_insert${:08x}", Utc::now().timestamp());
                    format!(
                        "CHANGES(INFORMATION => DEFAULT) AT (snapshot => '{}') END (snapshot => '{}') AS {table_alias}",
                        table_statistics.snapshot_id.simple(),
                        snapshot.snapshot_id.simple(),
                    )
                };
                (is_full, temporal_str)
            } else {
                (
                    true,
                    format!("AT (snapshot => '{}')", snapshot.snapshot_id.simple()),
                )
            };

            let quote = self
                .ctx
                .get_settings()
                .get_sql_dialect()?
                .default_ident_quote();

            // 0.01625 --> 12 buckets --> 4K size per column
            // 1.04 / math.sqrt(1<<12) --> 0.01625
            const DISTINCT_ERROR_RATE: f64 = 0.01625;
            let ndv_select_expr = snapshot
                .schema
                .fields()
                .iter()
                .filter(|f| RangeIndex::supported_type(&f.data_type().into()))
                .map(|f| {
                    format!(
                        "approx_count_distinct_state({DISTINCT_ERROR_RATE})({quote}{}{quote}) as ndv_{}",
                        f.name,
                        f.column_id()
                    )
                })
                .join(", ");

            let sql = format!(
                "SELECT {ndv_select_expr}, {is_full} as is_full from {}.{} {temporal_str}",
                plan.database, plan.table,
            );

            info!("Analyze via sql: {sql}");

            let (physical_plan, bind_context) = self.plan_sql(sql).await?;
            let mut build_res =
                build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
            // After profiling, computing histogram is heavy and the bottleneck is window function(90%).
            // It's possible to OOM if the table is too large and spilling isn't enabled.
            // We add a setting `enable_analyze_histogram` to control whether to compute histogram(default is closed).
            let mut histogram_info_receivers = HashMap::new();
            if self.ctx.get_settings().get_enable_analyze_histogram()? {
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
                    let (mut histogram_plan, bind_context) = self.plan_sql(sql).await?;
                    if !self.ctx.get_cluster().is_empty() {
                        histogram_plan = remove_exchange(histogram_plan);
                    }
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
                        .push(histogram_build_res.main_pipeline.finalize());
                    build_res
                        .sources_pipelines
                        .extend(histogram_build_res.sources_pipelines);
                    histogram_info_receivers.insert(col_id, rx);
                }
            }
            FuseTable::do_analyze(
                self.ctx.clone(),
                bind_context.output_schema(),
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.table,
                snapshot.snapshot_id,
                &mut build_res.main_pipeline,
                histogram_info_receivers,
            )?;
            return Ok(build_res);
        }

        return Ok(PipelineBuildResult::create());
    }
}

fn remove_exchange(plan: PhysicalPlan) -> PhysicalPlan {
    fn traverse(plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            PhysicalPlan::Filter(plan) => PhysicalPlan::Filter(Filter {
                plan_id: plan.plan_id,
                projections: plan.projections,
                input: Box::new(traverse(*plan.input)),
                predicates: plan.predicates,
                stat_info: plan.stat_info,
            }),
            PhysicalPlan::EvalScalar(plan) => PhysicalPlan::EvalScalar(EvalScalar {
                plan_id: plan.plan_id,
                projections: plan.projections,
                input: Box::new(traverse(*plan.input)),
                exprs: plan.exprs,
                stat_info: plan.stat_info,
            }),
            PhysicalPlan::AggregateExpand(plan) => PhysicalPlan::AggregateExpand(AggregateExpand {
                plan_id: plan.plan_id,
                input: Box::new(traverse(*plan.input)),
                group_bys: plan.group_bys,
                grouping_sets: plan.grouping_sets,
                stat_info: plan.stat_info,
            }),
            PhysicalPlan::AggregatePartial(plan) => {
                PhysicalPlan::AggregatePartial(AggregatePartial {
                    plan_id: plan.plan_id,
                    input: Box::new(traverse(*plan.input)),
                    group_by: plan.group_by,
                    agg_funcs: plan.agg_funcs,
                    rank_limit: plan.rank_limit,
                    enable_experimental_aggregate_hashtable: plan
                        .enable_experimental_aggregate_hashtable,
                    group_by_display: plan.group_by_display,
                    stat_info: plan.stat_info,
                })
            }
            PhysicalPlan::AggregateFinal(plan) => PhysicalPlan::AggregateFinal(AggregateFinal {
                plan_id: plan.plan_id,
                input: Box::new(traverse(*plan.input)),
                group_by: plan.group_by,
                agg_funcs: plan.agg_funcs,
                before_group_by_schema: plan.before_group_by_schema,
                group_by_display: plan.group_by_display,
                stat_info: plan.stat_info,
            }),
            PhysicalPlan::Window(plan) => PhysicalPlan::Window(Window {
                plan_id: plan.plan_id,
                index: plan.index,
                input: Box::new(traverse(*plan.input)),
                func: plan.func,
                partition_by: plan.partition_by,
                order_by: plan.order_by,
                window_frame: plan.window_frame,
                limit: plan.limit,
            }),
            PhysicalPlan::Sort(plan) => PhysicalPlan::Sort(Sort {
                plan_id: plan.plan_id,
                input: Box::new(traverse(*plan.input)),
                order_by: plan.order_by,
                limit: plan.limit,
                after_exchange: plan.after_exchange,
                pre_projection: plan.pre_projection,
                stat_info: plan.stat_info,
            }),
            PhysicalPlan::Exchange(plan) => traverse(*plan.input),
            _ => plan,
        }
    }

    traverse(plan)
}
