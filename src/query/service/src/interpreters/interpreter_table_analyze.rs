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

use chrono::Utc;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::AnalyzeTablePlan;
use databend_common_sql::plans::Plan;
use databend_common_sql::Planner;
use databend_common_storages_factory::NavigationPoint;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;
use itertools::Itertools;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
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

        let r = table.read_table_snapshot(self.ctx.txn_mgr()).await;
        let snapshot_opt = match r {
            Err(e) => return Err(e),
            Ok(v) => v,
        };

        if let Some(snapshot) = snapshot_opt {
            // plan sql
            let schema = table.schema();
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
                        .read_table_snapshot(self.ctx.txn_mgr())
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

            let index_cols: Vec<(u32, String)> = schema
                .fields()
                .iter()
                .filter(|f| RangeIndex::supported_type(&f.data_type().into()))
                .map(|f| (f.column_id(), f.name.clone()))
                .collect();

            // 0.01625 --> 12 buckets --> 4K size per column
            // 1.04 / math.sqrt(1<<12) --> 0.01625
            const DISTINCT_ERROR_RATE: f64 = 0.01625;
            let select_expr = index_cols
                .iter()
                .map(|c| {
                    format!(
                        "approx_count_distinct_state({DISTINCT_ERROR_RATE})({}) as ndv_{}",
                        c.1, c.0
                    )
                })
                .join(", ");

            let sql = format!(
                "SELECT {select_expr}, {is_full} as is_full from {}.{} {temporal_str}",
                plan.database, plan.table,
            );

            log::info!("Analyze via sql {:?}", sql);

            let mut planner = Planner::new(self.ctx.clone());
            let (plan, _) = planner.plan_sql(&sql).await?;
            let (select_plan, schema) = match &plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                    ..
                } => {
                    let mut builder1 =
                        PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                    (
                        builder1.build(s_expr, bind_context.column_set()).await?,
                        bind_context.output_schema(),
                    )
                }
                _ => unreachable!(),
            };

            let mut build_res =
                build_query_pipeline_without_render_result_set(&self.ctx, &select_plan).await?;

            FuseTable::do_analyze(
                self.ctx.clone(),
                schema,
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.table,
                snapshot.snapshot_id,
                &mut build_res.main_pipeline,
            )?;
            return Ok(build_res);
        }

        return Ok(PipelineBuildResult::create());
    }
}
