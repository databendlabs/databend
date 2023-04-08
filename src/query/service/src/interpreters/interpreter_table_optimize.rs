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

use std::sync::Arc;

use common_base::runtime::GlobalIORuntime;
use common_catalog::table::CompactTarget;
use common_catalog::table::Table;
use common_exception::Result;
use common_sql::plans::OptimizeTableAction;
use common_sql::plans::OptimizeTablePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct OptimizeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: OptimizeTablePlan,
}

impl OptimizeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: OptimizeTablePlan) -> Result<Self> {
        Ok(OptimizeTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizeTableInterpreter {
    fn name(&self) -> &str {
        "OptimizeTableInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let tbl_name = self.plan.table.clone();
        let ctx = self.ctx.clone();
        let table = self
            .ctx
            .get_table(&catalog_name, &db_name, &tbl_name)
            .await?;

        let action = self.plan.action.clone();
        let do_purge = matches!(
            action,
            OptimizeTableAction::Purge(_) | OptimizeTableAction::All
        );

        let limit_opt = match action {
            OptimizeTableAction::CompactBlocks(limit_opt) => limit_opt,
            OptimizeTableAction::CompactSegments(limit_opt) => limit_opt,
            _ => None,
        };

        let compact_target = match action {
            OptimizeTableAction::CompactBlocks(_) | OptimizeTableAction::All => {
                CompactTarget::Blocks
            }
            OptimizeTableAction::CompactSegments(_) => CompactTarget::Segments,
            _ => CompactTarget::None,
        };

        let mut build_res = PipelineBuildResult::create();
        table
            .compact(
                ctx.clone(),
                compact_target,
                limit_opt,
                &mut build_res.main_pipeline,
            )
            .await?;

        if do_purge {
            if build_res.main_pipeline.is_empty() {
                purge(ctx, table, action).await?;
            } else {
                build_res.main_pipeline.set_on_finished(move |may_error| {
                    if may_error.is_none() {
                        return GlobalIORuntime::instance().block_on(async move {
                            // currently, context caches the table, we have to "refresh"
                            // the table by using the catalog API directly
                            let table = ctx
                                .get_catalog(&catalog_name)?
                                .get_table(ctx.get_tenant().as_str(), &db_name, &tbl_name)
                                .await?;
                            purge(ctx, table, action).await
                        });
                    }

                    Err(may_error.as_ref().unwrap().clone())
                });
            }
        }

        Ok(build_res)
    }
}

async fn purge(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    action: OptimizeTableAction,
) -> Result<()> {
    let instant = if let OptimizeTableAction::Purge(point) = action {
        point
    } else {
        None
    };
    let keep_latest = true;
    table.purge(ctx, instant, keep_latest).await
}
