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
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::MVId;
use databend_common_meta_app::schema::MVMeta;
use databend_common_meta_app::schema::MVMetaIdent;
use databend_common_meta_app::schema::MVState;
use databend_common_meta_app::schema::TableIdHistoryIdent;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_sql::plans::CreateMaterializedViewPlan;

use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateMaterializedViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateMaterializedViewPlan,
}

impl CreateMaterializedViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateMaterializedViewPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateMaterializedViewInterpreter {
    fn name(&self) -> &str {
        "CreateMaterializedViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let table_interpreter =
            CreateTableInterpreter::try_create(self.ctx.clone(), self.plan.table_plan.clone())?;
        table_interpreter.validate_create().await?;

        let select_plan = self.plan.table_plan.as_select.clone().ok_or_else(|| {
            ErrorCode::InvalidMaterializedView(
                "CREATE MATERIALIZED VIEW must contain an AS SELECT plan",
            )
        })?;
        let (mut pipeline, table_commit) = table_interpreter
            .prepare_table_as_select(select_plan)
            .await?;
        let Some((catalog, table_req)) = table_commit else {
            // CREATE MATERIALIZED VIEW IF NOT EXISTS found an existing object before
            // building the insert pipeline, so this is a no-op empty pipeline.
            return Ok(pipeline);
        };

        let original_query = self.plan.original_query.clone();
        let query = self.plan.query.clone();
        let source_table_id = self.plan.source_table_id;
        let source_progress = self.plan.source_progress.clone();
        let create_option = self.plan.table_plan.create_option;
        let table_id = table_req.table_id;
        let mv_id = MVId::new(table_id);
        let expected_prev_mv_id = table_req.prev_table_id.map(MVId::new);

        if source_table_id == table_id {
            return Err(ErrorCode::InvalidMaterializedView(
                "a materialized view can not use itself as its source table",
            ));
        }
        if expected_prev_mv_id == Some(mv_id) {
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "materialized view {mv_id} can not replace itself"
            )));
        }

        let mv_ident = MVMetaIdent::new_generic(table_req.name_ident.tenant.clone(), mv_id);
        let orphan_ident = TableIdHistoryIdent {
            database_id: table_req.db_id,
            table_name: table_req.orphan_table_name.clone().ok_or_else(|| {
                ErrorCode::InvalidMaterializedView(
                    "CREATE MATERIALIZED VIEW must have an orphan table record",
                )
            })?,
        };

        pipeline
            .main_pipeline
            .lift_on_finished(move |info: &ExecutionInfo| {
                if info.res.is_ok() {
                    let mv_meta = MVMeta {
                        source_table_id,
                        source_progress,
                        state: MVState::Valid,
                        last_refresh_time: Some(Utc::now()),
                        last_refresh_error: None,
                    };
                    let definition = MVDefinition {
                        original_query,
                        query,
                    };
                    if let Err(err) =
                        GlobalIORuntime::instance().block_on(catalog.commit_materialized_view(
                            &mv_ident,
                            expected_prev_mv_id,
                            &orphan_ident,
                            &mv_meta,
                            &definition,
                        ))
                    {
                        if create_option == CreateOption::CreateIfNotExists
                            && err.code() == ErrorCode::MATERIALIZED_VIEW_ALREADY_EXISTS
                        {
                            return Ok(());
                        }
                        return Err(err);
                    }
                }

                Ok(())
            });

        Ok(pipeline)
    }
}
