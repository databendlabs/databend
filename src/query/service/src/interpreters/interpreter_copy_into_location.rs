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

use databend_common_catalog::plan::StageTableInfo;
use databend_common_exception::Result;
use databend_common_expression::infer_table_schema;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_sql::executor::physical_plans::CopyIntoLocation;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_storage::StageFilesInfo;
use log::debug;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyIntoLocationPlan;
use crate::sql::plans::Plan;

pub struct CopyIntoLocationInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyIntoLocationPlan,
}

impl CopyIntoLocationInterpreter {
    /// Create a CopyInterpreter with context and [`CopyIntoLocationPlan`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyIntoLocationPlan) -> Result<Self> {
        Ok(CopyIntoLocationInterpreter { ctx, plan })
    }

    #[async_backtrace::framed]
    async fn build_query(
        &self,
        query: &Plan,
    ) -> Result<(SelectInterpreter, Vec<UpdateStreamMetaReq>)> {
        let (s_expr, metadata, bind_context, formatted_ast) = match query {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                formatted_ast,
                ..
            } => (s_expr, metadata, bind_context, formatted_ast),
            v => unreachable!("Input plan must be Query, but it's {}", v),
        };

        let select_interpreter = SelectInterpreter::try_create(
            self.ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
            formatted_ast.clone(),
            false,
        )?;

        let update_stream_meta = dml_build_update_stream_req(self.ctx.clone(), metadata).await?;

        Ok((select_interpreter, update_stream_meta))
    }

    /// Build a pipeline for local copy into stage.
    #[async_backtrace::framed]
    async fn build_local_copy_into_stage_pipeline(
        &self,
        stage: &StageInfo,
        path: &str,
        query: &Plan,
    ) -> Result<PipelineBuildResult> {
        let (query_interpreter, update_stream_meta_req) = self.build_query(query).await?;
        let query_physical_plan = query_interpreter.build_physical_plan().await?;
        let query_result_schema = query_interpreter.get_result_schema();
        let table_schema = infer_table_schema(&query_result_schema)?;

        let mut physical_plan = PhysicalPlan::CopyIntoLocation(Box::new(CopyIntoLocation {
            plan_id: 0,
            input: Box::new(query_physical_plan),
            project_columns: query_interpreter.get_result_columns(),
            input_schema: query_result_schema,
            to_stage_info: StageTableInfo {
                schema: table_schema,
                stage_info: stage.clone(),
                files_info: StageFilesInfo {
                    path: path.to_string(),
                    files: None,
                    pattern: None,
                },
                files_to_copy: None,
                duplicated_files_detected: vec![],
                is_select: false,
                default_values: None,
            },
            update_stream_meta_req: Some(update_stream_meta_req),
        }));

        let mut next_plan_id = 0;
        physical_plan.adjust_plan_id(&mut next_plan_id);
        build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyIntoLocationInterpreter {
    fn name(&self) -> &str {
        "CopyIntoLocationInterpreterV2"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "copy_into_location_interpreter_execute_v2");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        self.build_local_copy_into_stage_pipeline(
            &self.plan.stage,
            &self.plan.path,
            &self.plan.from,
        )
        .await
    }
}
