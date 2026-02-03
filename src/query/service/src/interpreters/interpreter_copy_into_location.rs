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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::Result;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_pipeline::core::ExecutionInfo;
use databend_storages_common_stage::CopyIntoLocationInfo;
use log::debug;
use log::info;

use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::physical_plans::CopyIntoLocation;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
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

        let update_stream_meta = dml_build_update_stream_req(self.ctx.clone()).await?;

        Ok((select_interpreter, update_stream_meta))
    }

    /// Build a pipeline for local copy into stage.
    #[async_backtrace::framed]
    async fn build_local_copy_into_stage_pipeline(
        &self,
        query: &Plan,
        info: &CopyIntoLocationInfo,
    ) -> Result<(PipelineBuildResult, Vec<UpdateStreamMetaReq>)> {
        let (query_interpreter, update_stream_meta_req) = self.build_query(query).await?;
        let query_physical_plan = query_interpreter.build_physical_plan().await?;
        let query_result_schema = query_interpreter.get_result_schema();
        let table_schema = query_interpreter.get_result_table_schema()?;
        let mut physical_plan = PhysicalPlan::new(CopyIntoLocation {
            input: query_physical_plan,
            project_columns: query_interpreter.get_result_columns(),
            input_data_schema: query_result_schema,
            input_table_schema: table_schema,
            info: info.clone(),
            partition_by: self
                .plan
                .partition_by
                .as_ref()
                .map(|desc| desc.remote_expr.clone()),
            meta: PhysicalPlanMeta::new("CopyIntoLocation"),
        });

        let mut next_plan_id = 0;
        physical_plan.adjust_plan_id(&mut next_plan_id);
        Ok((
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?,
            update_stream_meta_req,
        ))
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

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "copy_into_location_interpreter_execute_v2");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let (mut pipeline_build_result, update_stream_reqs) = self
            .build_local_copy_into_stage_pipeline(&self.plan.from, &self.plan.info)
            .await?;

        // We are going to consuming streams, which are all of the default catalog
        let catalog = self.ctx.get_default_catalog()?;

        // Add a commit sink to the pipeline does not work, since the pipeline emits result set,
        // `inject_result` should work, but is cumbersome for this case
        pipeline_build_result.main_pipeline.set_on_finished(
            move |info: &ExecutionInfo| match &info.res {
                Ok(_) => GlobalIORuntime::instance().block_on(async move {
                    info!("Updating the stream meta for COPY INTO LOCATION statement",);
                    catalog.update_stream_metas(update_stream_reqs).await?;
                    Ok(())
                }),
                Err(e) => Err(e.clone()),
            },
        );

        Ok(pipeline_build_result)
    }
}
