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

use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::BlockThresholds;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_meta_app::principal::StageInfo;
use common_pipeline_core::Pipeline;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_sql::executor::CopyIntoTable;
use common_sql::executor::Exchange;
use common_sql::executor::FragmentKind;
use common_sql::executor::PhysicalPlan;
use common_sql::plans::CopyIntoTableMode;
use common_sql::plans::CopyIntoTablePlan;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use common_storages_stage::StageTable;
use tracing::info;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::builders::build_append2table_pipeline;
use crate::pipelines::builders::build_local_append_data_pipeline;
use crate::pipelines::builders::build_upsert_copied_files_to_meta_req;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_distributed_pipeline;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyPlan;
use crate::sql::plans::Plan;

pub struct CopyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyPlan,
}

impl CopyInterpreter {
    /// Create a CopyInterpreter with context and [`CopyPlan`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlan) -> Result<Self> {
        Ok(CopyInterpreter { ctx, plan })
    }

    #[async_backtrace::framed]
    async fn build_query(&self, query: &Plan) -> Result<(PipelineBuildResult, DataSchemaRef)> {
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

        // Building data schema from bind_context columns
        // TODO(leiyskey): Extract the following logic as new API of BindContext.
        let fields = bind_context
            .columns
            .iter()
            .map(|column_binding| {
                DataField::new(
                    &column_binding.column_name,
                    *column_binding.data_type.clone(),
                )
            })
            .collect();
        let data_schema = DataSchemaRefExt::create(fields);
        let plan = select_interpreter.build_physical_plan().await?;
        let build_res = select_interpreter.build_pipeline(plan).await?;
        Ok((build_res, data_schema))
    }

    /// Build a pipeline for local copy into stage.
    #[async_backtrace::framed]
    async fn build_local_copy_into_stage_pipeline(
        &self,
        stage: &StageInfo,
        path: &str,
        query: &Plan,
    ) -> Result<PipelineBuildResult> {
        let (mut build_res, data_schema) = self.build_query(query).await?;
        let table_schema = infer_table_schema(&data_schema)?;
        let stage_table_info = StageTableInfo {
            schema: table_schema,
            stage_info: stage.clone(),
            files_info: StageFilesInfo {
                path: path.to_string(),
                files: None,
                pattern: None,
            },
            files_to_copy: None,
            is_select: false,
        };
        let table = StageTable::try_create(stage_table_info)?;
        build_append2table_pipeline(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            table,
            data_schema,
            None,
            false,
            AppendMode::Normal,
        )?;
        Ok(build_res)
    }

    fn set_status(&self, status: &str) {
        self.ctx.set_status_info(status);
        info!(status);
    }

    #[async_backtrace::framed]
    async fn build_physical_plan(&self, plan: &CopyIntoTablePlan) -> Result<PhysicalPlan> {
        let to_table = self
            .ctx
            .get_table(
                &plan.serializable_part.catalog_name,
                &plan.serializable_part.database_name,
                &plan.serializable_part.table_name,
            )
            .await?;
        let table_ctx: Arc<dyn TableContext> = self.ctx.clone();
        let files = plan.collect_files(&table_ctx).await?;
        let mut stage_table_info = plan.serializable_part.stage_table_info.clone();
        stage_table_info.files_to_copy = Some(files);
        let stage_table = StageTable::try_create(stage_table_info)?;
        let read_source_plan = {
            stage_table
                .read_plan_with_catalog(
                    self.ctx.clone(),
                    plan.serializable_part.catalog_name.to_string(),
                    None,
                    None,
                    false,
                )
                .await?
        };
        let parts_num = read_source_plan.parts.len();
        let mut root = PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
            serializable_part: plan.serializable_part.clone(),
            source: Box::new(read_source_plan),
            table_info: to_table.get_table_info().clone(),
        }));
        if plan.enable_distributed && parts_num > 1 {
            root = PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(root),
                kind: FragmentKind::Merge,
                keys: Vec::new(),
            });
        }
        Ok(root)
    }

    #[async_backtrace::framed]
    async fn build_read_stage_table_data_pipeline(
        &self,
        pipeline: &mut Pipeline,
        plan: &CopyIntoTablePlan,
        block_thresholds: BlockThresholds,
        files: Vec<StageFileInfo>,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        let table_ctx: Arc<dyn TableContext> = ctx.clone();

        self.set_status("begin to read stage source plan");

        let mut stage_table_info = plan.serializable_part.stage_table_info.clone();
        stage_table_info.files_to_copy = Some(files.clone());
        let stage_table = StageTable::try_create(stage_table_info.clone())?;
        let read_source_plan = {
            stage_table
                .read_plan_with_catalog(
                    ctx.clone(),
                    plan.serializable_part.catalog_name.to_string(),
                    None,
                    None,
                    false,
                )
                .await?
        };

        self.set_status(&format!(
            "begin to read stage table data, parts:{}",
            read_source_plan.parts.len()
        ));

        stage_table.set_block_thresholds(block_thresholds);
        stage_table.read_data(table_ctx, &read_source_plan, pipeline)?;
        Ok(())
    }

    /// Build a pipeline to copy data from local.
    #[async_backtrace::framed]
    async fn build_local_copy_into_table_pipeline(
        &self,
        plan: &CopyIntoTablePlan,
    ) -> Result<PipelineBuildResult> {
        let ctx = self.ctx.clone();
        let to_table = ctx
            .get_table(&plan.catalog_name, &plan.database_name, &plan.table_name)
            .await?;

        let (mut build_res, source_schema, files) = if let Some(query) = &plan.query {
            let (build_res, source_schema) = self.build_query(query).await?;
            (
                build_res,
                source_schema,
                plan.stage_table_info
                    .files_to_copy
                    .clone()
                    .ok_or(ErrorCode::Internal("files_to_copy should not be None"))?,
            )
        } else {
            let table_ctx: Arc<dyn TableContext> = self.ctx.clone();
            let files = plan.collect_files(&table_ctx).await?;
            let mut build_res = PipelineBuildResult::create();
            if files.is_empty() {
                return Ok(build_res);
            }
            self.build_read_stage_table_data_pipeline(
                &mut build_res.main_pipeline,
                plan,
                to_table.get_block_thresholds(),
                files.clone(),
            )
            .await?;
            (build_res, plan.required_source_schema.clone(), files)
        };

        build_local_append_data_pipeline(
            ctx,
            &mut build_res.main_pipeline,
            plan.clone(),
            source_schema,
            to_table,
            files,
        )?;

        Ok(build_res)
    }

    /// Build a pipeline to copy data into table for distributed.
    #[async_backtrace::framed]
    async fn build_distributed_copy_into_table_pipeline(
        &self,
        distributed_plan: &CopyIntoTable,
    ) -> Result<PipelineBuildResult> {
        let mut build_res = build_distributed_pipeline(&self.ctx, &exchange_plan, false).await?;

        let catalog = self.ctx.get_catalog(&distributed_plan.catalog_name)?;
        let to_table = catalog.get_table_by_info(&distributed_plan.table_info)?;
        let copied_files = build_upsert_copied_files_to_meta_req(
            self.ctx.clone(),
            to_table.clone(),
            distributed_plan.stage_table_info.stage_info.clone(),
            distributed_plan.files.clone(),
            distributed_plan.force,
        )?;
        let mut overwrite_ = false;
        if let CopyIntoTableMode::Insert { overwrite } = distributed_plan.write_mode {
            overwrite_ = overwrite;
        }
        to_table.commit_insertion(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            copied_files,
            overwrite_,
        )?;

        Ok(build_res)
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreter {
    fn name(&self) -> &str {
        "CopyInterpreterV2"
    }

    #[tracing::instrument(level = "debug", name = "copy_interpreter_execute_v2", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        match &self.plan {
            CopyPlan::IntoTable(plan) => {
                let physical_plan = self.build_physical_plan(plan).await?;
                let build_res = build_query_pipeline_without_render_result_set(
                    &self.ctx,
                    &physical_plan,
                    false,
                )
                .await?;
                todo!()
            }
            CopyPlan::IntoStage {
                stage, from, path, ..
            } => {
                self.build_local_copy_into_stage_pipeline(stage, path, from)
                    .await
            }
            CopyPlan::NoFileToCopy => Ok(PipelineBuildResult::create()),
        }
    }
}
