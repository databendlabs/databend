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

use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::Result;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::SendableDataBlockStream;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans::StageContext;
use log::debug;
use log::info;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyIntoTablePlan;
use crate::sql::MetadataRef;
use crate::stream::DataBlockStream;

pub struct CopyIntoTableInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    metadata: MetadataRef,
    stage_context: Option<Box<StageContext>>,
    overwrite: bool,
}

#[async_trait::async_trait]
impl Interpreter for CopyIntoTableInterpreter {
    fn name(&self) -> &str {
        "CopyIntoTableInterpreterV2"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "copy_into_table_interpreter_execute_v2");
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        // build source and append pipeline
        let mut build_res = {
            let mut physical_plan_builder =
                PhysicalPlanBuilder::new(self.metadata.clone(), self.ctx.clone(), false);
            let physical_plan = physical_plan_builder
                .build(&self.s_expr, Default::default())
                .await?;
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?
        };

        // build commit pipeline
        let copy_into_table: CopyIntoTablePlan = self.s_expr.plan().clone().try_into()?;
        let target_table = self
            .ctx
            .get_table(
                &copy_into_table.catalog_name,
                &copy_into_table.database_name,
                &copy_into_table.table_name,
            )
            .await?;
        let copied_files_meta_req = match &self.stage_context {
            Some(stage_context) => PipelineBuilder::build_upsert_copied_files_to_meta_req(
                self.ctx.clone(),
                target_table.as_ref(),
                stage_context.purge,
                &stage_context.files_to_copy,
                stage_context.force,
            )?,
            None => None,
        };
        let update_stream_meta =
            dml_build_update_stream_req(self.ctx.clone(), &self.metadata).await?;
        target_table.commit_insertion(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            copied_files_meta_req,
            update_stream_meta,
            self.overwrite,
            None,
            unsafe { self.ctx.get_settings().get_deduplicate_label()? },
        )?;

        // Purge files on pipeline finished.
        if let Some(stage_context) = &self.stage_context {
            let StageContext {
                purge,
                force: _,
                files_to_copy,
                duplicated_files_detected,
                stage_info,
            } = stage_context.as_ref();
            info!(
                "set files to be purged, # of copied files: {}, # of duplicated files: {}",
                files_to_copy.len(),
                duplicated_files_detected.len()
            );

            let files_to_be_deleted = files_to_copy
                .iter()
                .map(|v| v.path.clone())
                .chain(duplicated_files_detected.clone())
                .collect::<Vec<_>>();
            PipelineBuilder::set_purge_files_on_finished(
                self.ctx.clone(),
                files_to_be_deleted,
                *purge,
                stage_info.clone(),
                &mut build_res.main_pipeline,
            )?;
        }

        // Execute hook.
        {
            let copy_into_table: CopyIntoTablePlan = self.s_expr.plan().clone().try_into()?;
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                copy_into_table.catalog_name.to_string(),
                copy_into_table.database_name.to_string(),
                copy_into_table.table_name.to_string(),
                MutationKind::Insert,
                LockTableOption::LockNoRetry,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let copy_into_table: CopyIntoTablePlan = self.s_expr.plan().clone().try_into()?;
        match &copy_into_table.mutation_kind {
            MutationKind::CopyInto => {
                let stage_context = self.stage_context.as_ref().unwrap();
                let blocks = self.get_copy_into_table_result(
                    stage_context.stage_info.copy_options.return_failed_only,
                )?;
                Ok(Box::pin(DataBlockStream::create(None, blocks)))
            }
            MutationKind::Insert => Ok(Box::pin(DataBlockStream::create(None, vec![]))),
            _ => unreachable!(),
        }
    }
}

impl CopyIntoTableInterpreter {
    /// Create a CopyInterpreter with context and [`CopyIntoTablePlan`].
    pub fn try_create(
        ctx: Arc<QueryContext>,
        s_expr: SExpr,
        metadata: MetadataRef,
        stage_context: Option<Box<StageContext>>,
        overwrite: bool,
    ) -> Result<Self> {
        Ok(CopyIntoTableInterpreter {
            ctx,
            s_expr,
            metadata,
            stage_context,
            overwrite,
        })
    }

    fn get_copy_into_table_result(&self) -> Result<Vec<DataBlock>> {
        let return_all = !self
            .plan
            .stage_table_info
            .stage_info
            .copy_options
            .return_failed_only;
        let cs = self.ctx.get_copy_status();

        let mut results = cs.files.iter().collect::<Vec<_>>();
        results.sort_by(|a, b| a.key().cmp(b.key()));

        let n = cs.files.len();
        let mut files = Vec::with_capacity(n);
        let mut rows_loaded = Vec::with_capacity(n);
        let mut errors_seen = Vec::with_capacity(n);
        let mut first_error = Vec::with_capacity(n);
        let mut first_error_line = Vec::with_capacity(n);

        for entry in results {
            let status = entry.value();
            if let Some(err) = &status.error {
                files.push(entry.key().clone());
                rows_loaded.push(status.num_rows_loaded as i32);
                errors_seen.push(err.num_errors as i32);
                first_error.push(Some(err.first_error.error.to_string().clone()));
                first_error_line.push(Some(err.first_error.line as i32 + 1));
            } else if return_all {
                files.push(entry.key().clone());
                rows_loaded.push(status.num_rows_loaded as i32);
                errors_seen.push(0);
                first_error.push(None);
                first_error_line.push(None);
            }
        }
        let blocks = vec![DataBlock::new_from_columns(vec![
            StringType::from_data(files),
            Int32Type::from_data(rows_loaded),
            Int32Type::from_data(errors_seen),
            StringType::from_opt_data(first_error),
            Int32Type::from_opt_data(first_error_line),
        ])];
        Ok(blocks)
    }

    /// Build commit insertion pipeline.
    async fn commit_insertion(
        &self,
        main_pipeline: &mut Pipeline,
        plan: &CopyIntoTablePlan,
        files_to_copy: Vec<StageFileInfo>,
        duplicated_files_detected: Vec<String>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        deduplicated_label: Option<String>,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        let to_table = ctx
            .get_table(
                plan.catalog_info.catalog_name(),
                &plan.database_name,
                &plan.table_name,
            )
            .await?;

        // Commit.
        {
            let copied_files_meta_req = PipelineBuilder::build_upsert_copied_files_to_meta_req(
                ctx.clone(),
                to_table.as_ref(),
                &plan.stage_table_info.stage_info,
                &files_to_copy,
                plan.force,
            )?;

            to_table.commit_insertion(
                ctx.clone(),
                main_pipeline,
                copied_files_meta_req,
                update_stream_meta,
                plan.write_mode.is_overwrite(),
                None,
                deduplicated_label,
            )?;
        }

        // Purge files.
        {
            info!(
                "set files to be purged, # of copied files: {}, # of duplicated files: {}",
                files_to_copy.len(),
                duplicated_files_detected.len()
            );

            let files_to_be_deleted = files_to_copy
                .into_iter()
                .map(|v| v.path)
                .chain(duplicated_files_detected)
                .collect::<Vec<_>>();
            // set on_finished callback.
            PipelineBuilder::set_purge_files_on_finished(
                ctx.clone(),
                files_to_be_deleted,
                plan.stage_table_info.stage_info.copy_options.purge,
                plan.stage_table_info.stage_info.clone(),
                main_pipeline,
            )?;
        }
        Ok(())
    }

    async fn on_no_files_to_copy(&self) -> Result<PipelineBuildResult> {
        // currently, there is only one thing that we care about:
        //
        // if `purge_duplicated_files_in_copy` and `purge` are all enabled,
        // and there are duplicated files detected, we should clean them up immediately.

        // it might be better to reuse the PipelineBuilder::set_purge_files_on_finished,
        // unfortunately, hooking the on_finished callback of a "blank" pipeline,
        // e.g. `PipelineBuildResult::create` leads to runtime error (during pipeline execution).

        if self.plan.stage_table_info.stage_info.copy_options.purge
            && !self
                .plan
                .stage_table_info
                .duplicated_files_detected
                .is_empty()
            && self
                .ctx
                .get_settings()
                .get_enable_purge_duplicated_files_in_copy()?
        {
            info!(
                "purge_duplicated_files_in_copy enabled, number of duplicated files: {}",
                self.plan.stage_table_info.duplicated_files_detected.len()
            );

            PipelineBuilder::purge_files_immediately(
                self.ctx.clone(),
                self.plan.stage_table_info.duplicated_files_detected.clone(),
                self.plan.stage_table_info.stage_info.clone(),
            )
            .await?;
        }
        Ok(PipelineBuildResult::create())
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyIntoTableInterpreter {
    fn name(&self) -> &str {
        "CopyIntoTableInterpreterV2"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "copy_into_table_interpreter_execute_v2");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        if self.plan.no_file_to_copy {
            info!("no file to copy");
            return self.on_no_files_to_copy().await;
        }

        let (physical_plan, update_stream_meta) = self.build_physical_plan(&self.plan).await?;
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;

        // Build commit insertion pipeline.
        {
            let files_to_copy = self
                .plan
                .stage_table_info
                .files_to_copy
                .clone()
                .unwrap_or_default();

            let duplicated_files_detected =
                self.plan.stage_table_info.duplicated_files_detected.clone();

            self.commit_insertion(
                &mut build_res.main_pipeline,
                &self.plan,
                files_to_copy,
                duplicated_files_detected,
                update_stream_meta,
                unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            )
            .await?;
        }

        // Execute hook.
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                self.plan.catalog_info.catalog_name().to_string(),
                self.plan.database_name.to_string(),
                self.plan.table_name.to_string(),
                MutationKind::Insert,
                LockTableOption::LockNoRetry,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let blocks = if self.plan.no_file_to_copy {
            vec![DataBlock::empty_with_schema(self.plan.schema())]
        } else {
            self.get_copy_into_table_result()?
        };

        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}
