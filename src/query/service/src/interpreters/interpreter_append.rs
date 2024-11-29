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
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::SendableDataBlockStream;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans::AppendType;
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
use crate::sql::plans::Append;
use crate::sql::MetadataRef;
use crate::stream::DataBlockStream;

pub struct AppendInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    metadata: MetadataRef,
    stage_table_info: Option<Box<StageTableInfo>>,
    overwrite: bool,
    col_type_modified: bool,
}

#[async_trait::async_trait]
impl Interpreter for AppendInterpreter {
    fn name(&self) -> &str {
        "AppendInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "append_interpreter_execute");
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let append: Append = self.s_expr.plan().clone().try_into()?;
        let (target_table, catalog, database, table) = {
            let metadata = self.metadata.read();
            let t = metadata.table(append.table_index);
            (
                t.table(),
                t.catalog().to_string(),
                t.database().to_string(),
                t.name().to_string(),
            )
        };

        target_table.check_mutable()?;

        // 1. build source and append pipeline
        let mut build_res = {
            let mut physical_plan_builder =
                PhysicalPlanBuilder::new(self.metadata.clone(), self.ctx.clone(), false);
            let physical_plan = physical_plan_builder
                .build(&self.s_expr, Default::default())
                .await?;
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?
        };

        // 2. build commit pipeline
        let copied_files_meta_req = match &self.stage_table_info {
            Some(stage_table_info) => PipelineBuilder::build_upsert_copied_files_to_meta_req(
                self.ctx.clone(),
                target_table.as_ref(),
                stage_table_info
                    .files_to_copy
                    .as_deref()
                    .unwrap_or_default(),
                &stage_table_info.copy_into_table_options,
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
            self.col_type_modified,
            unsafe { self.ctx.get_settings().get_deduplicate_label()? },
        )?;

        // 3. Purge files on pipeline finished.
        if let Some(stage_table_info) = &self.stage_table_info {
            let files_to_copy = stage_table_info
                .files_to_copy
                .as_deref()
                .unwrap_or_default();
            info!(
                "set files to be purged, # of copied files: {}, # of duplicated files: {}",
                files_to_copy.len(),
                stage_table_info.duplicated_files_detected.len()
            );

            let files_to_be_deleted = files_to_copy
                .iter()
                .map(|f| f.path.clone())
                .chain(stage_table_info.duplicated_files_detected.clone())
                .collect::<Vec<_>>();
            PipelineBuilder::set_purge_files_on_finished(
                self.ctx.clone(),
                files_to_be_deleted,
                &stage_table_info.copy_into_table_options,
                stage_table_info.stage_info.clone(),
                &mut build_res.main_pipeline,
            )?;
        }

        // 4. Execute hook.
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                catalog,
                database,
                table,
                MutationKind::Insert,
                LockTableOption::LockNoRetry,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let append: Append = self.s_expr.plan().clone().try_into()?;
        match &append.append_type {
            AppendType::CopyInto => {
                let blocks = self.get_copy_into_table_result()?;
                Ok(Box::pin(DataBlockStream::create(None, blocks)))
            }
            AppendType::Insert => Ok(Box::pin(DataBlockStream::create(None, vec![]))),
        }
    }
}

impl AppendInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        s_expr: SExpr,
        metadata: MetadataRef,
        stage_table_info: Option<Box<StageTableInfo>>,
        overwrite: bool,
        col_type_modified: bool,
    ) -> Result<Self> {
        Ok(AppendInterpreter {
            ctx,
            s_expr,
            metadata,
            stage_table_info,
            overwrite,
            col_type_modified,
        })
    }

    fn get_copy_into_table_result(&self) -> Result<Vec<DataBlock>> {
        let return_all = !self
            .stage_table_info
            .as_ref()
            .unwrap()
            .copy_into_table_options
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
}
