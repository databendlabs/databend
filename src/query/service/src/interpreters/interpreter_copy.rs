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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use common_base::runtime::GlobalIORuntime;
use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::BlockThresholds;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::Scalar;
use common_meta_app::principal::StageInfo;
use common_meta_app::schema::TableCopiedFileInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_pipeline_core::Pipeline;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_sql::plans::CopyIntoTableMode;
use common_sql::plans::CopyIntoTablePlan;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use common_storages_fuse::io::Files;
use common_storages_stage::StageTable;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::interpreters::common::append2table;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::processors::transforms::TransformAddConstColumns;
use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::processors::TransformResortAddOn;
use crate::pipelines::PipelineBuildResult;
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

    #[async_backtrace::framed]
    async fn build_copy_into_stage_pipeline(
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
        };
        let table = StageTable::try_create(stage_table_info)?;
        append2table(
            self.ctx.clone(),
            table,
            data_schema,
            &mut build_res,
            false,
            true,
            AppendMode::Normal,
        )?;
        Ok(build_res)
    }

    #[async_backtrace::framed]
    async fn try_purge_files(
        ctx: Arc<QueryContext>,
        stage_info: &StageInfo,
        stage_file_infos: &[StageFileInfo],
    ) {
        let purge_start = Instant::now();
        let num_copied_files = stage_file_infos.len();

        // Status.
        {
            let status = format!("begin to purge files:{}", num_copied_files);
            ctx.set_status_info(&status);
            info!(status);
        }

        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let op = StageTable::get_op(stage_info);
        match op {
            Ok(op) => {
                let file_op = Files::create(table_ctx, op);
                let files = stage_file_infos
                    .iter()
                    .map(|v| v.path.clone())
                    .collect::<Vec<_>>();
                if let Err(e) = file_op.remove_file_in_batch(&files).await {
                    error!("Failed to delete file: {:?}, error: {}", files, e);
                }
            }
            Err(e) => {
                error!("Failed to get stage table op, error: {}", e);
            }
        }
        // Status.
        info!(
            "end to purge files:{}, elapsed:{}",
            num_copied_files,
            purge_start.elapsed().as_secs()
        );
    }

    fn set_status(&self, status: &str) {
        self.ctx.set_status_info(status);
        info!(status);
    }

    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    async fn build_read_stage(
        &self,
        pipeline: &mut Pipeline,
        plan: &CopyIntoTablePlan,
        block_thresholds: BlockThresholds,
        files: Vec<StageFileInfo>,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        let table_ctx: Arc<dyn TableContext> = ctx.clone();

        self.set_status("begin to read stage source plan");

        let mut stage_table_info = plan.stage_table_info.clone();
        stage_table_info.files_to_copy = Some(files.clone());
        let stage_table = StageTable::try_create(stage_table_info.clone())?;
        let read_source_plan = {
            stage_table
                .read_plan_with_catalog(ctx.clone(), plan.catalog_name.to_string(), None, None)
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

    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    async fn build_copy_into_table_pipeline(
        &self,
        plan: &CopyIntoTablePlan,
    ) -> Result<PipelineBuildResult> {
        let start = Instant::now();
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

            self.build_read_stage(
                &mut build_res.main_pipeline,
                plan,
                to_table.get_block_thresholds(),
                files.clone(),
            )
            .await?;
            (build_res, plan.required_source_schema.clone(), files)
        };
        let table_data_schema = Arc::new(to_table.schema().into());

        debug!("source schema:{:?}", source_schema);
        debug!("required source schema:{:?}", plan.required_source_schema);
        debug!("required values schema:{:?}", plan.required_values_schema);
        debug!("table_data_schema:{:?}", table_data_schema);

        if source_schema != plan.required_source_schema {
            // only parquet need cast
            let func_ctx = ctx.get_function_context()?;
            build_res.main_pipeline.add_transform(
                |transform_input_port, transform_output_port| {
                    TransformCastSchema::try_create(
                        transform_input_port,
                        transform_output_port,
                        source_schema.clone(),
                        plan.required_source_schema.clone(),
                        func_ctx.clone(),
                    )
                },
            )?;
        }

        if !plan.values_consts.is_empty() {
            fill_const_columns(
                ctx.clone(),
                &mut build_res.main_pipeline,
                source_schema,
                plan.required_values_schema.clone(),
                plan.values_consts.clone(),
            )?;
        }

        if matches!(
            plan.write_mode,
            CopyIntoTableMode::Insert { .. } | CopyIntoTableMode::Copy
        ) {
            if plan.required_values_schema != table_data_schema {
                build_res.main_pipeline.add_transform(
                    |transform_input_port, transform_output_port| {
                        TransformResortAddOn::try_create(
                            ctx.clone(),
                            transform_input_port,
                            transform_output_port,
                            plan.required_values_schema.clone(),
                            to_table.clone(),
                        )
                    },
                )?;
            }

            to_table.append_data(
                ctx.clone(),
                &mut build_res.main_pipeline,
                AppendMode::Copy,
                false,
            )?;
        }

        let stage_table_info_clone = plan.stage_table_info.clone();
        let force = plan.force;
        let write_mode = plan.write_mode;
        build_res.main_pipeline.set_on_finished(move |may_error| {
            match may_error {
                None => {
                    match write_mode {
                        CopyIntoTableMode::Insert { overwrite } => {
                            let operations = ctx.consume_precommit_blocks();

                            GlobalIORuntime::instance().block_on(async move {
                                to_table
                                    .commit_insertion(ctx.clone(), operations, None, overwrite)
                                    .await?;
                                CopyInterpreter::try_purge_files(
                                    ctx.clone(),
                                    &stage_table_info_clone.stage_info,
                                    &files,
                                )
                                .await;
                                Ok(())
                            })?;
                        }
                        CopyIntoTableMode::Replace => {
                            GlobalIORuntime::instance().block_on(async move {
                                CopyInterpreter::try_purge_files(
                                    ctx.clone(),
                                    &stage_table_info_clone.stage_info,
                                    &files,
                                )
                                .await;
                                Ok(())
                            })?;
                        }
                        CopyIntoTableMode::Copy => {
                            CopyInterpreter::commit_copy_into_table(
                                ctx.clone(),
                                to_table,
                                stage_table_info_clone.stage_info,
                                files,
                                write_mode,
                                force,
                            )?;
                            // Status.
                            {
                                info!("all copy finished, elapsed:{}", start.elapsed().as_secs());
                            }
                        }
                    }
                }
                Some(error) => {
                    error!(
                        "copy failed, elapsed:{}, reason: {}",
                        start.elapsed().as_secs(),
                        error
                    );
                }
            }
            Ok(())
        });

        Ok(build_res)
    }

    /// Pipeline finish.
    /// 1. commit the data.
    /// 2. update the NeedCopy file into to meta.
    /// 3. log on_error mode errors.
    /// 4. purge the copied files.
    #[allow(clippy::too_many_arguments)]
    fn commit_copy_into_table(
        ctx: Arc<QueryContext>,
        to_table: Arc<dyn Table>,
        stage_info: StageInfo,
        copied_files: Vec<StageFileInfo>,
        mode: CopyIntoTableMode,
        force: bool,
    ) -> Result<()> {
        let num_copied_files = copied_files.len();
        let mut copied_file_tree = BTreeMap::new();
        for file in &copied_files {
            // Short the etag to 7 bytes for less space in metasrv.
            let short_etag = file.etag.clone().map(|mut v| {
                v.truncate(7);
                v
            });
            copied_file_tree.insert(file.path.clone(), TableCopiedFileInfo {
                etag: short_etag,
                content_length: file.size,
                last_modified: Some(file.last_modified),
            });
        }

        GlobalIORuntime::instance().block_on(async move {
            // 1. Commit data to table.
            let operations = ctx.consume_precommit_blocks();

            let expire_hours = ctx.get_settings().get_load_file_metadata_expire_hours()?;

            let upsert_copied_files_request = {
                if mode != CopyIntoTableMode::Copy {
                    None
                } else  if stage_info.copy_options.purge && force {
                    // if `purge-after-copy` is enabled, and in `force` copy mode,
                    // we do not need to upsert copied files into meta server
                    info!("[purge] and [force] are both enabled,  will not update copied-files set. ({})", &to_table.get_table_info().desc);
                    None
                } else {
                    let fail_if_duplicated = !force;
                    Self::upsert_copied_files_request(
                        expire_hours,
                        copied_file_tree,
                        fail_if_duplicated,
                    )
                }
            };

            {
                let status = format!("begin commit, number of copied files:{}", num_copied_files);
                ctx.set_status_info(&status);
                info!(status);
            }

            to_table
                .commit_insertion(
                    ctx.clone(),
                    operations,
                    upsert_copied_files_request,
                    false,
                )
                .await?;

            info!("end of commit");

            // 3. log on_error mode errors.
            // todo(ariesdevil): persist errors with query_id
            if let Some(error_map) = ctx.get_maximum_error_per_file() {
                for (file_name, e) in error_map {
                    error!(
                                "copy(on_error={}): file {} encounter error {},",
                                stage_info.copy_options.on_error,
                                file_name,
                                e.to_string()
                            );
                }
            }

            // 4. Try to purge copied files if purge option is true, if error will skip.
            // If a file is already copied(status with AlreadyCopied) we will try to purge them.
            if stage_info.copy_options.purge {
                CopyInterpreter::try_purge_files(ctx.clone(), &stage_info, &copied_files).await;
            }

            Ok(())
        })
    }

    fn upsert_copied_files_request(
        expire_hours: u64,
        copy_stage_files: BTreeMap<String, TableCopiedFileInfo>,
        fail_if_duplicated: bool,
    ) -> Option<UpsertTableCopiedFileReq> {
        if copy_stage_files.is_empty() {
            return None;
        }
        tracing::debug!("upsert_copied_files_info: {:?}", copy_stage_files);
        let expire_at = expire_hours * 60 * 60 + Utc::now().timestamp() as u64;
        let req = UpsertTableCopiedFileReq {
            file_info: copy_stage_files,
            expire_at: Some(expire_at),
            fail_if_duplicated,
        };
        Some(req)
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
        match &self.plan {
            CopyPlan::IntoTable(plan) => self.build_copy_into_table_pipeline(plan).await,

            CopyPlan::IntoStage {
                stage, from, path, ..
            } => self.build_copy_into_stage_pipeline(stage, path, from).await,
            CopyPlan::NoFileToCopy => Ok(PipelineBuildResult::create()),
        }
    }
}

fn fill_const_columns(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    input_schema: DataSchemaRef,
    output_schema: DataSchemaRef,
    const_values: Vec<Scalar>,
) -> Result<()> {
    pipeline.add_transform(|transform_input_port, transform_output_port| {
        TransformAddConstColumns::try_create(
            ctx.clone(),
            transform_input_port,
            transform_output_port,
            input_schema.clone(),
            output_schema.clone(),
            const_values.clone(),
        )
    })?;
    Ok(())
}
