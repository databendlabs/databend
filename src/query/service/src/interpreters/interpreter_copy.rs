// Copyright 2022 Datafuse Labs.
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
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_meta_app::principal::StageInfo;
use common_meta_app::schema::TableCopiedFileInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_types::MetaId;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_storage::StageFileInfo;
use common_storage::StageFileStatus;
use common_storage::StageFilesInfo;
use common_storages_fuse::io::Files;
use common_storages_stage::StageTable;
use tracing::error;
use tracing::info;

use crate::interpreters::common::append2table;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::processors::transforms::TransformRuntimeCastSchema;
use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::processors::TransformLimit;
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

    async fn try_purge_files(
        ctx: Arc<QueryContext>,
        stage_info: &StageInfo,
        stage_file_infos: &[StageFileInfo],
    ) {
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
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_copy_into_table_with_transform_pipeline(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        query: &Plan,
        stage_info: StageInfo,
        all_source_file_infos: Vec<StageFileInfo>,
        need_copy_file_infos: Vec<StageFileInfo>,
        force: bool,
    ) -> Result<PipelineBuildResult> {
        let start = Instant::now();
        let ctx = self.ctx.clone();
        let (mut build_res, source_schema) = self.build_query(query).await?;
        let to_table = ctx
            .get_table(catalog_name, database_name, table_name)
            .await?;

        let dst_schema = Arc::new(to_table.schema().into());
        if source_schema != dst_schema {
            let func_ctx = ctx.get_function_context()?;
            build_res.main_pipeline.add_transform(
                |transform_input_port, transform_output_port| {
                    TransformCastSchema::try_create(
                        transform_input_port,
                        transform_output_port,
                        source_schema.clone(),
                        dst_schema.clone(),
                        func_ctx,
                    )
                },
            )?;
        }

        // Build append data pipeline.
        to_table.append_data(
            ctx.clone(),
            &mut build_res.main_pipeline,
            AppendMode::Copy,
            false,
        )?;

        build_res.main_pipeline.set_on_finished(move |may_error| {
            if may_error.is_none() {
                CopyInterpreter::commit_copy_into_table(
                    ctx.clone(),
                    to_table,
                    stage_info,
                    all_source_file_infos,
                    need_copy_file_infos,
                    force,
                )?;
                // Status.
                {
                    info!("all copy finished, elapsed:{}", start.elapsed().as_secs());
                }
                Ok(())
            } else {
                Err(may_error.as_ref().unwrap().clone())
            }
        });

        Ok(build_res)
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_copy_into_table_pipeline(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        force: bool,
        stage_table_info: &StageTableInfo,
    ) -> Result<PipelineBuildResult> {
        let start = Instant::now();
        let ctx = self.ctx.clone();
        let table_ctx: Arc<dyn TableContext> = ctx.clone();

        // Status.
        {
            let status = "begin to list files";
            ctx.set_status_info(status);
            info!(status);
        }

        let mut stage_table_info = stage_table_info.clone();
        let max_files = stage_table_info.stage_info.copy_options.max_files;
        let max_files = if max_files == 0 {
            None
        } else {
            Some(max_files)
        };

        let mut all_source_file_infos = if force {
            StageTable::list_files(&stage_table_info, max_files).await?
        } else {
            StageTable::list_files(&stage_table_info, None).await?
        };

        info!("end to list files: {}", all_source_file_infos.len());

        if !force {
            // Status.
            {
                let status = "begin to color copied files";
                ctx.set_status_info(status);
                info!(status);
            }

            all_source_file_infos = table_ctx
                .color_copied_files(
                    catalog_name,
                    database_name,
                    table_name,
                    all_source_file_infos,
                    max_files,
                )
                .await?;

            info!("end to color copied files: {}", all_source_file_infos.len());
        }

        let mut need_copy_file_infos = vec![];
        for file in &all_source_file_infos {
            if file.status == StageFileStatus::NeedCopy {
                need_copy_file_infos.push(file.clone());
            }
        }

        info!(
            "copy: read all files finished, all:{}, need copy:{}, elapsed:{}",
            all_source_file_infos.len(),
            need_copy_file_infos.len(),
            start.elapsed().as_secs()
        );

        let mut build_res = PipelineBuildResult::create();
        if need_copy_file_infos.is_empty() {
            return Ok(build_res);
        }

        // Status.
        {
            let status = "begin to read stage source plan";
            ctx.set_status_info(status);
            info!(status);
        }

        stage_table_info.files_to_copy = Some(need_copy_file_infos.clone());
        let stage_table = StageTable::try_create(stage_table_info.clone())?;
        let read_source_plan = {
            stage_table
                .read_plan_with_catalog(ctx.clone(), catalog_name.to_string(), None, None)
                .await?
        };

        // Status.
        {
            let status = format!(
                "begin to read stage table data, parts:{}",
                read_source_plan.parts.len()
            );
            ctx.set_status_info(&status);
            info!(status);
        }

        let to_table = ctx
            .get_table(catalog_name, database_name, table_name)
            .await?;
        stage_table.set_block_compact_thresholds(to_table.get_block_compact_thresholds());
        stage_table.read_data(table_ctx, &read_source_plan, &mut build_res.main_pipeline)?;

        // Build Limit pipeline.
        let limit = stage_table_info.stage_info.copy_options.size_limit;
        if limit > 0 {
            build_res.main_pipeline.resize(1)?;
            build_res.main_pipeline.add_transform(
                |transform_input_port, transform_output_port| {
                    Ok(ProcessorPtr::create(TransformLimit::try_create(
                        Some(limit),
                        0,
                        transform_input_port,
                        transform_output_port,
                    )?))
                },
            )?;
        }

        if stage_table_info
            .stage_info
            .file_format_options
            .format
            .has_inner_schema()
        {
            let dst_schema: Arc<DataSchema> = Arc::new(to_table.schema().into());
            let func_ctx = self.ctx.get_function_context()?;
            build_res.main_pipeline.add_transform(
                |transform_input_port, transform_output_port| {
                    TransformRuntimeCastSchema::try_create(
                        transform_input_port,
                        transform_output_port,
                        dst_schema.clone(),
                        func_ctx,
                    )
                },
            )?;
        }

        // Build append data pipeline.
        to_table.append_data(
            ctx.clone(),
            &mut build_res.main_pipeline,
            AppendMode::Copy,
            false,
        )?;

        let stage_table_info_clone = stage_table_info.clone();
        build_res.main_pipeline.set_on_finished(move |may_error| {
            if may_error.is_none() {
                CopyInterpreter::commit_copy_into_table(
                    ctx.clone(),
                    to_table,
                    stage_table_info_clone.stage_info,
                    all_source_file_infos,
                    need_copy_file_infos,
                    force,
                )?;
                // Status.
                {
                    info!("all copy finished, elapsed:{}", start.elapsed().as_secs());
                }
                Ok(())
            } else {
                Err(may_error.as_ref().unwrap().clone())
            }
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
        all_source_files: Vec<StageFileInfo>,
        need_copy_files: Vec<StageFileInfo>,
        force: bool,
    ) -> Result<()> {
        let mut copied_files = BTreeMap::new();
        for file in need_copy_files {
            // Short the etag to 7 bytes for less space in metasrv.
            let short_etag = file.etag.clone().map(|mut v| {
                v.truncate(7);
                v
            });
            copied_files.insert(file.path.clone(), TableCopiedFileInfo {
                etag: short_etag,
                content_length: file.size,
                last_modified: Some(file.last_modified),
            });
        }

        GlobalIORuntime::instance().block_on(async move {
            // 1. Commit data to table.
            let operations = ctx.consume_precommit_blocks();

            let table_id = to_table.get_id();
            let expire_hours = ctx.get_settings().get_load_file_metadata_expire_hours()?;
            let num_copied_files = copied_files.len();

            let fail_if_duplicated = !force;
            let upsert_copied_files_request = Self::upsert_copied_files_request(
                table_id,
                expire_hours,
                copied_files,
                fail_if_duplicated,
            );

            {
                let status = format!("begin commit, number of copied files:{}", num_copied_files,);
                ctx.set_status_info(&status);
                info!(status);
            }

            let overwrite_table_data = false;
            to_table
                .commit_insertion(
                    ctx.clone(),
                    operations,
                    upsert_copied_files_request,
                    overwrite_table_data,
                )
                .await?;

            info!("end of commit");

            // 3. log on_error mode errors.
            // todo(ariesdevil): persist errors with query_id
            if let Some(error_map) = ctx.get_on_error_map() {
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
                let purge_start = Instant::now();

                // Status.
                {
                    let status = format!("begin to purge files:{}", all_source_files.len());
                    ctx.set_status_info(&status);
                    info!(status);
                }

                CopyInterpreter::try_purge_files(ctx.clone(), &stage_info, &all_source_files).await;

                // Status.
                info!(
                    "end to purge files:{}, elapsed:{}",
                    all_source_files.len(),
                    purge_start.elapsed().as_secs()
                );
            }

            Ok(())
        })
    }

    fn upsert_copied_files_request(
        table_id: MetaId,
        expire_hours: u64,
        copy_stage_files: BTreeMap<String, TableCopiedFileInfo>,
        fail_if_duplicated: bool,
    ) -> Option<UpsertTableCopiedFileReq> {
        if copy_stage_files.is_empty() {
            return None;
        }
        tracing::debug!("upsert_copied_files_info: {:?}", copy_stage_files);
        let expire_at = expire_hours * 60 + Utc::now().timestamp() as u64;
        let req = UpsertTableCopiedFileReq {
            table_id,
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
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        match &self.plan {
            CopyPlan::IntoTable {
                catalog_name,
                database_name,
                table_name,
                from,
                force,
                ..
            } => match &from.source_info {
                DataSourceInfo::StageSource(table_info) => {
                    self.build_copy_into_table_pipeline(
                        catalog_name,
                        database_name,
                        table_name,
                        *force,
                        table_info,
                    )
                    .await
                }
                other => Err(ErrorCode::Internal(format!(
                    "Cannot list files for the source info: {:?}",
                    other
                ))),
            },
            CopyPlan::IntoTableWithTransform {
                catalog_name,
                database_name,
                table_name,
                stage_info,
                from,
                all_source_file_infos,
                need_copy_file_infos,
                force,
                ..
            } => {
                self.build_copy_into_table_with_transform_pipeline(
                    catalog_name,
                    database_name,
                    table_name,
                    from,
                    *stage_info.clone(),
                    all_source_file_infos.clone(),
                    need_copy_file_infos.clone(),
                    *force,
                )
                .await
            }
            CopyPlan::IntoStage {
                stage, from, path, ..
            } => self.build_copy_into_stage_pipeline(stage, path, from).await,
        }
    }
}
