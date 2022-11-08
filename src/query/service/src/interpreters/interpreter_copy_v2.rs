use std::collections::BTreeMap;
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
use std::sync::Arc;

use chrono::Utc;
use common_base::base::GlobalIORuntime;
use common_catalog::catalog::Catalog;
use common_catalog::plan::CopyInfo;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableCopiedFileInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_types::UserStageInfo;
use common_pipeline_sources::processors::sources::input_formats::InputContext;
use common_pipeline_transforms::processors::transforms::TransformLimit;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_storages_stage::StageFilePartition;
use common_storages_stage::StageFileStatus;
use common_storages_stage::StageTable;

use crate::interpreters::common::append2table;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreterV2;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;

const MAX_QUERY_COPIED_FILES_NUM: usize = 50;
const TABLE_COPIED_FILE_KEY_EXPIRE_AFTER_DAYS: Option<u64> = Some(7);

pub struct CopyInterpreterV2 {
    ctx: Arc<QueryContext>,
    plan: CopyPlanV2,
}

impl CopyInterpreterV2 {
    /// Create a CopyInterpreterV2 with context and [`CopyPlanV2`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlanV2) -> Result<Self> {
        Ok(CopyInterpreterV2 { ctx, plan })
    }

    async fn build_copy_into_stage_pipeline(
        &self,
        stage: &UserStageInfo,
        path: &str,
        query: &Plan,
    ) -> Result<PipelineBuildResult> {
        let (s_expr, metadata, bind_context) = match query {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => (s_expr, metadata, bind_context),
            v => unreachable!("Input plan must be Query, but it's {}", v),
        };

        let select_interpreter = SelectInterpreterV2::try_create(
            self.ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
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
        let stage_table_info = StageTableInfo {
            schema: data_schema.clone(),
            stage_info: stage.clone(),
            path: path.to_string(),
            files: vec![],
        };

        let mut build_res = select_interpreter.execute2().await?;
        let table = StageTable::try_create(stage_table_info)?;

        append2table(
            self.ctx.clone(),
            table.clone(),
            data_schema.clone(),
            &mut build_res,
            false,
            true,
            AppendMode::Normal,
        )?;
        Ok(build_res)
    }

    async fn do_upsert_copied_files_info_to_meta(
        expire_at: Option<u64>,
        tenant: String,
        database_name: String,
        table_id: u64,
        catalog: Arc<dyn Catalog>,
        copy_stage_files: &mut BTreeMap<String, TableCopiedFileInfo>,
    ) -> Result<()> {
        let req = UpsertTableCopiedFileReq {
            table_id,
            file_info: copy_stage_files.clone(),
            expire_at,
        };
        catalog
            .upsert_table_copied_file_info(&tenant, &database_name, req)
            .await?;
        copy_stage_files.clear();
        Ok(())
    }

    async fn upsert_copied_files_info_to_meta(
        tenant: String,
        database_name: String,
        table_id: u64,
        catalog: Arc<dyn Catalog>,
        copy_stage_files: BTreeMap<String, TableCopiedFileInfo>,
    ) -> Result<()> {
        tracing::debug!("upsert_copied_files_info: {:?}", copy_stage_files);

        if copy_stage_files.is_empty() {
            return Ok(());
        }

        let expire_at = TABLE_COPIED_FILE_KEY_EXPIRE_AFTER_DAYS
            .map(|after_days| after_days * 86400 + Utc::now().timestamp() as u64);
        let mut do_copy_stage_files = BTreeMap::new();
        for (file_name, file_info) in copy_stage_files {
            do_copy_stage_files.insert(file_name.clone(), file_info);
            if do_copy_stage_files.len() > MAX_QUERY_COPIED_FILES_NUM {
                CopyInterpreterV2::do_upsert_copied_files_info_to_meta(
                    expire_at,
                    tenant.clone(),
                    database_name.clone(),
                    table_id,
                    catalog.clone(),
                    &mut do_copy_stage_files,
                )
                .await?;
            }
        }
        if !do_copy_stage_files.is_empty() {
            CopyInterpreterV2::do_upsert_copied_files_info_to_meta(
                expire_at,
                tenant.clone(),
                database_name.clone(),
                table_id,
                catalog.clone(),
                &mut do_copy_stage_files,
            )
            .await?;
        }

        Ok(())
    }

    async fn try_purge_files(
        ctx: Arc<QueryContext>,
        stage_info: &UserStageInfo,
        stage_file_infos: &[StageFilePartition],
    ) -> Result<()> {
        if stage_info.copy_options.purge {
            let table_ctx: Arc<dyn TableContext> = ctx.clone();
            let op = StageTable::get_op(&table_ctx, stage_info)?;
            for file in stage_file_infos {
                if let Err(e) = op.object(&file.path).delete().await {
                    tracing::error!("Failed to delete file: {}, error: {}", file.path, e);
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_copy_into_table_pipeline(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        files: &[String],
        path: &str,
        pattern: &str,
        force: bool,
        stage_table_info: &StageTableInfo,
    ) -> Result<PipelineBuildResult> {
        let ctx = self.ctx.clone();
        let stage_table = StageTable::try_create(stage_table_info.clone())?;

        // Get the source plan with all files from stage_table.read_partitions:
        // 1. Need copy files(status with NeedCopy)
        // 2. Have copied files(status with AlreadyCopied)
        let read_source_plan = {
            let copy_info = CopyInfo {
                force,
                files: files.to_vec(),
                path: path.to_string(),
                pattern: pattern.to_string(),
                stage_info: stage_table_info.stage_info.clone(),
                into_table_catalog_name: catalog_name.to_string(),
                into_table_database_name: database_name.to_string(),
                into_table_name: table_name.to_string(),
            };
            let pushdown = PushDownInfo {
                projection: None,
                filters: vec![],
                prewhere: None,
                limit: None,
                order_by: vec![],
                copy: Some(copy_info),
            };
            stage_table
                .read_plan_with_catalog(ctx.clone(), catalog_name.to_string(), Some(pushdown))
                .await?
        };

        // All files info.
        let mut all_source_file_infos = vec![];
        // Need copied file info.
        let mut need_copied_file_infos = vec![];
        for part in &read_source_plan.parts {
            if let Some(stage_file_info) = part.as_any().downcast_ref::<StageFilePartition>() {
                all_source_file_infos.push(stage_file_info.clone());
                if stage_file_info.status == StageFileStatus::NeedCopy {
                    need_copied_file_infos.push(stage_file_info.clone());
                }
            }
        }

        // Build the NeedCopy source plan: remove the AlreadyCopied status files.
        let mut need_copy_source_plan = read_source_plan.clone();
        need_copy_source_plan.parts.retain(|v| {
            if let Some(sv) = v.as_any().downcast_ref::<StageFilePartition>() {
                return sv.status == StageFileStatus::NeedCopy;
            }
            false
        });

        // COPY into <table> table info.
        let to_table = ctx
            .get_table(catalog_name, database_name, table_name)
            .await?;
        stage_table.set_block_compact_thresholds(to_table.get_block_compact_thresholds());

        // Build pipeline.
        let mut build_res = PipelineBuildResult::create();
        {
            //  Build copy pipeline.
            let files = need_copied_file_infos
                .iter()
                .map(|v| v.path.clone())
                .collect::<Vec<_>>();

            let table_ctx: Arc<dyn TableContext> = ctx.clone();
            let operator = StageTable::get_op(&table_ctx, &stage_table_info.stage_info)?;
            let settings = ctx.get_settings();
            let schema = stage_table_info.schema.clone();
            let stage_info = stage_table_info.stage_info.clone();
            let compact_threshold = stage_table.get_block_compact_thresholds();
            let input_ctx = Arc::new(
                InputContext::try_create_from_copy(
                    operator,
                    settings,
                    schema,
                    stage_info,
                    files,
                    ctx.get_scan_progress(),
                    compact_threshold,
                )
                .await?,
            );
            input_ctx
                .format
                .exec_copy(input_ctx.clone(), &mut build_res.main_pipeline)?;

            // Build Limit pipeline.
            let limit = stage_table_info.stage_info.copy_options.size_limit;
            if limit > 0 {
                build_res.main_pipeline.resize(1)?;
                build_res.main_pipeline.add_transform(
                    |transform_input_port, transform_output_port| {
                        TransformLimit::try_create(
                            Some(limit),
                            0,
                            transform_input_port,
                            transform_output_port,
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
        }

        let stage_table_info_clone = stage_table_info.clone();
        let catalog = self.ctx.get_catalog(catalog_name)?;
        let tenant = self.ctx.get_tenant();
        let database_name = database_name.to_string();
        let table_id = to_table.get_id();
        build_res.main_pipeline.set_on_finished(move |may_error| {
            if may_error.is_none() {
                // capture out variable
                let ctx = ctx.clone();
                let to_table = to_table.clone();
                let stage_info = stage_table_info_clone.stage_info.clone();
                let all_source_files = all_source_file_infos.clone();
                let need_copied_files = need_copied_file_infos.clone();
                let tenant = tenant.clone();
                let database_name = database_name.clone();
                let catalog = catalog.clone();
                let mut copied_files = BTreeMap::new();
                for file in &need_copied_files {
                    copied_files.insert(file.path.clone(), TableCopiedFileInfo {
                        etag: file.etag.clone(),
                        content_length: file.size,
                        last_modified: Some(file.last_modified),
                    });
                }

                return GlobalIORuntime::instance().block_on(async move {
                    // 1. Commit datas.
                    let operations = ctx.consume_precommit_blocks();
                    to_table
                        .commit_insertion(ctx.clone(), operations, false)
                        .await?;

                    // 2. Try to purge copied files if purge option is true, if error will skip.
                    // If a file is already copied(status with AlreadyCopied) we will try to purge them.
                    CopyInterpreterV2::try_purge_files(ctx.clone(), &stage_info, &all_source_files)
                        .await?;

                    // 3. Upsert files(status with NeedCopy) info to meta.
                    CopyInterpreterV2::upsert_copied_files_info_to_meta(
                        tenant,
                        database_name,
                        table_id,
                        catalog,
                        copied_files,
                    )
                    .await
                });
            }
            Err(may_error.as_ref().unwrap().clone())
        });

        Ok(build_res)
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreterV2 {
    fn name(&self) -> &str {
        "CopyInterpreterV2"
    }

    #[tracing::instrument(level = "debug", name = "copy_interpreter_execute_v2", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        match &self.plan {
            CopyPlanV2::IntoTable {
                catalog_name,
                database_name,
                table_name,
                files,
                pattern,
                from,
                force,
                ..
            } => match &from.source_info {
                DataSourceInfo::StageSource(table_info) => {
                    let path = &table_info.path;
                    self.build_copy_into_table_pipeline(
                        catalog_name,
                        database_name,
                        table_name,
                        files,
                        path,
                        pattern,
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
            CopyPlanV2::IntoStage {
                stage, from, path, ..
            } => self.build_copy_into_stage_pipeline(stage, path, from).await,
        }
    }
}
