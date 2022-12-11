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
use common_catalog::catalog::Catalog;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::StageFileInfo;
use common_catalog::plan::StageFileStatus;
use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::TableCopiedFileInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_types::UserStageInfo;
use common_pipeline_transforms::processors::transforms::TransformLimit;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use common_storages_fuse::io::Files;
use common_storages_stage::StageTable;
use tracing::error;
use tracing::info;

use crate::interpreters::common::append2table;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreterV2;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;

const MAX_QUERY_COPIED_FILES_NUM: usize = 50;

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
        let stage_table_info = StageTableInfo {
            schema: data_schema.clone(),
            user_stage_info: stage.clone(),
            path: path.to_string(),
            files: vec![],
            pattern: "".to_string(),
            files_to_copy: None,
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
        ctx: &Arc<QueryContext>,
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

        let expire_hours = ctx.get_settings().get_load_file_metadata_expire_hours()?;
        let expire_at = expire_hours * 60 + Utc::now().timestamp() as u64;
        let mut do_copy_stage_files = BTreeMap::new();
        for (file_name, file_info) in copy_stage_files {
            do_copy_stage_files.insert(file_name.clone(), file_info);
            if do_copy_stage_files.len() > MAX_QUERY_COPIED_FILES_NUM {
                CopyInterpreterV2::do_upsert_copied_files_info_to_meta(
                    Some(expire_at),
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
                Some(expire_at),
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

    pub async fn color_copied_files(
        ctx: &Arc<dyn TableContext>,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        files: Vec<StageFileInfo>,
    ) -> Result<Vec<StageFileInfo>> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(catalog_name)?;
        let table = catalog
            .get_table(&tenant, database_name, table_name)
            .await?;
        let table_id = table.get_id();

        let mut copied_files = BTreeMap::new();
        for chunk in files.chunks(MAX_QUERY_COPIED_FILES_NUM) {
            let files = chunk.iter().map(|v| v.path.clone()).collect::<Vec<_>>();
            let req = GetTableCopiedFileReq { table_id, files };
            let resp = catalog
                .get_table_copied_file_info(&tenant, database_name, req)
                .await?;
            copied_files.extend(resp.file_info);
        }

        // Colored.
        let mut results = vec![];
        for mut file in files {
            if let Some(copied_file) = copied_files.get(&file.path) {
                match &copied_file.etag {
                    Some(copied_etag) => {
                        if let Some(file_etag) = &file.etag {
                            // Check the 7 bytes etag prefix.
                            if file_etag.starts_with(copied_etag) {
                                file.status = StageFileStatus::AlreadyCopied;
                            }
                        }
                    }
                    None => {
                        // etag is none, compare with content_length and last_modified.
                        if copied_file.content_length == file.size
                            && copied_file.last_modified == Some(file.last_modified)
                        {
                            file.status = StageFileStatus::AlreadyCopied;
                        }
                    }
                }
            }
            results.push(file);
        }
        Ok(results)
    }

    async fn try_purge_files(
        ctx: Arc<QueryContext>,
        stage_info: &UserStageInfo,
        stage_file_infos: &[StageFileInfo],
    ) {
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let op = StageTable::get_op(&table_ctx, stage_info);
        match op {
            Ok(op) => {
                let file = Files::create(table_ctx, op);
                let files = stage_file_infos
                    .iter()
                    .map(|v| v.path.clone())
                    .collect::<Vec<_>>();
                if let Err(e) = file.remove_file_in_batch(&files).await {
                    error!("Failed to delete file: {:?}, error: {}", files, e);
                }
            }
            Err(e) => {
                error!("Failed to get stage table op, error: {}", e);
            }
        }
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
        let mut stage_table_info = stage_table_info.clone();
        let mut all_source_file_infos =
            StageTable::list_files(&table_ctx, &stage_table_info).await?;

        if !force {
            all_source_file_infos = CopyInterpreterV2::color_copied_files(
                &table_ctx,
                catalog_name,
                database_name,
                table_name,
                all_source_file_infos,
            )
            .await?;

            // Need copied file info.
        }

        let mut need_copied_file_infos = vec![];
        for file in &all_source_file_infos {
            if file.status == StageFileStatus::NeedCopy {
                need_copied_file_infos.push(file.clone());
            }
        }

        info!(
            "copy: read all files finished, all:{}, need copy:{}, elapsed:{}",
            all_source_file_infos.len(),
            need_copied_file_infos.len(),
            start.elapsed().as_secs()
        );

        let mut build_res = PipelineBuildResult::create();
        if need_copied_file_infos.is_empty() {
            return Ok(build_res);
        }

        stage_table_info.files_to_copy = Some(need_copied_file_infos.clone());

        let stage_table = StageTable::try_create(stage_table_info.clone())?;
        let read_source_plan = {
            stage_table
                .read_plan_with_catalog(ctx.clone(), catalog_name.to_string(), None)
                .await?
        };

        let to_table = ctx
            .get_table(catalog_name, database_name, table_name)
            .await?;
        stage_table.set_block_compact_thresholds(to_table.get_block_compact_thresholds());
        stage_table.read_data(table_ctx, &read_source_plan, &mut build_res.main_pipeline)?;

        // Build Limit pipeline.
        let limit = stage_table_info.user_stage_info.copy_options.size_limit;
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

        // Pipeline finish.
        // 1. commit the data
        // 2. purge the copied files
        // 3. update the NeedCopy file into to meta
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
                let stage_info = stage_table_info_clone.user_stage_info.clone();
                let all_source_files = all_source_file_infos.clone();
                let need_copied_files = need_copied_file_infos.clone();
                let tenant = tenant.clone();
                let database_name = database_name.clone();
                let catalog = catalog.clone();

                let mut copied_files = BTreeMap::new();
                for file in &need_copied_files {
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

                return GlobalIORuntime::instance().block_on(async move {
                    // 1. Commit datas.
                    let operations = ctx.consume_precommit_blocks();
                    info!(
                        "copy: try to commit operations:{}, elapsed:{}",
                        operations.len(),
                        start.elapsed().as_secs()
                    );
                    to_table
                        .commit_insertion(ctx.clone(), operations, false)
                        .await?;

                    // 2. Try to purge copied files if purge option is true, if error will skip.
                    // If a file is already copied(status with AlreadyCopied) we will try to purge them.

                    if stage_info.copy_options.purge {
                        info!(
                            "copy: try to purge files:{}, elapsed:{}",
                            all_source_files.len(),
                            start.elapsed().as_secs()
                        );
                        CopyInterpreterV2::try_purge_files(
                            ctx.clone(),
                            &stage_info,
                            &all_source_files,
                        )
                        .await;
                    }

                    // 3. Upsert files(status with NeedCopy) info to meta.
                    info!(
                        "copy: try to upsert file infos:{} to meta, elapsed:{}",
                        copied_files.len(),
                        start.elapsed().as_secs()
                    );
                    CopyInterpreterV2::upsert_copied_files_info_to_meta(
                        &ctx,
                        tenant,
                        database_name,
                        table_id,
                        catalog,
                        copied_files,
                    )
                    .await?;

                    info!(
                        "copy: all copy finished, elapsed:{}",
                        start.elapsed().as_secs()
                    );

                    Ok(())
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
            CopyPlanV2::IntoStage {
                stage, from, path, ..
            } => self.build_copy_into_stage_pipeline(stage, path, from).await,
        }
    }
}
