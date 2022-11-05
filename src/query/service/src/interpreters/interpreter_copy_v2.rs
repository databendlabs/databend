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
use std::path::Path;
use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::TableCopiedFileInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_types::StageFile;
use common_meta_types::UserStageInfo;
use common_planner::stage_table::StageTableInfo;
use common_planner::ReadDataSourcePlan;
use common_planner::SourceInfo;
use common_storages_stage::StageTable;
use regex::Regex;

use crate::catalogs::Catalog;
use crate::interpreters::common::append2table;
use crate::interpreters::common::list_files;
use crate::interpreters::common::stat_file;
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

    async fn get_copied_files_info(
        &self,
        catalog_name: String,
        database_name: String,
        table_id: u64,
        query_copied_files: &[String],
        file_info: &mut BTreeMap<String, TableCopiedFileInfo>,
    ) -> Result<()> {
        let catalog = self.ctx.get_catalog(&catalog_name)?;
        let tenant = self.ctx.get_tenant();
        let req = GetTableCopiedFileReq {
            table_id,
            files: query_copied_files.to_owned(),
        };
        let resp = catalog
            .get_table_copied_file_info(&tenant, &database_name, req)
            .await?;

        file_info.extend(resp.file_info);

        Ok(())
    }

    async fn do_upsert_copied_files_info(
        expire_at: Option<u64>,
        tenant: String,
        database_name: String,
        table_id: u64,
        copy_stage_files: &mut BTreeMap<String, TableCopiedFileInfo>,
        catalog: Arc<dyn Catalog>,
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

    async fn filter_have_copied_files(
        &self,
        force: bool,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        stage_files: &[StageFile],
    ) -> Result<(u64, BTreeMap<String, TableCopiedFileInfo>)> {
        let catalog = self.ctx.get_catalog(catalog_name)?;
        let tenant = self.ctx.get_tenant();
        let table = catalog
            .get_table(&tenant, database_name, table_name)
            .await?;
        let table_id = table.get_id();

        let mut file_map = BTreeMap::new();

        if !force {
            // if force is false, copy only the files that unmatch to the meta copied files info.
            let files = stage_files
                .iter()
                .map(|v| v.path.clone())
                .collect::<Vec<_>>();
            let mut copied_files = BTreeMap::new();
            for query_copied_files in files.chunks(MAX_QUERY_COPIED_FILES_NUM) {
                self.get_copied_files_info(
                    catalog_name.to_string(),
                    database_name.to_string(),
                    table_id,
                    query_copied_files,
                    &mut copied_files,
                )
                .await?;
            }

            for stage_file in stage_files {
                if let Some(copied_file) = copied_files.get(&stage_file.path) {
                    match &copied_file.etag {
                        Some(_etag) => {
                            // No need to copy the file again if etag is_some and match.
                            if stage_file.etag == copied_file.etag {
                                tracing::warn!(
                                    "ignore copy file {:?} matched by etag",
                                    copied_file
                                );
                                continue;
                            }
                        }
                        None => {
                            // etag is none, compare with content_length and last_modified.
                            if copied_file.content_length == stage_file.size
                                && copied_file.last_modified == Some(stage_file.last_modified)
                            {
                                tracing::warn!(
                                    "ignore copy file {:?} matched by content_length and last_modified",
                                    copied_file
                                );
                                continue;
                            }
                        }
                    }
                }

                // unmatch case: insert into file map for copy.
                file_map.insert(stage_file.path.clone(), TableCopiedFileInfo {
                    etag: stage_file.etag.clone(),
                    content_length: stage_file.size,
                    last_modified: Some(stage_file.last_modified),
                });
            }
        } else {
            // if force is true, copy all the file.
            for stage_file in stage_files {
                file_map.insert(stage_file.path.clone(), TableCopiedFileInfo {
                    etag: stage_file.etag.clone(),
                    content_length: stage_file.size,
                    last_modified: Some(stage_file.last_modified),
                });
            }
        }
        Ok((table_id, file_map))
    }

    async fn upsert_copied_files_info(
        tenant: String,
        database_name: String,
        table_id: u64,
        copy_stage_files: BTreeMap<String, TableCopiedFileInfo>,
        catalog: Arc<dyn Catalog>,
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
                CopyInterpreterV2::do_upsert_copied_files_info(
                    expire_at,
                    tenant.clone(),
                    database_name.clone(),
                    table_id,
                    &mut do_copy_stage_files,
                    catalog.clone(),
                )
                .await?;
            }
        }
        if !do_copy_stage_files.is_empty() {
            CopyInterpreterV2::do_upsert_copied_files_info(
                expire_at,
                tenant.clone(),
                database_name.clone(),
                table_id,
                &mut do_copy_stage_files,
                catalog.clone(),
            )
            .await?;
        }

        Ok(())
    }

    async fn purge_files(
        ctx: Arc<QueryContext>,
        from: &ReadDataSourcePlan,
        files: &Vec<String>,
    ) -> Result<()> {
        match &from.source_info {
            SourceInfo::StageSource(table_info) => {
                if table_info.stage_info.copy_options.purge {
                    let rename_me: Arc<dyn TableContext> = ctx.clone();
                    let op = StageTable::get_op(&rename_me, &table_info.stage_info)?;
                    for f in files {
                        if let Err(e) = op.object(f).delete().await {
                            tracing::error!("Failed to delete file: {}, error: {}", f, e);
                        }
                    }
                    tracing::info!("purge files: {:?}", files);
                }
                Ok(())
            }
            other => Err(ErrorCode::Internal(format!(
                "Cannot list files for the source info: {:?}",
                other
            ))),
        }
    }

    /// Rewrite the ReadDataSourcePlan.S3StageSource.file_name to new file name.
    fn rewrite_read_plan_file_name(
        mut plan: ReadDataSourcePlan,
        files: &[String],
    ) -> ReadDataSourcePlan {
        if let SourceInfo::StageSource(ref mut stage) = plan.source_info {
            stage.files = files.to_vec()
        }
        plan
    }

    // Read a file and commit it to the table.
    // Progress:
    // 1. Build a select pipeline
    // 2. Execute the pipeline and get the stream
    // 3. Read from the stream and write to the table.
    // Note:
    //  We parse the `s3://` to ReadSourcePlan instead of to a SELECT plan is that:
    #[tracing::instrument(level = "debug", name = "copy_files_to_table", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn copy_files_to_table(
        &self,
        catalog_name: &String,
        db_name: &String,
        tbl_name: &String,
        table_id: u64,
        from: &ReadDataSourcePlan,
        copy_stage_files: BTreeMap<String, TableCopiedFileInfo>,
    ) -> Result<PipelineBuildResult> {
        // let mut v = [5, 4, 1, 3, 2];
        // v.sort_by(|a, b| a.cmp(b));
        // assert!(v == [1, 2, 3, 4, 5]);
        // https://play.rust-lang.org/?version=stable&mode=debug&edition=2015&gist=aefe5b6d2d4340aa7453b6d62452cbdc
        let mut copy_files_vec = Vec::from_iter(copy_stage_files.clone());
        copy_files_vec.sort_by(|(_, a), (_, b)| a.last_modified.cmp(&b.last_modified));
        let files: Vec<String> = copy_files_vec.iter().map(|(k, _)| k.clone()).collect();

        let mut build_res = PipelineBuildResult::create();
        let read_source_plan = Self::rewrite_read_plan_file_name(from.clone(), &files);
        tracing::debug!("copy_files_to_table from source: {:?}", read_source_plan);

        let from_table = self.ctx.build_table_from_source_plan(&read_source_plan)?;
        let to_table = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;
        from_table.set_block_compact_thresholds(to_table.get_block_compact_thresholds());

        from_table.read_partitions(self.ctx.clone(), None).await?;
        from_table.read_data(
            self.ctx.clone(),
            &read_source_plan,
            &mut build_res.main_pipeline,
        )?;

        to_table.append_data(self.ctx.clone(), &mut build_res.main_pipeline, false)?;

        let ctx = self.ctx.clone();
        let files = files.clone();
        let from = from.clone();
        let catalog_name = catalog_name.clone();
        let db_name = db_name.clone();
        let catalog = self.ctx.get_catalog(&catalog_name)?;
        let tenant = self.ctx.get_tenant();

        build_res.main_pipeline.set_on_finished(move |may_error| {
            if may_error.is_none() {
                // capture out variable
                let ctx = ctx.clone();
                let files = files.clone();
                let from = from.clone();
                let to_table = to_table.clone();
                let copy_stage_files = copy_stage_files.clone();
                let db_name = db_name.clone();
                let catalog = catalog.clone();
                let tenant = tenant.clone();

                return GlobalIORuntime::instance().block_on(async move {
                    // Commit
                    let operations = ctx.consume_precommit_blocks();
                    to_table
                        .commit_insertion(ctx.clone(), operations, false)
                        .await?;

                    // Purge
                    CopyInterpreterV2::purge_files(ctx, &from, &files).await?;

                    // Upsert table copied file info.
                    CopyInterpreterV2::upsert_copied_files_info(
                        tenant,
                        db_name,
                        table_id,
                        copy_stage_files,
                        catalog,
                    )
                    .await
                });
            }

            Err(may_error.as_ref().unwrap().clone())
        });

        Ok(build_res)
    }

    async fn execute_copy_into_stage(
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
        )?;
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
            // TODO(xuanwo): extract them as a separate function.
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
                SourceInfo::StageSource(table_info) => {
                    let path = &table_info.path;

                    let mut stage_files = if !files.is_empty() {
                        let mut res = vec![];

                        for file in files {
                            // Here we add the path to the file: /path/to/path/file1.
                            let new_path = Path::new(path).join(file).to_string_lossy().to_string();
                            let info =
                                stat_file(&self.ctx, &table_info.stage_info, &new_path).await?;
                            res.push(info);
                        }
                        res
                    } else {
                        list_files(&self.ctx.clone(), &table_info.stage_info, path).await?
                    };

                    // Pattern match check.
                    {
                        let pattern = &pattern;
                        if !pattern.is_empty() {
                            let regex = Regex::new(pattern).map_err(|e| {
                                ErrorCode::SyntaxException(format!(
                                    "Pattern format invalid, got:{}, error:{:?}",
                                    pattern, e
                                ))
                            })?;
                            stage_files.retain(|v| regex.is_match(&v.path));
                        }
                        tracing::debug!("matched files: {:?}, pattern: {}", stage_files, pattern);
                    }

                    let (table_id, copy_stage_files) = self
                        .filter_have_copied_files(
                            *force,
                            catalog_name,
                            database_name,
                            table_name,
                            &stage_files,
                        )
                        .await?;

                    tracing::debug!(
                        "matched copy unduplicate files: {:?}",
                        &copy_stage_files.keys(),
                    );

                    if copy_stage_files.is_empty() {
                        return Ok(PipelineBuildResult::create());
                    }

                    self.copy_files_to_table(
                        catalog_name,
                        database_name,
                        table_name,
                        table_id,
                        from,
                        copy_stage_files,
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
            } => self.execute_copy_into_stage(stage, path, from).await,
        }
    }
}
