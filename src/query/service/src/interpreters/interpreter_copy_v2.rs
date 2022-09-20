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
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_base::base::TrySpawn;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::ReadDataSourcePlan;
use common_legacy_planners::SourceInfo;
use common_legacy_planners::StageTableInfo;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::TableCopiedFileInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_types::UserStageInfo;
use futures::TryStreamExt;
use regex::Regex;

use super::append2table;
use crate::interpreters::interpreter_common::stat_file;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreterV2;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;
use crate::storages::stage::StageSourceHelper;
use crate::storages::stage::StageTable;

pub struct CopyInterpreterV2 {
    ctx: Arc<QueryContext>,
    plan: CopyPlanV2,
}

impl CopyInterpreterV2 {
    /// Create a CopyInterpreterV2 with context and [`CopyPlanV2`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlanV2) -> Result<Self> {
        Ok(CopyInterpreterV2 { ctx, plan })
    }

    async fn filter_duplicate_files(
        &self,
        force: bool,
        table_info: &StageTableInfo,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        files: &[String],
    ) -> Result<(u64, BTreeMap<String, TableCopiedFileInfo>)> {
        let catalog = self.ctx.get_catalog(catalog_name)?;
        let tenant = self.ctx.get_tenant();
        let table = catalog
            .get_table(&tenant, database_name, table_name)
            .await?;
        let table_id = table.get_id();
        let req = GetTableCopiedFileReq {
            table_id,
            files: files.to_owned(),
        };
        let mut file_map = BTreeMap::new();

        if !force {
            // if force is false, copy only the files that unmatch to the meta copied files info.
            let resp = catalog.get_table_copied_file_info(req).await?;
            for file in files.iter() {
                let stage_file = stat_file(&self.ctx, &table_info.stage_info, file).await?;

                if let Some(file_info) = resp.file_info.get(file) {
                    match &file_info.etag {
                        Some(_etag) => {
                            // No need to copy the file again if etag is_some and match.
                            if stage_file.etag == file_info.etag {
                                tracing::warn!("ignore copy file {:?} matched by etag", file);
                                continue;
                            }
                        }
                        None => {
                            // etag is none, compare with content_length and last_modified.
                            if file_info.content_length == stage_file.size
                                && file_info.last_modified == Some(stage_file.last_modified)
                            {
                                tracing::warn!(
                                    "ignore copy file {:?} matched by content_length and last_modified",
                                    file
                                );
                                continue;
                            }
                        }
                    }
                }

                // unmatch case: insert into file map for copy.
                file_map.insert(file.clone(), TableCopiedFileInfo {
                    etag: stage_file.etag.clone(),
                    content_length: stage_file.size,
                    last_modified: Some(stage_file.last_modified),
                });
            }
        } else {
            // if force is true, copy all the file.
            for file in files.iter() {
                let stage_file = stat_file(&self.ctx, &table_info.stage_info, file).await?;

                file_map.insert(file.clone(), TableCopiedFileInfo {
                    etag: stage_file.etag.clone(),
                    content_length: stage_file.size,
                    last_modified: Some(stage_file.last_modified),
                });
            }
        }
        Ok((table_id, file_map))
    }

    async fn upsert_copied_files_info(
        &self,
        catalog_name: &str,
        table_id: u64,
        copy_stage_files: BTreeMap<String, TableCopiedFileInfo>,
    ) -> Result<()> {
        tracing::info!("upsert_copied_files_info: {:?}", copy_stage_files);

        if !copy_stage_files.is_empty() {
            let req = UpsertTableCopiedFileReq {
                table_id,
                file_info: copy_stage_files.clone(),
                expire_at: None,
            };
            let catalog = self.ctx.get_catalog(catalog_name)?;
            catalog.upsert_table_copied_file_info(req).await?;
        }
        Ok(())
    }

    /// List the files.
    /// There are two cases here:
    /// 1. If the plan.files is not empty, we already set the files sets to the COPY command with: `files=(<file1>, <file2>)` syntax, only need to add the prefix to the file.
    /// 2. If the plan.files is empty, there are also two case:
    ///     2.1 If the path is a file like /path/to/path/file, S3File::list() will return the same file path.
    ///     2.2 If the path is a folder, S3File::list() will return all the files in it.
    ///
    /// TODO(xuanwo): Align with interpreters/interpreter_common.rs `list_files`
    async fn list_files(
        &self,
        from: &ReadDataSourcePlan,
        files: &Vec<String>,
    ) -> Result<Vec<String>> {
        match &from.source_info {
            SourceInfo::StageSource(table_info) => {
                let path = &table_info.path;
                // Here we add the path to the file: /path/to/path/file1.
                let files_with_path = if !files.is_empty() {
                    let mut files_with_path = vec![];
                    for file in files {
                        let new_path = Path::new(path).join(file);
                        files_with_path.push(new_path.to_string_lossy().to_string());
                    }
                    files_with_path
                } else if !path.ends_with('/') {
                    let rename_me: Arc<dyn TableContext> = self.ctx.clone();
                    let op = StageSourceHelper::get_op(&rename_me, &table_info.stage_info).await?;
                    if op.object(path).is_exist().await? {
                        vec![path.to_string()]
                    } else {
                        vec![]
                    }
                } else {
                    let rename_me: Arc<dyn TableContext> = self.ctx.clone();
                    let op = StageSourceHelper::get_op(&rename_me, &table_info.stage_info).await?;

                    // TODO: Workaround for OpenDAL's bug: https://github.com/datafuselabs/opendal/issues/670
                    // Should be removed after OpenDAL fixes.
                    let mut list = HashSet::new();

                    // TODO: we could rewrite into try_collect.
                    let mut objects = op.batch().walk_top_down(path)?;
                    while let Some(de) = objects.try_next().await? {
                        if de.mode().is_dir() {
                            continue;
                        }
                        list.insert(de.path().to_string());
                    }

                    list.into_iter().collect::<Vec<_>>()
                };

                tracing::info!("listed files: {:?}", &files_with_path);

                Ok(files_with_path)
            }
            other => Err(ErrorCode::LogicalError(format!(
                "Cannot list files for the source info: {:?}",
                other
            ))),
        }
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
                    let op = StageSourceHelper::get_op(&rename_me, &table_info.stage_info).await?;
                    for f in files {
                        if let Err(e) = op.object(f).delete().await {
                            tracing::error!("Failed to delete file: {}, error: {}", f, e);
                        }
                    }
                    tracing::info!("purge files: {:?}", files);
                }
                Ok(())
            }
            other => Err(ErrorCode::LogicalError(format!(
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
        from: &ReadDataSourcePlan,
        files: Vec<String>,
    ) -> Result<PipelineBuildResult> {
        let mut build_res = PipelineBuildResult::create();

        let read_source_plan = Self::rewrite_read_plan_file_name(from.clone(), &files);
        tracing::info!("copy_files_to_table from source: {:?}", read_source_plan);

        let from_table = self.ctx.build_table_from_source_plan(&read_source_plan)?;
        from_table.read2(
            self.ctx.clone(),
            &read_source_plan,
            &mut build_res.main_pipeline,
        )?;

        let to_table = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;

        to_table.append2(self.ctx.clone(), &mut build_res.main_pipeline, false)?;

        let ctx = self.ctx.clone();
        let catalog_name = catalog_name.clone();
        let files = files.clone();
        let from = from.clone();

        build_res.main_pipeline.set_on_finished(move |may_error| {
            if may_error.is_none() {
                // capture out variable
                let ctx = ctx.clone();
                let files = files.clone();
                let from = from.clone();
                let catalog_name = catalog_name.clone();
                let to_table = to_table.clone();

                let task = GlobalIORuntime::instance().spawn(async move {
                    // Commit
                    let operations = ctx.consume_precommit_blocks();
                    to_table
                        .commit_insertion(ctx.clone(), &catalog_name, operations, false)
                        .await?;

                    // Purge
                    CopyInterpreterV2::purge_files(ctx, &from, &files).await
                });

                return match futures::executor::block_on(task) {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(error)) => Err(error),
                    Err(cause) => Err(ErrorCode::PanicError(format!(
                        "Maybe panic while in commit insert. {}",
                        cause
                    ))),
                };
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
            } => {
                let mut files = self.list_files(from, files).await?;

                // Pattern match check.
                let pattern = &pattern;
                if !pattern.is_empty() {
                    let regex = Regex::new(pattern).map_err(|e| {
                        ErrorCode::SyntaxException(format!(
                            "Pattern format invalid, got:{}, error:{:?}",
                            pattern, e
                        ))
                    })?;

                    let matched_files = files
                        .iter()
                        .filter(|file| regex.is_match(file))
                        .cloned()
                        .collect();
                    files = matched_files;
                }

                tracing::info!("matched files: {:?}, pattern: {}", &files, pattern);

                match &from.source_info {
                    SourceInfo::StageSource(table_info) => {
                        let (table_id, copy_stage_files) = self
                            .filter_duplicate_files(
                                *force,
                                table_info,
                                catalog_name,
                                database_name,
                                table_name,
                                &files,
                            )
                            .await?;

                        tracing::info!(
                            "matched copy unduplicate files: {:?}",
                            &copy_stage_files.keys(),
                        );

                        if copy_stage_files.is_empty() {
                            return Ok(PipelineBuildResult::create());
                        }

                        let result = self
                            .copy_files_to_table(
                                catalog_name,
                                database_name,
                                table_name,
                                from,
                                copy_stage_files.keys().cloned().collect(),
                            )
                            .await;

                        if result.is_ok() {
                            let _ = self
                                .upsert_copied_files_info(catalog_name, table_id, copy_stage_files)
                                .await?;
                        }

                        result
                    }
                    _other => {
                        return self
                            .copy_files_to_table(
                                catalog_name,
                                database_name,
                                table_name,
                                from,
                                files.clone(),
                            )
                            .await;
                    }
                }
            }
            CopyPlanV2::IntoStage {
                stage, from, path, ..
            } => self.execute_copy_into_stage(stage, path, from).await,
        }
    }
}
