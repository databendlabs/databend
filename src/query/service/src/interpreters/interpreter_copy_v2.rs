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
use std::path::Path;
use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use common_planners::StageTableInfo;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::TryStreamExt;
use regex::Regex;
use tracing::info;

use super::append2table;
use super::commit2table;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreterV2;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::Pipeline;
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
                    let mut list = vec![];

                    // TODO: we could rewrite into try_collect.
                    let mut objects = op.batch().walk_top_down(path)?;
                    while let Some(de) = objects.try_next().await? {
                        if de.mode().is_dir() {
                            continue;
                        }
                        list.push(de.path().to_string());
                    }

                    list
                };

                info!("listed files: {:?}", &files_with_path);

                Ok(files_with_path)
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
        files: Vec<String>,
    ) -> ReadDataSourcePlan {
        if let SourceInfo::StageSource(ref mut stage) = plan.source_info {
            stage.files = files
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
    ) -> Result<Vec<DataBlock>> {
        let ctx = self.ctx.clone();
        let settings = self.ctx.get_settings();

        let mut pipeline = Pipeline::create();
        let read_source_plan = from.clone();
        let read_source_plan = Self::rewrite_read_plan_file_name(read_source_plan, files);
        info!("copy_files_to_table from source: {:?}", read_source_plan);
        let table = ctx.build_table_from_source_plan(&read_source_plan)?;
        let res = table.read2(ctx.clone(), &read_source_plan, &mut pipeline);
        if let Err(e) = res {
            return Err(e);
        }

        let table = ctx.get_table(catalog_name, db_name, tbl_name).await?;

        table.append2(ctx.clone(), &mut pipeline)?;
        pipeline.set_max_threads(settings.get_max_threads()? as usize);

        let async_runtime = GlobalIORuntime::instance();
        let query_need_abort = ctx.query_need_abort();
        let executor_settings = ExecutorSettings::try_create(&settings)?;
        let executor = PipelineCompleteExecutor::try_create(
            async_runtime,
            query_need_abort,
            pipeline,
            executor_settings,
        )?;
        executor.execute()?;

        Ok(ctx.consume_precommit_blocks())
    }

    async fn execute_copy_into_stage(
        &self,
        stage: &UserStageInfo,
        path: &str,
        query: &Plan,
    ) -> Result<SendableDataBlockStream> {
        let (s_expr, metadata, bind_context) = match query {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => (s_expr, metadata, bind_context),
            v => unreachable!("Input plan must be Query, but it's {v}"),
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

        let build_res = select_interpreter.create_new_pipeline().await?;
        let table = StageTable::try_create(stage_table_info)?;

        append2table(
            self.ctx.clone(),
            table.clone(),
            data_schema.clone(),
            build_res,
        )?;
        commit2table(self.ctx.clone(), table.clone(), false).await?;

        Ok(Box::pin(DataBlockStream::create(
            Arc::new(DataSchema::empty()),
            None,
            vec![],
        )))
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreterV2 {
    fn name(&self) -> &str {
        "CopyInterpreterV2"
    }

    #[tracing::instrument(level = "debug", name = "copy_interpreter_execute_v2", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        match &self.plan {
            // TODO(xuanwo): extract them as a separate function.
            CopyPlanV2::IntoTable {
                catalog_name,
                database_name,
                table_name,
                files,
                pattern,
                from,
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

                info!("matched files: {:?}, pattern: {}", &files, pattern);

                let write_results = self
                    .copy_files_to_table(catalog_name, database_name, table_name, from, files)
                    .await?;

                let table = self
                    .ctx
                    .get_table(catalog_name, database_name, table_name)
                    .await?;

                // Commit.
                table
                    .commit_insertion(self.ctx.clone(), catalog_name, write_results, false)
                    .await?;

                Ok(Box::pin(DataBlockStream::create(
                    // TODO(xuanwo): Is this correct?
                    Arc::new(DataSchema::new(vec![])),
                    None,
                    vec![],
                )))
            }
            CopyPlanV2::IntoStage {
                stage, from, path, ..
            } => self.execute_copy_into_stage(stage, path, from).await,
        }
    }
}
