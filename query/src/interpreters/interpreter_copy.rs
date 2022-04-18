// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::S3File;
use common_planners::CopyPlan;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::TryStreamExt;
use regex::Regex;

use crate::interpreters::stream::ProcessorExecutorStream;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::new::executor::PipelinePullingExecutor;
use crate::pipelines::new::QueryPipelineBuilder;
use crate::sessions::QueryContext;
use crate::storages::StageSource;

pub struct CopyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyPlan,
}

impl CopyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CopyInterpreter { ctx, plan }))
    }

    // List the files.
    // There are two cases here:
    // 1. If the plan.files is not empty, we already set the files sets to the COPY command with: `files=(<file1>, <file2>)` syntax, only need to add the prefix to the file.
    // 2. If the plan.files is empty, there are also two case:
    //     2.1 If the path is a file like /path/to/path/file, S3File::list() will return the same file path.
    //     2.2 If the path is a folder, S3File::list() will return all the files in it.
    async fn list_files(&self) -> Result<Vec<String>> {
        let files = match &self.plan.from.source_info {
            SourceInfo::S3StageSource(table_info) => {
                let path = &table_info.path;
                // Here we add the path to the file: /path/to/path/file1.
                let files_with_path = if !self.plan.files.is_empty() {
                    let mut files_with_path = vec![];
                    for file in &self.plan.files {
                        let new_path = Path::new(path).join(file);
                        files_with_path.push(new_path.to_string_lossy().to_string());
                    }
                    files_with_path
                } else {
                    let op = StageSource::get_op(&self.ctx, &table_info.stage_info).await?;
                    S3File::list(&op, path).await?
                };

                Ok(files_with_path)
            }
            other => Err(ErrorCode::LogicalError(format!(
                "Cannot list files for the source info: {:?}",
                other
            ))),
        };

        files
    }

    // Rewrite the ReadDataSourcePlan.S3StageSource.file_name to new file name.
    fn rewrite_read_plan_file_name(
        mut plan: ReadDataSourcePlan,
        file_name: Option<String>,
    ) -> ReadDataSourcePlan {
        if let SourceInfo::S3StageSource(ref mut s3) = plan.source_info {
            s3.file_name = file_name;
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
    //  COPY should deal with the file one by one and do some error handler on the OnError strategy.

    #[tracing::instrument(level = "debug", name = "copy_one_file_to_table", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn copy_one_file_to_table(&self, file_name: Option<String>) -> Result<Vec<DataBlock>> {
        let ctx = self.ctx.clone();
        let settings = self.ctx.get_settings();

        let read_source_plan = self.plan.from.clone();
        let read_source_plan = Self::rewrite_read_plan_file_name(read_source_plan, file_name);

        tracing::info!("copy_one_file_to_table: source plan:{:?}", read_source_plan);

        let from_plan = common_planners::SelectPlan {
            input: Arc::new(PlanNode::ReadSource(read_source_plan)),
        };

        let pipeline_builder = QueryPipelineBuilder::create(ctx.clone());
        let mut pipeline = pipeline_builder.finalize(&from_plan)?;
        pipeline.set_max_threads(settings.get_max_threads()? as usize);

        let async_runtime = ctx.get_storage_runtime();
        let executor = PipelinePullingExecutor::try_create(async_runtime, pipeline)?;
        let source_stream = Box::pin(ProcessorExecutorStream::create(executor)?);

        let table = ctx
            .get_table(
                &self.plan.catalog_name,
                &self.plan.db_name,
                &self.plan.tbl_name,
            )
            .await?;
        let operations = table
            .append_data(ctx.clone(), source_stream)
            .await?
            .try_collect()
            .await?;

        Ok(operations)
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreter {
    fn name(&self) -> &str {
        "CopyInterpreter"
    }

    #[tracing::instrument(level = "debug", name = "copy_interpreter_execute", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        mut _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let mut files = self.list_files().await?;

        // Pattern match check.
        let pattern = &self.plan.pattern;
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

        tracing::info!("copy file list:{:?}, pattern:{}", &files, pattern,);

        let mut write_results = vec![];
        for file in files {
            let result = self.copy_one_file_to_table(Some(file)).await?;
            write_results.extend_from_slice(result.as_slice());
        }

        let table = self
            .ctx
            .get_table(
                &self.plan.catalog_name,
                &self.plan.db_name,
                &self.plan.tbl_name,
            )
            .await?;

        // Commit.
        table
            .commit_insertion(self.ctx.clone(), write_results, false)
            .await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
