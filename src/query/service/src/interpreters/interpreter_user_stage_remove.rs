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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::plans::RemoveStagePlan;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_common_storages_stage::StageTable;
use databend_storages_common_io::Files;
use futures_util::StreamExt;
use log::debug;
use log::error;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct RemoveUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: RemoveStagePlan,
}

impl RemoveUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RemoveStagePlan) -> Result<Self> {
        Ok(RemoveUserStageInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RemoveUserStageInterpreter {
    fn name(&self) -> &str {
        "RemoveUserStageInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "remove_user_stage_execute");

        let thread_num = self.ctx.get_settings().get_max_threads()? as usize;

        let plan = self.plan.clone();
        let op = StageTable::get_op(&self.plan.stage)?;
        let pattern = if plan.pattern.is_empty() {
            None
        } else {
            Some(plan.pattern.clone())
        };
        let files_info = StageFilesInfo {
            path: plan.path.clone(),
            files: None,
            pattern,
        };
        let files = files_info.list_stream(&op, thread_num, None).await?;

        let table_ctx: Arc<dyn TableContext> = self.ctx.clone();
        let file_op = Files::create(table_ctx, op);

        const REMOVE_BATCH: usize = 1000;
        let mut chunks = files.chunks(REMOVE_BATCH);

        // s3 can remove at most 1k files in one request
        while let Some(chunk) = chunks.next().await {
            let chunk: Result<Vec<StageFileInfo>> = chunk.into_iter().collect();
            let chunk = chunk?.into_iter().map(|x| x.path).collect::<Vec<_>>();
            if let Err(e) = file_op.remove_file_in_batch(&chunk).await {
                error!("Failed to delete file: {:?}, error: {}", chunk, e);
            }

            if self.ctx.check_aborting().is_err() {
                return Ok(PipelineBuildResult::create());
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
