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

use common_exception::Result;
use common_planners::RemoveUserStagePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use tracing::debug;

use crate::interpreters::interpreter_common::list_files;
use crate::interpreters::Interpreter;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::stage::StageSourceHelper;

#[derive(Debug)]
pub struct RemoveUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: RemoveUserStagePlan,
}

impl RemoveUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RemoveUserStagePlan) -> Result<Self> {
        Ok(RemoveUserStageInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RemoveUserStageInterpreter {
    fn name(&self) -> &str {
        "RemoveUserStageInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();

        let files = list_files(&self.ctx, &plan.stage, &plan.path, &plan.pattern).await?;
        let rename_me: Arc<dyn TableContext> = self.ctx.clone();
        let op = StageSourceHelper::get_op(&rename_me, &self.plan.stage).await?;

        for name in files.iter().map(|f| f.path.as_str()) {
            let obj = format!("{}/{}", plan.stage.get_prefix(), name);
            debug!("Removing object: {}", obj);
            let _ = op.object(&obj).delete().await;
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema(),
            None,
            vec![],
        )))
    }
}
