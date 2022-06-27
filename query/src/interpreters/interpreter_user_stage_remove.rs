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
use common_meta_types::StageType;
use common_planners::RemoveUserStagePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::interpreter_common::list_files;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;
use crate::storages::stage::StageSource;

#[derive(Debug)]
pub struct RemoveUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: RemoveUserStagePlan,
}

impl RemoveUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RemoveUserStagePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(RemoveUserStageInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for RemoveUserStageInterpreter {
    fn name(&self) -> &str {
        "RemoveUserStageInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();
        let user_mgr = self.ctx.get_user_manager();
        let tenant = self.ctx.get_tenant();

        let files = list_files(&self.ctx, &plan.stage, &plan.path, &plan.pattern).await?;
        let files = files.iter().map(|f| f.path.clone()).collect::<Vec<_>>();
        let op = StageSource::get_op(&self.ctx, &self.plan.stage).await?;
        if plan.stage.stage_type == StageType::Internal {
            user_mgr
                .remove_files(&tenant, &plan.stage.stage_name, files.clone())
                .await?;
        }

        for name in files.into_iter() {
            let obj = format!("{}/{}", plan.stage.get_prefix(), name);
            tracing::debug!("Removing object: {}", obj);
            let _ = op.object(&obj).delete().await;
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema(),
            None,
            vec![],
        )))
    }
}
