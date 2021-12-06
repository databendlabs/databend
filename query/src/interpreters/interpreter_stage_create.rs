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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::CreateUserStagePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct CreatStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateUserStagePlan,
}

impl CreatStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateUserStagePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CreatStageInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for CreatStageInterpreter {
    fn name(&self) -> &str {
        "CreatStageInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();
        let user_mgr = self.ctx.get_sessions_manager().get_user_manager();
        let user_stage = plan.user_stage_info;
        let create_stage = user_mgr.add_stage(user_stage).await;
        if plan.if_not_exists {
            create_stage.or_else(|e| {
                // StageAlreadyExists(4061)
                if e.code() == 4061 {
                    Ok(u64::MIN)
                } else {
                    Err(e)
                }
            })?;
        } else {
            create_stage?;
        }

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
