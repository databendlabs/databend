// Copyright 2020 Datafuse Labs.
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
use common_management::UserInfo;
use common_planners::CreateUserPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::DatabendQueryContextRef;

#[derive(Debug)]
pub struct CreatUserInterpreter {
    ctx: DatabendQueryContextRef,
    plan: CreateUserPlan,
}

impl CreatUserInterpreter {
    pub fn try_create(
        ctx: DatabendQueryContextRef,
        plan: CreateUserPlan,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(CreatUserInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for CreatUserInterpreter {
    fn name(&self) -> &str {
        "CreateUserInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();
        let user_mgr = self.ctx.get_sessions_manager().get_user_manager();
        let user_info = UserInfo {
            name: plan.name,
            host_name: plan.host_name,
            password: plan.password,
            auth_type: plan.auth_type,
        };
        user_mgr.add_user(user_info).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
