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

use common_exception::Result;
use common_planners::AlterUserPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct AlterUserInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterUserPlan,
}

impl AlterUserInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterUserPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(AlterUserInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterUserInterpreter {
    fn name(&self) -> &str {
        "AlterUserInterpreter"
    }

    #[tracing::instrument(level = "debug", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let user_mgr = self.ctx.get_user_manager();
        if let Some(new_auth_info) = plan.auth_info {
            user_mgr
                .update_user(
                    &tenant,
                    plan.name.as_str(),
                    plan.hostname.as_str(),
                    new_auth_info,
                )
                .await?;
        }

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
