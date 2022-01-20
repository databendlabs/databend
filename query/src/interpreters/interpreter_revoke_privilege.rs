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
use common_planners::RevokePrivilegePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::interpreter_common::validate_grant_object_exists;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct RevokePrivilegeInterpreter {
    ctx: Arc<QueryContext>,
    plan: RevokePrivilegePlan,
}

impl RevokePrivilegeInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RevokePrivilegePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(RevokePrivilegeInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for RevokePrivilegeInterpreter {
    fn name(&self) -> &str {
        "RevokePrivilegeInterpreter"
    }

    #[tracing::instrument(level = "debug", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();

        validate_grant_object_exists(&self.ctx, &plan.on).await?;

        // TODO: check user existence
        // TODO: check privilege on granting on the grant object

        let tenant = self.ctx.get_tenant();
        let user_mgr = self.ctx.get_user_manager();
        user_mgr
            .revoke_user_privileges(
                &tenant,
                &plan.username,
                &plan.hostname,
                plan.on,
                plan.priv_types,
            )
            .await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
