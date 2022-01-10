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
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeSet;
use common_planners::GrantPrivilegePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::interpreter_common::validate_grant_object_exists;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct GrantPrivilegeInterpreter {
    ctx: Arc<QueryContext>,
    plan: GrantPrivilegePlan,
}

impl GrantPrivilegeInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: GrantPrivilegePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(GrantPrivilegeInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for GrantPrivilegeInterpreter {
    fn name(&self) -> &str {
        "GrantPrivilegeInterpreter"
    }

    #[tracing::instrument(level = "debug", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();

        validate_grant_privileges(&plan.on, plan.priv_types)?;
        validate_grant_object_exists(&self.ctx, &plan.on).await?;

        // TODO: check user existence
        // TODO: check privilege on granting on the grant object

        let tenant = self.ctx.get_tenant();
        let user_mgr = self.ctx.get_user_manager();
        user_mgr
            .grant_user_privileges(
                &tenant,
                &plan.name,
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

/// Check if there's any privilege which can not be granted to this GrantObject.
/// Some global privileges can not be granted to a database or table, for example,
/// a KILL statement is meaningless for a table.
pub fn validate_grant_privileges(object: &GrantObject, privileges: UserPrivilegeSet) -> Result<()> {
    let available_privileges = object.available_privileges();
    let ok = privileges
        .iter()
        .all(|p| available_privileges.has_privilege(p));
    if !ok {
        return Err(common_exception::ErrorCode::IllegalGrant("Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used"));
    }
    Ok(())
}
