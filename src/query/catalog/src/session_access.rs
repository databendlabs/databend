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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserPrivilegeType;
use serde::Deserialize;
use serde::Serialize;

use crate::table_context::TableContext;

/// Which sessions a user may list in `system.processes` and kill: its own sessions in its
/// tenant, or with SUPER all sessions of its tenant. It is captured from the session that runs
/// the statement and sent to the other nodes, which only have worker sessions for it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionAccess {
    pub tenant: String,
    pub user: UserIdentity,
    pub is_super: bool,
}

impl SessionAccess {
    pub async fn of_current_user(ctx: &dyn TableContext) -> Result<Self> {
        let is_super = match ctx
            .validate_privilege(&GrantObject::Global, UserPrivilegeType::Super, false)
            .await
        {
            Ok(()) => true,
            Err(err) if err.code() == ErrorCode::PERMISSION_DENIED => false,
            Err(err) => return Err(err),
        };
        Ok(SessionAccess {
            tenant: ctx.get_tenant().tenant_name().to_string(),
            user: ctx.get_current_user()?.identity(),
            is_super,
        })
    }

    /// Whether a session of `target_user` in `target_tenant` is accessible. A session without
    /// a user yet is accessible only with SUPER.
    pub fn can_access(&self, target_tenant: &str, target_user: Option<&UserIdentity>) -> bool {
        !self.tenant.is_empty()
            && self.tenant == target_tenant
            && (self.is_super || target_user == Some(&self.user))
    }
}
