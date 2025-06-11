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

use databend_common_catalog::session_type::SessionType;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;

use crate::sessions::Session;
use crate::sessions::SessionManager;

/// Create a user for history log with necessary privileges
pub fn get_history_log_user(tenant_id: &str, cluster_id: &str) -> UserInfo {
    let mut user = UserInfo::new_no_auth(
        format!("{}-{}-history-log", tenant_id, cluster_id).as_str(),
        "0.0.0.0",
    );
    user.grants.grant_privileges(
        &GrantObject::Global,
        UserPrivilegeType::CreateDatabase.into(),
    );
    user
}

/// Create a dummy session for history log
pub async fn create_session(tenant_id: &str, cluster_id: &str) -> Result<Arc<Session>> {
    let session_manager = SessionManager::instance();
    let dummy_session = session_manager.create_session(SessionType::Dummy).await?;
    let session = session_manager.register_session(dummy_session)?;
    let user = get_history_log_user(tenant_id, cluster_id);
    session
        .set_authed_user(user.clone(), Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()))
        .await?;
    Ok(session)
}
