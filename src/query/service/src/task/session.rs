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

use crate::sessions::Session;
use crate::sessions::SessionManager;

/// Create a user for task with necessary privileges
pub fn get_task_user(tenant_id: &str, cluster_id: &str) -> UserInfo {
    let mut user = UserInfo::new_no_auth(
        format!("{}-{}-task", tenant_id, cluster_id).as_str(),
        "0.0.0.0",
    );
    user.grants.grant_privileges(
        &GrantObject::Global,
        UserPrivilegeType::CreateDatabase.into(),
    );
    user
}

/// Create a dummy session for task
pub async fn create_session(
    user: UserInfo,
    restricted_role: Option<String>,
) -> Result<Arc<Session>> {
    let session_manager = SessionManager::instance();
    let dummy_session = session_manager.create_session(SessionType::Dummy).await?;
    let session = session_manager.register_session(dummy_session)?;
    session.set_authed_user(user, restricted_role).await?;
    Ok(session)
}
