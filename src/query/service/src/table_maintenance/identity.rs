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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::session_type::SessionType;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::DefaultSettings;
use databend_common_settings::SettingMode;
use databend_common_settings::SettingScope;
use databend_common_settings::Settings;

use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionPrivilegeManager;
use crate::sessions::TableContextSettings;

/// A value snapshot, never a session, transaction, query cache or cancellation token.
/// Temporary tables cannot use this identity: their remaining hooks stay session-local.
#[derive(Clone)]
pub(crate) struct MaintenanceIdentity {
    pub(crate) tenant: Tenant,
    user: UserInfo,
    auth_role: Option<String>,
    role: Option<String>,
    secondary_roles: Option<Vec<String>>,
    warehouse: Option<String>,
    settings: HashMap<String, String>,
}

impl MaintenanceIdentity {
    pub(crate) async fn capture(ctx: &Arc<QueryContext>) -> Result<Self> {
        let session = ctx.get_current_session();
        Ok(Self {
            tenant: session.get_current_tenant(),
            user: session.get_current_user()?,
            auth_role: session.privilege_mgr().get_auth_role(),
            role: session.get_current_role().map(|role| role.name),
            secondary_roles: session.get_secondary_roles(),
            warehouse: session.get_current_warehouse().await,
            settings: ctx
                .get_settings()
                .changes()
                .iter()
                // Long-lived maintenance uses current system budgets, not the statement's
                // resource overrides. Preserve expression/semantic settings as values.
                .filter(|entry| {
                    // Global-only/read-only values (notably enterprise_license) cannot be
                    // replayed with SET SESSION. Reload them from current node/tenant policy.
                    DefaultSettings::check_setting_scope(entry.key(), SettingScope::Session).is_ok()
                        && DefaultSettings::check_setting_mode(entry.key(), SettingMode::Write)
                            .is_ok()
                        && !matches!(
                            entry.key().as_str(),
                            "max_threads"
                                | "max_memory_usage"
                                | "max_query_memory_usage"
                                | "max_execute_time_in_seconds"
                                | "max_storage_io_requests"
                                | "recluster_block_size"
                                | "query_out_of_memory_behavior"
                                | "allow_query_exceeded_limit"
                        )
                })
                .map(|entry| (entry.key().clone(), entry.value().value.to_string()))
                .collect(),
        })
    }

    pub(crate) async fn create_session(&self) -> Result<Arc<Session>> {
        let settings = Settings::create(self.tenant.clone());
        settings.load_changes().await?;
        settings.set_batch_settings(&self.settings, false)?;
        let manager = SessionManager::instance();
        let mut session = manager.create_with_settings(SessionType::Dummy, settings, None)?;
        session.set_current_tenant(self.tenant.clone())?;
        let session = manager.register_session(session)?;
        session
            .set_authed_user(self.user.clone(), self.auth_role.clone())
            .await?;
        if let Some(role) = &self.role {
            session.set_current_role_checked(role).await?;
        }
        session
            .set_secondary_roles_checked(self.secondary_roles.clone())
            .await?;
        session
            .set_current_warehouse(self.warehouse.clone())
            .await?;
        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_version::BUILD_INFO;

    use super::*;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_global_only_settings_reload_without_session_scope_replay() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();
        settings
            .set_global_setting("enterprise_license".into(), "old-global-policy".into())
            .await?;
        settings.set_setting("timezone".into(), "Asia/Shanghai".into())?;
        settings.set_max_threads(32)?;
        let identity = MaintenanceIdentity::capture(&ctx).await?;
        assert!(!identity.settings.contains_key("enterprise_license"));
        assert!(!identity.settings.contains_key("max_threads"));
        assert_eq!(
            identity.settings.get("timezone").map(String::as_str),
            Some("Asia/Shanghai")
        );
        settings
            .set_global_setting("enterprise_license".into(), "current-global-policy".into())
            .await?;
        let session = identity.create_session().await?;
        assert_eq!(
            session.get_settings().get_enterprise_license(&BUILD_INFO),
            "current-global-policy"
        );
        assert_eq!(session.get_settings().get_timezone()?, "Asia/Shanghai");
        Ok(())
    }
}
