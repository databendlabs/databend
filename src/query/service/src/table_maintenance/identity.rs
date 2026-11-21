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

use databend_common_catalog::cluster_info::Cluster;
use databend_common_catalog::session_type::SessionType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::DefaultSettings;
use databend_common_settings::SettingMode;
use databend_common_settings::SettingScope;
use databend_common_settings::Settings;
use databend_common_users::UserApiProvider;

use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionPrivilegeManager;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;

/// A value snapshot, never a session, transaction, query cache or cancellation token.
/// Session-local temporary tables run maintenance inline; their session state is never queued.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MaintenanceIdentity {
    pub(crate) tenant: Tenant,
    user: UserIdentity,
    auth_role: Option<String>,
    role: Option<String>,
    secondary_roles: Option<Vec<String>>,
    warehouse: Option<String>,
    execution_warehouse: String,
    settings: HashMap<String, String>,
}

impl MaintenanceIdentity {
    pub(crate) async fn capture(ctx: &Arc<QueryContext>) -> Result<Self> {
        let session = ctx.get_current_session();
        Ok(Self {
            tenant: session.get_current_tenant(),
            user: session.get_current_user()?.identity(),
            auth_role: session.privilege_mgr().get_auth_role(),
            role: session.get_current_role().map(|role| role.name),
            secondary_roles: session.get_secondary_roles(),
            warehouse: session.get_current_warehouse().await,
            execution_warehouse: ctx.get_cluster().get_warehouse_id()?,
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
                                | "auto_compaction_segments_limit"
                                | "query_out_of_memory_behavior"
                                | "allow_query_exceeded_limit"
                        )
                })
                .map(|entry| (entry.key().clone(), entry.value().value.to_string()))
                .collect(),
        })
    }

    /// A restored session setting does not establish where discovery placed this node.
    pub(crate) fn check_execution_warehouse(&self, cluster: &Cluster) -> Result<()> {
        let actual = cluster.get_warehouse_id()?;
        if cluster.unassign || actual != self.execution_warehouse {
            return Err(ErrorCode::InvalidWarehouse(format!(
                "Maintenance for warehouse '{}' cannot run on node '{}' (warehouse '{}', unassigned: {})",
                self.execution_warehouse, cluster.local_id, actual, cluster.unassign
            )));
        }
        Ok(())
    }

    pub(crate) async fn create_session(&self) -> Result<Arc<Session>> {
        // Queued work is delegated by a user, not an indefinitely authenticated session.
        // Reload the account and grants before each unit; do not replay a stale UserInfo.
        let user = UserApiProvider::instance()
            .get_user(&self.tenant, self.user.clone())
            .await?;
        if user.option.disabled().copied().unwrap_or(false) {
            return Err(ErrorCode::AuthenticateFailure(
                "Maintenance execution user is disabled",
            ));
        }
        let settings = Settings::create(self.tenant.clone());
        settings.load_changes().await?;
        settings.set_batch_settings(&self.settings, false)?;
        let manager = SessionManager::instance();
        let mut session = manager.create_with_settings(SessionType::Dummy, settings, None)?;
        session.set_current_tenant(self.tenant.clone())?;
        let session = manager.register_session(session)?;
        session
            .set_authed_user(user, self.auth_role.clone())
            .await?;
        if let Some(role) = &self.role {
            session.set_current_role_checked(role).await?;
        }
        if let Some(roles) = &self.secondary_roles {
            for role in roles {
                session
                    .privilege_mgr()
                    .validate_available_role(role)
                    .await?;
            }
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
    use crate::test_kits::ClusterDescriptor;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_queued_identity_requires_original_execution_warehouse() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let cluster = |warehouse: &str| {
            ClusterDescriptor::new()
                .with_local_id("node")
                .with_node_info("node", "127.0.0.1:1", "cluster", warehouse)
        };
        let original = fixture.new_query_ctx_with_cluster(cluster("A")).await?;
        let identity = MaintenanceIdentity::capture(&original).await?;
        identity.check_execution_warehouse(&original.get_cluster())?;
        for warehouse in ["", "B"] {
            let reassigned = fixture
                .new_query_ctx_with_cluster(cluster(warehouse))
                .await?;
            let error = identity
                .check_execution_warehouse(&reassigned.get_cluster())
                .unwrap_err();
            assert_eq!(error.code(), ErrorCode::INVALID_WAREHOUSE);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wrong_warehouse_skips_unit_and_releases_grant() -> Result<()> {
        use std::time::Duration;

        use tokio_util::sync::CancellationToken;

        use crate::table_maintenance::TableMaintenanceService;
        use crate::table_maintenance::executor::execute_unit;

        let fixture = TestFixture::setup().await?;
        let original = fixture
            .new_query_ctx_with_cluster(
                ClusterDescriptor::new()
                    .with_local_id("node")
                    .with_node_info("node", "127.0.0.1:1", "cluster", "other_warehouse"),
            )
            .await?;
        let identity = MaintenanceIdentity::capture(&original).await?;
        let service = TableMaintenanceService::instance();
        let resources = service.resources();
        let error = execute_unit::<(), _>(
            &identity,
            CancellationToken::default(),
            resources,
            1,
            |_| async {
                panic!("wrong warehouse polled maintenance work");
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::INVALID_WAREHOUSE);
        tokio::time::timeout(Duration::from_secs(1), resources.drain())
            .await
            .expect("failed admission leaked a grant");
        Ok(())
    }

    async fn capture_user_identity(
        fixture: &TestFixture,
        name: &str,
    ) -> Result<MaintenanceIdentity> {
        let user = UserApiProvider::instance()
            .get_user(&fixture.default_tenant(), UserIdentity::new(name, "%"))
            .await?;
        let session = fixture.new_session_with_type(SessionType::Dummy).await?;
        // The fixture starts as root; remove its current role before changing users.
        session.unset_current_role().await?;
        session.set_authed_user(user, None).await?;
        MaintenanceIdentity::capture(&session.create_query_context(&BUILD_INFO).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_deleted_or_disabled_user_cannot_start_another_unit() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        fixture
            .execute_command("CREATE USER maintenance_user IDENTIFIED BY 'test-password'")
            .await?;
        let identity = capture_user_identity(&fixture, "maintenance_user").await?;
        assert!(identity.create_session().await.is_ok());
        fixture
            .execute_command("ALTER USER maintenance_user WITH DISABLED = TRUE")
            .await?;
        let error = identity
            .create_session()
            .await
            .err()
            .expect("disabled user must be rejected");
        assert_eq!(error.code(), ErrorCode::AUTHENTICATE_FAILURE);
        fixture
            .execute_command("DROP USER maintenance_user")
            .await?;
        let error = identity
            .create_session()
            .await
            .err()
            .expect("deleted user must be rejected");
        assert_eq!(error.code(), ErrorCode::UNKNOWN_USER);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_revoked_role_is_not_replayed_from_queued_grants() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        fixture
            .execute_command("CREATE USER maintenance_user IDENTIFIED BY 'test-password'")
            .await?;
        fixture
            .execute_command("CREATE ROLE maintenance_role")
            .await?;
        fixture
            .execute_command("GRANT ROLE maintenance_role TO USER maintenance_user")
            .await?;
        let mut identity = capture_user_identity(&fixture, "maintenance_user").await?;
        identity.role = Some("maintenance_role".into());
        assert!(identity.create_session().await.is_ok());
        fixture
            .execute_command("REVOKE ROLE maintenance_role FROM USER maintenance_user")
            .await?;
        let error = identity
            .create_session()
            .await
            .err()
            .expect("revoked role must be rejected");
        assert_eq!(error.code(), ErrorCode::INVALID_ROLE);
        identity.role = None;
        identity.secondary_roles = Some(vec!["maintenance_role".into()]);
        let error = identity
            .create_session()
            .await
            .err()
            .expect("revoked secondary role must be rejected");
        assert_eq!(error.code(), ErrorCode::INVALID_ROLE);
        Ok(())
    }

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
        settings.set_setting("auto_compaction_segments_limit".into(), "2".into())?;
        let identity = MaintenanceIdentity::capture(&ctx).await?;
        assert!(
            !identity
                .settings
                .contains_key("auto_compaction_segments_limit")
        );
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
