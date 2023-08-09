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
use std::net::SocketAddr;
use std::sync::Arc;

use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::RoleInfo;
use common_meta_app::principal::UserInfo;
use common_meta_app::principal::UserPrivilegeType;
use common_settings::ChangeValue;
use common_settings::Settings;
use common_users::RoleCacheManager;
use common_users::BUILTIN_ROLE_PUBLIC;
use log::debug;
use parking_lot::RwLock;

use crate::clusters::ClusterDiscovery;
use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::QueryContext;
use crate::sessions::QueryContextShared;
use crate::sessions::SessionContext;
use crate::sessions::SessionManager;
use crate::sessions::SessionStatus;
use crate::sessions::SessionType;

pub struct Session {
    pub(in crate::sessions) id: String,
    pub(in crate::sessions) typ: RwLock<SessionType>,
    pub(in crate::sessions) session_ctx: Arc<SessionContext>,
    status: Arc<RwLock<SessionStatus>>,
    pub(in crate::sessions) mysql_connection_id: Option<u32>,
    format_settings: FormatSettings,
}

impl Session {
    pub fn try_create(
        id: String,
        typ: SessionType,
        session_ctx: Arc<SessionContext>,
        mysql_connection_id: Option<u32>,
    ) -> Result<Arc<Session>> {
        let status = Arc::new(Default::default());
        Ok(Arc::new(Session {
            id,
            typ: RwLock::new(typ),
            status,
            session_ctx,
            mysql_connection_id,
            format_settings: FormatSettings::default(),
        }))
    }

    pub fn get_mysql_conn_id(self: &Arc<Self>) -> Option<u32> {
        self.mysql_connection_id
    }

    pub fn get_id(self: &Arc<Self>) -> String {
        self.id.clone()
    }

    pub fn get_type(&self) -> SessionType {
        let lock = self.typ.read();
        lock.clone()
    }

    pub fn set_type(&self, typ: SessionType) {
        let mut lock = self.typ.write();
        *lock = typ;
    }

    pub fn is_aborting(self: &Arc<Self>) -> bool {
        self.session_ctx.get_abort()
    }

    pub fn quit(self: &Arc<Self>) {
        let session_ctx = self.session_ctx.clone();
        if session_ctx.get_current_query_id().is_some() {
            if let Some(shutdown_fun) = session_ctx.take_io_shutdown_tx() {
                shutdown_fun();
            }
        }

        let http_queries_manager = HttpQueryManager::instance();
        http_queries_manager.kill_session(&self.id);
    }

    pub fn kill(self: &Arc<Self>) {
        self.session_ctx.set_abort(true);
        self.quit();
    }

    pub fn force_kill_session(self: &Arc<Self>) {
        self.force_kill_query(ErrorCode::AbortedQuery(
            "Aborted query, because the server is shutting down or the query was killed",
        ));
        self.kill(/* shutdown io stream */);
    }

    pub fn force_kill_query(self: &Arc<Self>, cause: ErrorCode) {
        let session_ctx = self.session_ctx.clone();

        if let Some(context_shared) = session_ctx.get_query_context_shared() {
            context_shared.kill(cause);
        }
    }

    /// Create a query context for query.
    /// For a query, execution environment(e.g cluster) should be immutable.
    /// We can bind the environment to the context in create_context method.
    #[async_backtrace::framed]
    pub async fn create_query_context(self: &Arc<Self>) -> Result<Arc<QueryContext>> {
        let config = GlobalConfig::instance();
        let session = self.clone();
        let cluster = ClusterDiscovery::instance().discover(&config).await?;
        let shared = QueryContextShared::try_create(session, cluster)?;

        self.session_ctx
            .set_query_context_shared(Arc::downgrade(&shared));
        Ok(QueryContext::create_from_shared(shared))
    }

    // only used for values and mysql output
    pub fn set_format_settings(&mut self, other: FormatSettings) {
        self.format_settings = other
    }

    pub fn get_format_settings(&self) -> FormatSettings {
        self.format_settings.clone()
    }

    pub fn get_current_query_id(&self) -> Option<String> {
        self.session_ctx.get_current_query_id()
    }

    pub fn attach<F>(self: &Arc<Self>, host: Option<SocketAddr>, io_shutdown: F)
    where F: FnOnce() + Send + Sync + 'static {
        self.session_ctx.set_client_host(host);
        self.session_ctx.set_io_shutdown_tx(io_shutdown);
    }

    pub fn set_current_database(self: &Arc<Self>, database_name: String) {
        self.session_ctx.set_current_database(database_name);
    }

    pub fn get_current_database(self: &Arc<Self>) -> String {
        self.session_ctx.get_current_database()
    }

    pub fn get_current_catalog(self: &Arc<Self>) -> String {
        self.session_ctx.get_current_catalog()
    }

    pub fn get_current_tenant(self: &Arc<Self>) -> String {
        self.session_ctx.get_current_tenant()
    }

    pub fn set_current_tenant(self: &Arc<Self>, tenant: String) {
        self.session_ctx.set_current_tenant(tenant);
    }

    pub fn get_current_user(self: &Arc<Self>) -> Result<UserInfo> {
        self.session_ctx
            .get_current_user()
            .ok_or_else(|| ErrorCode::AuthenticateFailure("unauthenticated"))
    }

    // set_authed_user() is called after authentication is passed in various protocol handlers, like
    // HTTP handler, clickhouse query handler, mysql query handler. auth_role represents the role
    // granted by external authenticator, it will over write the current user's granted roles, and
    // becomes the CURRENT ROLE if not set X-DATABEND-ROLE.
    #[async_backtrace::framed]
    pub async fn set_authed_user(
        self: &Arc<Self>,
        user: UserInfo,
        auth_role: Option<String>,
    ) -> Result<()> {
        self.session_ctx.set_current_user(user);
        self.session_ctx.set_auth_role(auth_role);
        self.ensure_current_role().await?;
        Ok(())
    }

    // ensure_current_role() is called after authentication and before any privilege checks
    #[async_backtrace::framed]
    async fn ensure_current_role(self: &Arc<Self>) -> Result<()> {
        let tenant = self.get_current_tenant();
        let public_role = RoleCacheManager::instance()
            .find_role(&tenant, BUILTIN_ROLE_PUBLIC)
            .await?
            .unwrap_or_else(|| RoleInfo::new(BUILTIN_ROLE_PUBLIC));

        // if CURRENT ROLE is not set, take current session's AUTH ROLE
        let mut current_role_name = self.get_current_role().map(|r| r.name);
        if current_role_name.is_none() {
            current_role_name = self.session_ctx.get_auth_role();
        }

        // if CURRENT ROLE and AUTH ROLE are not set, take current user's DEFAULT ROLE
        if current_role_name.is_none() {
            current_role_name = self
                .session_ctx
                .get_current_user()
                .map(|u| u.option.default_role().cloned())
                .unwrap_or(None)
        };

        // if CURRENT ROLE, AUTH ROLE and DEFAULT ROLE are not set, take PUBLIC role
        let current_role_name =
            current_role_name.unwrap_or_else(|| BUILTIN_ROLE_PUBLIC.to_string());

        // I can not use the CURRENT ROLE, reset to PUBLIC role
        let role = self
            .validate_available_role(&current_role_name)
            .await
            .or_else(|e| {
                if e.code() == ErrorCode::INVALID_ROLE {
                    Ok(public_role)
                } else {
                    Err(e)
                }
            })?;
        self.session_ctx.set_current_role(Some(role));
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn validate_available_role(self: &Arc<Self>, role_name: &str) -> Result<RoleInfo> {
        let available_roles = self.get_all_available_roles().await?;
        let role = available_roles.iter().find(|r| r.name == role_name);
        match role {
            Some(role) => Ok(role.clone()),
            None => {
                let available_role_names = available_roles
                    .iter()
                    .map(|r| r.name.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                Err(ErrorCode::InvalidRole(format!(
                    "Invalid role {} for current session, available: {}",
                    role_name, available_role_names,
                )))
            }
        }
    }

    // Only the available role can be set as current role. The current role can be set by the SET
    // ROLE statement, or by the X-DATABEND-ROLE header in HTTP protocol (not implemented yet).
    #[async_backtrace::framed]
    pub async fn set_current_role_checked(self: &Arc<Self>, role_name: &str) -> Result<()> {
        let role = self.validate_available_role(role_name).await?;
        self.session_ctx.set_current_role(Some(role));
        Ok(())
    }

    pub fn get_current_role(self: &Arc<Self>) -> Option<RoleInfo> {
        self.session_ctx.get_current_role()
    }

    pub fn unset_current_role(self: &Arc<Self>) {
        self.session_ctx.set_current_role(None)
    }

    // Returns all the roles the current session has. If the user have been granted auth_role,
    // the other roles will be ignored.
    // On executing SET ROLE, the role have to be one of the available roles.
    #[async_backtrace::framed]
    pub async fn get_all_available_roles(self: &Arc<Self>) -> Result<Vec<RoleInfo>> {
        let roles = match self.session_ctx.get_auth_role() {
            Some(auth_role) => vec![auth_role],
            None => {
                let current_user = self.get_current_user()?;
                current_user.grants.roles()
            }
        };

        let tenant = self.get_current_tenant();
        let mut related_roles = RoleCacheManager::instance()
            .find_related_roles(&tenant, &roles)
            .await?;
        related_roles.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(related_roles)
    }

    #[async_backtrace::framed]
    pub async fn validate_privilege(
        self: &Arc<Self>,
        object: &GrantObject,
        privilege: Vec<UserPrivilegeType>,
    ) -> Result<()> {
        if matches!(self.get_type(), SessionType::Local) {
            return Ok(());
        }

        // 1. check user's privilege set
        let current_user = self.get_current_user()?;
        let user_verified = current_user
            .grants
            .verify_privilege(object, privilege.clone());
        if user_verified {
            return Ok(());
        }

        // 2. check the user's roles' privilege set
        self.ensure_current_role().await?;
        let available_roles = self.get_all_available_roles().await?;
        let role_verified = available_roles
            .iter()
            .any(|r| r.grants.verify_privilege(object, privilege.clone()));
        if role_verified {
            return Ok(());
        }
        let roles_name = available_roles
            .iter()
            .map(|r| r.name.clone())
            .collect::<Vec<_>>()
            .join(",");

        Err(ErrorCode::PermissionDenied(format!(
            "Permission denied, privilege {:?} is required on {} for user {} with roles [{}]",
            privilege.clone(),
            object,
            &current_user.identity(),
            roles_name,
        )))
    }

    pub fn get_settings(self: &Arc<Self>) -> Arc<Settings> {
        self.session_ctx.get_settings()
    }

    pub fn get_changed_settings(&self) -> HashMap<String, ChangeValue> {
        self.session_ctx.get_changed_settings()
    }

    pub fn apply_changed_settings(&self, changes: HashMap<String, ChangeValue>) -> Result<()> {
        self.session_ctx.apply_changed_settings(changes)
    }

    pub fn get_memory_usage(self: &Arc<Self>) -> usize {
        // TODO(winter): use thread memory tracker
        0
    }

    pub fn get_status(self: &Arc<Self>) -> Arc<RwLock<SessionStatus>> {
        self.status.clone()
    }

    pub fn get_query_result_cache_key(self: &Arc<Self>, query_id: &str) -> Option<String> {
        self.session_ctx.get_query_result_cache_key(query_id)
    }

    pub fn update_query_ids_results(self: &Arc<Self>, query_id: String, result_cache_key: String) {
        self.session_ctx
            .update_query_ids_results(query_id, Some(result_cache_key))
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        debug!("Drop session {}", self.id.clone());
        SessionManager::instance().destroy_session(&self.id.clone());
    }
}
