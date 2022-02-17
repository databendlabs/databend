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

use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_macros::MallocSizeOf;
use common_mem_allocator::malloc_size;
use common_meta_types::GrantObject;
use common_meta_types::UserInfo;
use common_meta_types::UserPrivilegeType;
use futures::channel::*;
use opendal::Operator;

use crate::catalogs::DatabaseCatalog;
use crate::configs::Config;
use crate::sessions::QueryContext;
use crate::sessions::QueryContextShared;
use crate::sessions::SessionContext;
use crate::sessions::SessionManager;
use crate::sessions::Settings;
use crate::users::UserApiProvider;

#[derive(Clone, MallocSizeOf)]
pub struct Session {
    pub(in crate::sessions) id: String,
    pub(in crate::sessions) typ: String,
    #[ignore_malloc_size_of = "insignificant"]
    pub(in crate::sessions) conf: Config,
    #[ignore_malloc_size_of = "insignificant"]
    pub(in crate::sessions) session_mgr: Arc<SessionManager>,
    pub(in crate::sessions) ref_count: Arc<AtomicUsize>,
    pub(in crate::sessions) session_ctx: Arc<SessionContext>,
    #[ignore_malloc_size_of = "insignificant"]
    session_settings: Settings,
}

impl Session {
    pub fn try_create(
        conf: Config,
        id: String,
        typ: String,
        session_mgr: Arc<SessionManager>,
    ) -> Result<Arc<Session>> {
        let user_api = session_mgr.get_user_manager();
        let session_ctx = Arc::new(SessionContext::try_create(conf.clone())?);
        let session_settings = Settings::try_create(&conf, session_ctx.clone(), user_api)?;
        let ref_count = Arc::new(AtomicUsize::new(0));

        Ok(Arc::new(Session {
            id,
            typ,
            conf,
            session_mgr,
            ref_count,
            session_ctx,
            session_settings,
        }))
    }

    pub fn get_id(self: &Arc<Self>) -> String {
        self.id.clone()
    }

    pub fn get_type(self: &Arc<Self>) -> String {
        self.typ.clone()
    }

    pub fn is_aborting(self: &Arc<Self>) -> bool {
        self.session_ctx.get_abort()
    }

    pub fn kill(self: &Arc<Self>) {
        let session_ctx = self.session_ctx.clone();
        session_ctx.set_abort(true);
        if session_ctx.query_context_shared_is_none() {
            if let Some(io_shutdown) = session_ctx.take_io_shutdown_tx() {
                let (tx, rx) = oneshot::channel();
                if io_shutdown.send(tx).is_ok() {
                    // We ignore this error because the receiver is return cancelled error.
                    let _ = futures::executor::block_on(rx);
                }
            }
        }
    }

    pub fn force_kill_session(self: &Arc<Self>) {
        self.force_kill_query();
        self.kill(/* shutdown io stream */);
    }

    pub fn force_kill_query(self: &Arc<Self>) {
        let session_ctx = self.session_ctx.clone();

        if let Some(context_shared) = session_ctx.take_query_context_shared() {
            context_shared.kill(/* shutdown executing query */);
        }
    }

    /// Create a query context for query.
    /// For a query, execution environment(e.g cluster) should be immutable.
    /// We can bind the environment to the context in create_context method.
    pub async fn create_query_context(self: &Arc<Self>) -> Result<Arc<QueryContext>> {
        let query_ctx = self.session_ctx.get_query_context_shared();

        Ok(match query_ctx.as_ref() {
            Some(shared) => QueryContext::create_from_shared(shared.clone()),
            None => {
                let config = self.conf.clone();
                let discovery = self.session_mgr.get_cluster_discovery();

                let session = self.clone();
                let cluster = discovery.discover().await?;
                let shared = QueryContextShared::try_create(config, session, cluster)?;

                let query_ctx = self.session_ctx.get_query_context_shared();
                match query_ctx.as_ref() {
                    Some(shared) => QueryContext::create_from_shared(shared.clone()),
                    None => {
                        self.session_ctx
                            .set_query_context_shared(Some(shared.clone()));
                        QueryContext::create_from_shared(shared)
                    }
                }
            }
        })
    }

    pub fn attach<F>(self: &Arc<Self>, host: Option<SocketAddr>, io_shutdown: F)
    where F: FnOnce() + Send + 'static {
        let (tx, rx) = futures::channel::oneshot::channel();
        self.session_ctx.set_client_host(host);
        self.session_ctx.set_io_shutdown_tx(Some(tx));

        common_base::tokio::spawn(async move {
            if let Ok(tx) = rx.await {
                (io_shutdown)();
                tx.send(()).ok();
            }
        });
    }

    pub fn set_current_database(self: &Arc<Self>, database_name: String) {
        self.session_ctx.set_current_database(database_name);
    }

    pub fn get_current_database(self: &Arc<Self>) -> String {
        self.session_ctx.get_current_database()
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

    pub fn set_current_user(self: &Arc<Self>, user: UserInfo) {
        self.session_ctx.set_current_user(user)
    }

    pub async fn validate_privilege(
        self: &Arc<Self>,
        object: &GrantObject,
        privilege: UserPrivilegeType,
    ) -> Result<()> {
        let current_user = self.get_current_user()?;
        let user_verified = current_user.grants.verify_privilege(object, privilege);
        if user_verified {
            return Ok(());
        }

        let tenant = self.get_current_tenant();
        let role_cache = self.session_mgr.get_role_cache_manager();
        let role_verified = role_cache
            .verify_privilege(&tenant, &current_user.grants.roles(), object, privilege)
            .await?;
        if role_verified {
            return Ok(());
        }

        Err(ErrorCode::PermissionDenied(format!(
            "Permission denied, user '{}'@'{}' requires {} privilege on {}",
            &current_user.name, &current_user.hostname, privilege, object
        )))
    }

    pub fn get_settings(self: &Arc<Self>) -> Arc<Settings> {
        Arc::new(self.session_settings.clone())
    }

    pub fn get_session_manager(self: &Arc<Self>) -> Arc<SessionManager> {
        self.session_mgr.clone()
    }

    pub fn get_catalog(self: &Arc<Self>) -> Arc<DatabaseCatalog> {
        self.session_mgr.get_catalog()
    }

    pub fn get_user_manager(self: &Arc<Self>) -> Arc<UserApiProvider> {
        self.session_mgr.get_user_manager()
    }

    pub fn get_memory_usage(self: &Arc<Self>) -> usize {
        malloc_size(self)
    }

    pub fn get_storage_operator(self: &Arc<Self>) -> Operator {
        self.session_mgr.get_storage_operator()
    }
}
