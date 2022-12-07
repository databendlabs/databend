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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use common_config::GlobalConfig;
use common_exception::Result;
use common_meta_types::RoleInfo;
use common_meta_types::UserInfo;
use common_settings::Settings;
use futures::channel::oneshot::Sender;
use parking_lot::RwLock;

use crate::sessions::QueryContextShared;

pub struct SessionContext {
    abort: AtomicBool,
    settings: Arc<Settings>,
    current_catalog: RwLock<String>,
    current_database: RwLock<String>,
    // The current tenant can be determined by databend-query's config file, or by X-DATABEND-TENANT
    // if it's in management mode. If databend-query is not in management mode, the current tenant
    // can not be modified at runtime.
    current_tenant: RwLock<String>,
    // The current user is determined by the authentication phase on each connection. It will not be
    // changed during a session.
    current_user: RwLock<Option<UserInfo>>,
    // Each session have a current role which takes effects, the privileges from the user's other
    // roles will not take effect. The user can switch to another available role by `SET ROLE`.
    // If the current_role is not set, it takes the user's default role.
    current_role: RwLock<Option<RoleInfo>>,
    // The role granted to user by external auth provider, when auth_role is provided, the current
    // user's all other roles are overridden by this role.
    auth_role: RwLock<Option<String>>,
    // The client IP from the client.
    client_host: RwLock<Option<SocketAddr>>,
    io_shutdown_tx: RwLock<Option<Sender<Sender<()>>>>,
    query_context_shared: RwLock<Weak<QueryContextShared>>,
}

impl SessionContext {
    pub fn try_create(settings: Arc<Settings>) -> Result<Arc<Self>> {
        Ok(Arc::new(SessionContext {
            settings,
            abort: Default::default(),
            current_user: Default::default(),
            current_role: Default::default(),
            auth_role: Default::default(),
            current_tenant: Default::default(),
            client_host: Default::default(),
            current_catalog: RwLock::new("default".to_string()),
            current_database: RwLock::new("default".to_string()),
            io_shutdown_tx: Default::default(),
            query_context_shared: Default::default(),
        }))
    }

    // Get abort status.
    pub fn get_abort(&self) -> bool {
        self.abort.load(Ordering::Relaxed)
    }

    // Set abort status.
    pub fn set_abort(&self, v: bool) {
        self.abort.store(v, Ordering::Relaxed);
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.settings.clone()
    }

    pub fn get_changed_settings(&self) -> Arc<Settings> {
        Arc::new(self.settings.get_changed_settings())
    }

    pub fn apply_changed_settings(&self, changed_settings: Arc<Settings>) -> Result<()> {
        self.settings.apply_changed_settings(changed_settings)
    }

    // Get current catalog name.
    pub fn get_current_catalog(&self) -> String {
        let lock = self.current_catalog.read();
        lock.clone()
    }

    // Set current catalog.
    pub fn set_current_catalog(&self, catalog_name: String) {
        let mut lock = self.current_catalog.write();
        *lock = catalog_name
    }

    // Get current database.
    pub fn get_current_database(&self) -> String {
        let lock = self.current_database.read();
        lock.clone()
    }

    // Set current database.
    pub fn set_current_database(&self, db: String) {
        let mut lock = self.current_database.write();
        *lock = db
    }

    // Return the current role if it's set. If the current role is not set, it'll take the user's
    // default role.
    pub fn get_current_role(&self) -> Option<RoleInfo> {
        let lock = self.current_role.read();
        lock.clone()
    }

    pub fn set_current_role(&self, role: Option<RoleInfo>) {
        let mut lock = self.current_role.write();
        *lock = role
    }

    pub fn get_current_tenant(&self) -> String {
        let conf = GlobalConfig::instance();
        if conf.query.management_mode {
            let lock = self.current_tenant.read();
            if !lock.is_empty() {
                return lock.clone();
            }
        }
        conf.query.tenant_id.clone()
    }

    pub fn set_current_tenant(&self, tenant: String) {
        let mut lock = self.current_tenant.write();
        *lock = tenant;
    }

    // Get current user
    pub fn get_current_user(&self) -> Option<UserInfo> {
        let lock = self.current_user.read();
        lock.clone()
    }

    // Set the current user after authentication
    pub fn set_current_user(&self, user: UserInfo) {
        let mut lock = self.current_user.write();
        *lock = Some(user);
    }

    // Get auth role. Auth role is the role granted by authenticator.
    pub fn get_auth_role(&self) -> Option<String> {
        let lock = self.auth_role.read();
        lock.clone()
    }

    pub fn set_auth_role(&self, role: Option<String>) {
        let mut lock = self.auth_role.write();
        *lock = role;
    }

    pub fn get_client_host(&self) -> Option<SocketAddr> {
        let lock = self.client_host.read();
        *lock
    }

    pub fn set_client_host(&self, sock: Option<SocketAddr>) {
        let mut lock = self.client_host.write();
        *lock = sock
    }

    pub fn set_io_shutdown_tx(&self, tx: Option<Sender<Sender<()>>>) {
        let mut lock = self.io_shutdown_tx.write();
        *lock = tx
    }

    //  Take the io_shutdown_tx and the self.io_shuttdown_tx is None.
    pub fn take_io_shutdown_tx(&self) -> Option<Sender<Sender<()>>> {
        let mut lock = self.io_shutdown_tx.write();
        lock.take()
    }

    pub fn get_current_query_id(&self) -> Option<String> {
        self.query_context_shared
            .read()
            .upgrade()
            .map(|shared| shared.init_query_id.read().clone())
    }

    pub fn get_query_context_shared(&self) -> Option<Arc<QueryContextShared>> {
        let lock = self.query_context_shared.read();
        lock.upgrade()
    }

    pub fn set_query_context_shared(&self, ctx: Weak<QueryContextShared>) {
        let mut lock = self.query_context_shared.write();
        *lock = ctx
    }
}
