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
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::tenant::Tenant;
use log::warn;
use parking_lot::RwLock;
use tokio::task::JoinHandle;

use crate::UserApiProvider;
use crate::role_util::find_all_related_roles;

struct CachedRoles {
    roles: HashMap<String, RoleInfo>,
    cached_at: Instant,
}

pub struct RoleCacheManager {
    user_manager: Arc<UserApiProvider>,
    cache: Arc<RwLock<HashMap<Tenant, CachedRoles>>>,
    polling_interval: Duration,
    polling_join_handle: Option<JoinHandle<()>>,
}

impl RoleCacheManager {
    pub fn init() -> Result<()> {
        // Check that the user API has been initialized.
        let instance = UserApiProvider::instance();

        GlobalInstance::set(Self::try_create(instance)?);
        Ok(())
    }

    pub fn try_create(user_manager: Arc<UserApiProvider>) -> Result<Arc<RoleCacheManager>> {
        let mut role_cache_manager = Self {
            user_manager,
            polling_join_handle: None,
            cache: Arc::new(RwLock::new(HashMap::new())),
            polling_interval: Duration::new(15, 0),
        };

        role_cache_manager.background_polling();
        Ok(Arc::new(role_cache_manager))
    }

    pub fn instance() -> Arc<RoleCacheManager> {
        GlobalInstance::get()
    }

    pub fn background_polling(&mut self) {
        let cache = self.cache.clone();
        let polling_interval = self.polling_interval;
        let user_manager = self.user_manager.clone();
        self.polling_join_handle = Some(databend_common_base::runtime::spawn(async move {
            loop {
                let tenants = {
                    let cached = cache.read();
                    cached.keys().cloned().collect::<Vec<_>>()
                };
                for tenant in tenants {
                    match load_roles_data(&user_manager, &tenant).await {
                        Err(err) => {
                            warn!(
                                "role_cache_mgr load roles data of tenant {} failed: {}",
                                tenant.display(),
                                err,
                            )
                        }
                        Ok(data) => {
                            let mut cached = cache.write();
                            cached.insert(tenant, data);
                        }
                    }
                }
                tokio::time::sleep(polling_interval).await
            }
        }));
    }

    pub fn invalidate_cache(&self, tenant: &Tenant) {
        let mut cached = self.cache.write();
        cached.remove(tenant);
    }

    #[async_backtrace::framed]
    pub async fn find_role(&self, tenant: &Tenant, role: &str) -> Result<Option<RoleInfo>> {
        let cached = self.cache.read();
        let cached_roles = match cached.get(tenant) {
            None => return Ok(None),
            Some(cached_roles) => cached_roles,
        };
        Ok(cached_roles.roles.get(role).cloned())
    }

    // TODO(liyz): really cache it if got any performance issue. IMHO the ownership data won't become a big memory.
    #[async_backtrace::framed]
    pub async fn find_object_owner(
        &self,
        tenant: &Tenant,
        object: &OwnershipObject,
    ) -> Result<Option<String>> {
        match self.user_manager.get_ownership(tenant, object).await? {
            None => return Ok(None),
            Some(owner) => Ok(Some(owner.role)),
        }
    }

    // find_related_roles is called on validating an user's privileges.
    #[async_backtrace::framed]
    pub async fn find_related_roles(
        &self,
        tenant: &Tenant,
        roles: &[String],
    ) -> Result<Vec<RoleInfo>> {
        let cached_roles = match self.maybe_reload(tenant).await? {
            Some(roles) => roles,
            None => {
                let cached_roles = {
                    let cached = self.cache.read();
                    cached
                        .get(tenant)
                        .map(|cached_roles| cached_roles.roles.clone())
                };
                match cached_roles {
                    Some(cached_roles) => cached_roles,
                    None => self.load_and_cache_roles(tenant).await?,
                }
            }
        };
        Ok(find_all_related_roles(&cached_roles, roles))
    }

    #[async_backtrace::framed]
    pub async fn force_reload(&self, tenant: &Tenant) -> Result<()> {
        self.load_and_cache_roles(tenant).await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn load_and_cache_roles(&self, tenant: &Tenant) -> Result<HashMap<String, RoleInfo>> {
        let data = load_roles_data(&self.user_manager, tenant).await?;
        let roles = data.roles.clone();
        let mut cached = self.cache.write();
        cached.insert(tenant.clone(), data);
        Ok(roles)
    }

    // Load roles data if not found in cache. Watch this tenant's role data in background if
    // once it loads successfully.
    #[async_backtrace::framed]
    async fn maybe_reload(&self, tenant: &Tenant) -> Result<Option<HashMap<String, RoleInfo>>> {
        let need_reload = {
            let cached = self.cache.read();
            match cached.get(tenant) {
                None => true,
                Some(cached_roles) => {
                    // force reload the data when:
                    // - if the cache is too old (the background polling task
                    //   may got some network errors, leaves the cache outdated)
                    // - if the cache is empty
                    cached_roles.cached_at.elapsed() >= self.polling_interval * 2
                        || cached_roles.roles.is_empty()
                }
            }
        };
        if need_reload {
            return self.load_and_cache_roles(tenant).await.map(Some);
        }
        Ok(None)
    }
}

async fn load_roles_data(user_api: &Arc<UserApiProvider>, tenant: &Tenant) -> Result<CachedRoles> {
    let roles = user_api.get_roles(tenant).await?;
    let roles_map = roles
        .into_iter()
        .map(|r| (r.identity().to_string(), r))
        .collect::<HashMap<_, _>>();
    Ok(CachedRoles {
        roles: roles_map,
        cached_at: Instant::now(),
    })
}
