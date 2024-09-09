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

use dashmap::DashMap;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::task::JoinHandle;
use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::tenant::Tenant;
use log::warn;

use crate::role_util::find_all_related_roles;
use crate::UserApiProvider;

struct CachedRoles {
    roles: HashMap<String, RoleInfo>,
    cached_at: Instant,
}

pub struct RoleCacheManager {
    user_manager: Arc<UserApiProvider>,
    cache: Arc<DashMap<Tenant, CachedRoles>>,
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
            cache: Arc::new(DashMap::new()),
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
                for entry in cache.iter() {
                    let tenant = entry.key().clone();
                    match load_roles_data(&user_manager, &tenant).await {
                        Err(err) => {
                            warn!(
                                "role_cache_mgr load roles data of tenant {} failed: {}",
                                tenant.display(),
                                err,
                            )
                        }
                        Ok(data) => {
                            cache.insert(tenant, data);
                        }
                    }
                }
                tokio::time::sleep(polling_interval).await
            }
        }));
    }

    pub fn invalidate_cache(&self, tenant: &Tenant) {
        self.cache.remove(tenant);
    }

    #[async_backtrace::framed]
    pub async fn find_role(&self, tenant: &Tenant, role: &str) -> Result<Option<RoleInfo>> {
        let cached_roles = match self.cache.get(tenant) {
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
        self.maybe_reload(tenant).await?;

        match self.cache.get(tenant) {
            None => Ok(vec![]),
            Some(cached_roles) => Ok(find_all_related_roles(&cached_roles.roles, roles)),
        }
    }

    #[async_backtrace::framed]
    pub async fn force_reload(&self, tenant: &Tenant) -> Result<()> {
        let data = load_roles_data(&self.user_manager, tenant).await?;
        self.cache.insert(tenant.clone(), data);
        Ok(())
    }

    // Load roles data if not found in cache. Watch this tenant's role data in background if
    // once it loads successfully.
    #[async_backtrace::framed]
    async fn maybe_reload(&self, tenant: &Tenant) -> Result<()> {
        let need_reload = {
            match self.cache.get(tenant) {
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
            self.force_reload(tenant).await?;
        }
        Ok(())
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
