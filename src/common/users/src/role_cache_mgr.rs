// Copyright 2022 Datafuse Labs.
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

use common_base::base::tokio;
use common_base::base::tokio::task::JoinHandle;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::RoleInfo;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tracing::warn;

use crate::role_util::find_all_related_roles;
use crate::UserApiProvider;

struct CachedRoles {
    roles: HashMap<String, RoleInfo>,
    cached_at: Instant,
}

pub struct RoleCacheManager {
    user_manager: Arc<UserApiProvider>,
    cache: Arc<RwLock<HashMap<String, CachedRoles>>>,
    polling_interval: Duration,
    polling_join_handle: Option<JoinHandle<()>>,
}

static ROLE_CACHE_MANAGER: OnceCell<Arc<RoleCacheManager>> = OnceCell::new();

impl RoleCacheManager {
    pub fn init() -> Result<()> {
        // Check that the user API has been initialized.
        let instance = UserApiProvider::instance();

        let role_cache_manager = Self::try_create(instance)?;

        match ROLE_CACHE_MANAGER.set(role_cache_manager) {
            Ok(_) => Ok(()),
            Err(_) => Err(ErrorCode::LogicalError(
                "Cannot init RoleCacheManager twice",
            )),
        }
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
        match ROLE_CACHE_MANAGER.get() {
            None => panic!("RoleCacheManager is not init"),
            Some(role_cache_manager) => role_cache_manager.clone(),
        }
    }

    pub fn destroy() {
        unsafe {
            let const_ptr = &ROLE_CACHE_MANAGER as *const OnceCell<Arc<RoleCacheManager>>;
            let mut_ptr = const_ptr as *mut OnceCell<Arc<RoleCacheManager>>;

            if let Some(role_cache_manager) = (*mut_ptr).take() {
                drop(role_cache_manager);
            }
        }
    }

    pub fn background_polling(&mut self) {
        let cache = self.cache.clone();
        let polling_interval = self.polling_interval;
        let user_manager = self.user_manager.clone();
        self.polling_join_handle = Some(tokio::spawn(async move {
            loop {
                let tenants: Vec<String> = {
                    let cached = cache.read();
                    cached.keys().cloned().collect()
                };
                for tenant in tenants {
                    match load_roles_data(&user_manager, &tenant).await {
                        Err(err) => {
                            warn!(
                                "role_cache_mgr load roles data of tenant {} failed: {}",
                                tenant, err,
                            )
                        }
                        Ok(data) => {
                            let mut cached = cache.write();
                            cached.insert(tenant.to_string(), data);
                        }
                    }
                }
                tokio::time::sleep(polling_interval).await
            }
        }));
    }

    pub fn invalidate_cache(&self, tenant: &str) {
        let mut cached = self.cache.write();
        cached.remove(tenant);
    }

    // find_related_roles is called on validating an user's privileges.
    pub async fn find_related_roles(
        &self,
        tenant: &str,
        roles: &[String],
    ) -> Result<Vec<RoleInfo>> {
        self.maybe_reload(tenant).await?;
        let cached = self.cache.read();
        let cached_roles = match cached.get(tenant) {
            None => return Ok(vec![]),
            Some(cached_roles) => cached_roles,
        };
        Ok(find_all_related_roles(&cached_roles.roles, roles))
    }

    // Load roles data if not found in cache. Watch this tenant's role data in background if
    // once it loads successfully.
    async fn maybe_reload(&self, tenant: &str) -> Result<()> {
        let need_reload = {
            let cached = self.cache.read();
            match cached.get(tenant) {
                None => true,
                Some(cached_roles) => {
                    // if the cache is too old, force reload the data (the background polling task
                    // may got some network errors, leaves the cache outdated)
                    cached_roles.cached_at.elapsed() >= self.polling_interval * 2
                }
            }
        };
        if need_reload {
            let data = load_roles_data(&self.user_manager, tenant).await?;
            let mut cached = self.cache.write();
            cached.insert(tenant.to_string(), data);
        }
        Ok(())
    }
}

async fn load_roles_data(user_api: &Arc<UserApiProvider>, tenant: &str) -> Result<CachedRoles> {
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
