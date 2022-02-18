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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_base::tokio;
use common_base::tokio::task::JoinHandle;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::GrantObject;
use common_meta_types::RoleIdentity;
use common_meta_types::RoleInfo;
use common_meta_types::UserPrivilegeType;
use common_tracing::tracing;

use crate::users::UserApiProvider;

struct CachedRoles {
    roles: HashMap<String, RoleInfo>,
    cached_at: Instant,
}

pub struct RoleCacheMgr {
    user_api: Arc<UserApiProvider>,
    cache: Arc<RwLock<HashMap<String, CachedRoles>>>,
    polling_interval: Duration,
    polling_join_handle: Option<JoinHandle<()>>,
}

impl RoleCacheMgr {
    pub fn new(user_api: Arc<UserApiProvider>) -> Self {
        let mut mgr = Self {
            user_api,
            cache: Arc::new(RwLock::new(HashMap::new())),
            polling_interval: Duration::new(15, 0),
            polling_join_handle: None,
        };
        mgr.background_polling();
        mgr
    }

    pub fn background_polling(&mut self) {
        let user_api = self.user_api.clone();
        let cache = self.cache.clone();
        let polling_interval = self.polling_interval;
        self.polling_join_handle = Some(tokio::spawn(async move {
            loop {
                let tenants: Vec<String> = {
                    let cached = cache.read();
                    cached.keys().cloned().collect()
                };
                for tenant in tenants {
                    match load_roles_data(&user_api, &tenant).await {
                        Err(err) => {
                            tracing::warn!(
                                "role_cache_mgr load roles data of tenant {} failed: {}",
                                tenant,
                                err,
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

    pub fn invalidate_cache(&mut self, tenant: &str) {
        let mut cached = self.cache.write();
        cached.remove(tenant);
    }

    pub async fn verify_privilege(
        &self,
        tenant: &str,
        role_identities: &[RoleIdentity],
        object: &GrantObject,
        privilege: UserPrivilegeType,
    ) -> Result<bool> {
        self.maybe_reload(tenant).await?;
        let cached = self.cache.read();
        let cached_roles = match cached.get(tenant) {
            None => return Ok(false),
            Some(cached_roles) => cached_roles,
        };
        let related_roles = find_all_related_roles(&cached_roles.roles, role_identities);
        Ok(related_roles
            .iter()
            .any(|r| r.grants.verify_privilege(object, privilege)))
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
            let data = load_roles_data(&self.user_api, tenant).await?;
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

// An role can be granted with multiple roles, find all the related roles in a DFS manner
pub fn find_all_related_roles(
    cache: &HashMap<String, RoleInfo>,
    role_identities: &[RoleIdentity],
) -> Vec<RoleInfo> {
    let mut visited: HashSet<RoleIdentity> = HashSet::new();
    let mut result: Vec<RoleInfo> = vec![];
    let mut q: VecDeque<RoleIdentity> = role_identities.iter().cloned().collect();
    while let Some(role_identity) = q.pop_front() {
        if visited.contains(&role_identity) {
            continue;
        }
        let cache_key = role_identity.to_string();
        visited.insert(role_identity);
        let role = match cache.get(&cache_key) {
            None => continue,
            Some(role) => role,
        };
        result.push(role.clone());
        for related_role in role.grants.roles() {
            q.push_back(related_role);
        }
    }
    result
}
