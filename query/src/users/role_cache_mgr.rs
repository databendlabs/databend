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
    last_cache_time: Option<Instant>,
}

impl CachedRoles {
    fn empty() -> Self {
        Self {
            roles: HashMap::new(),
            last_cache_time: None,
        }
    }
}

pub struct RoleCacheMgr {
    user_api: Arc<UserApiProvider>,
    cache: Arc<RwLock<HashMap<String, CachedRoles>>>,
    polling_interval: Duration,
    polling_join_handle: Option<JoinHandle<()>>,
}

impl RoleCacheMgr {
    pub fn new(user_api: Arc<UserApiProvider>) -> Self {
        Self {
            user_api,
            cache: Arc::new(RwLock::new(HashMap::empty())),
            polling_interval: Duration::new(15, 0),
            polling_join_handle: None,
        }
    }

    pub fn start_polling(&mut self) {
        let user_api = self.user_api.clone();
        let cache = self.cache.clone();
        let polling_interval = self.polling_interval;
        let handle = tokio::spawn(async move {
            loop {
                let tenants: Vec<String> = {
                    let cached = cache.read();
                    cached.keys().collect()
                };
                for tenant in tenants {
                    match load_roles_data(&user_api, &tenant).await {
                        Err(err) => {
                            tracing::warn!("role_cache_mgr load roles data failed: {}", err)
                        }
                        Ok(data) => {
                            let mut cached = cache.write();
                            cached.insert(tenant.to_string(), data);
                        }
                    }
                }
                tokio::time::sleep(polling_interval).await
            }
        });
        self.polling_join_handle.replace(handle);
    }

    pub fn invalidate_cache(&mut self, tenant: &str) {
        let mut cached = self.cache.write();
        cached.insert(tenant.to_string(), CachedRoles::empty());
    }

    pub async fn verify_privilege(
        &self,
        tenant: &str,
        role_identies: &[RoleIdentity],
        _object: &GrantObject,
        _privilege: UserPrivilegeType,
    ) -> Result<bool> {
        self.maybe_reload(tenant);
        Ok(false)
    }

    // Load roles data if not found in cache. Watch this tenant's role data in background if
    // once it loads successfully.
    async fn maybe_reload(&self, tenant: &str) -> Result<()> {
        let need_reload = {
            let cached = self.cache.read();
            cached.get(tenant).is_none()
        };
        if need_reload {
            let data = load_roles_data(&self.user_api, tenant).await?;
            let mut cached = self.cache.write();
            cached.insert(tenant.to_string(), data);
        }
        Ok(())
    }
}

fn make_role_cache_key(tenant: &str, role_identity: &RoleIdentity) -> String {
    format!("{}/{}", tenant, role_identity)
}

async fn load_roles_data(user_api: &Arc<UserApiProvider>, tenant: &str) -> Result<CachedRoles> {
    let roles = user_api.get_roles(tenant).await?;
    let roles_map = roles
        .into_iter()
        .map(|r| (make_role_cache_key(tenant, &r.identity()), r))
        .collect::<HashMap<_, _>>();
    Ok(CachedRoles {
        roles: roles_map,
        last_cache_time: Some(Instant::now()),
    })
}

// An role can be granted with multiple roles, find all the related roles in a DFS manner
fn find_all_related_roles(
    _cache: &HashMap<String, RoleInfo>,
    _roles: &[RoleIdentity],
    _tenant: &str,
) -> Vec<RoleInfo> {
    vec![]
}
