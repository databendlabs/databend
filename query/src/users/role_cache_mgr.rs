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
use common_meta_types::RoleInfo;
use common_meta_types::UserIdentity;
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
    cache: Arc<RwLock<CachedRoles>>,
    polling_interval: Duration,
    polling_join_handle: Option<JoinHandle<()>>,
}

impl RoleCacheMgr {
    pub fn new(user_api: Arc<UserApiProvider>) -> Self {
        Self {
            user_api,
            cache: Arc::new(RwLock::new(CachedRoles::empty())),
            polling_interval: Duration::new(15, 0),
            polling_join_handle: None,
        }
    }

    pub fn start_polling(&mut self, tenant: &str) {
        let user_api = self.user_api.clone();
        let cache = self.cache.clone();
        let tenant = tenant.to_string();
        let polling_interval = self.polling_interval;
        let handle = tokio::spawn(async move {
            loop {
                match load_roles_data(&user_api, &tenant).await {
                    Err(err) => tracing::warn!("role_cache_mgr load roles data failed: {}", err),
                    Ok(data) => {
                        let mut cached = cache.write();
                        *cached = data;
                    }
                }
                tokio::time::sleep(polling_interval).await
            }
        });
        self.polling_join_handle.replace(handle);
    }

    pub fn invalidate_cache(&mut self) {
        let mut cached = self.cache.write();
        *cached = CachedRoles::empty();
    }

    pub async fn validate_privilege(
        &self,
        tenant: &str,
        _roles: &[UserIdentity],
        _object: &GrantObject,
        _privilege: UserPrivilegeType,
    ) -> Result<()> {
        if self.need_reload() {
            let data = load_roles_data(&self.user_api, tenant).await?;
            let mut cached = self.cache.write();
            *cached = data;
        }
        let _cached = self.cache.read();
        // TODO
        Ok(())
    }

    fn need_reload(&self) -> bool {
        let cached = self.cache.read();
        let last_cache_time: &Option<Instant> = &cached.last_cache_time;
        match last_cache_time {
            None => true,
            Some(t) => t.elapsed() >= self.polling_interval,
        }
    }
}

fn make_role_cache_key(tenant: &str, role_identity: &UserIdentity) -> String {
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
