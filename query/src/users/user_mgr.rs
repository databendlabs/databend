// Copyright 2020 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::Result;
use common_kv_api::KVApi;
use common_management::AuthType;
use common_management::NewUser;
use common_management::UserInfo;
use common_management::UserMgr;
use common_management::UserMgrApi;
use sha2::Digest;

use crate::common::StoreApiProvider;
use crate::configs::Config;

pub type UserManagerRef = Arc<UserManager>;

pub struct UserManager {
    api_provider: Arc<dyn UserMgrApi>,
}

impl UserManager {
    async fn create_kv_client(cfg: &Config) -> Result<Arc<dyn KVApi>> {
        let store_api_provider = StoreApiProvider::new(cfg);
        match store_api_provider.try_get_kv_client().await {
            Ok(client) => Ok(client),
            Err(cause) => Err(cause.add_message_back("(while create user api).")),
        }
    }

    pub async fn create_global(cfg: Config) -> Result<UserManagerRef> {
        let client = UserManager::create_kv_client(&cfg).await?;
        let tenant = &cfg.query.tenant;
        let user_manager = UserMgr::new(client, tenant);

        Ok(Arc::new(UserManager {
            api_provider: Arc::new(user_manager),
        }))
    }

    // Get one user from by tenant.
    pub fn get_user(&self, user: &str) -> Result<UserInfo> {
        match user {
            // TODO(BohuTANG): Mock, need removed.
            "default" | "" | "root" => {
                let user = NewUser::new(user, "", AuthType::None);
                Ok(user.into())
            }
            _ => Ok(self.api_provider.get_user(user.to_string(), None)?.1),
        }
    }

    pub fn auth_user(&self, user: &str, password: impl AsRef<[u8]>) -> Result<bool> {
        let user = self.get_user(user)?;

        match user.auth_type {
            AuthType::None => Ok(true),
            AuthType::PlainText => Ok(user.password == password.as_ref()),
            AuthType::DoubleSha1 => {
                let mut m = sha1::Sha1::new();
                m.update(password.as_ref());

                let bs = m.digest().bytes();
                let mut m = sha1::Sha1::new();
                m.update(&bs[..]);

                Ok(user.password == m.digest().bytes().to_vec())
            }
            AuthType::Sha256 => {
                let result = sha2::Sha256::digest(password.as_ref());
                Ok(user.password == result.to_vec())
            }
        }
    }

    // Get the tenant all users list.
    pub fn get_users(&self) -> Result<Vec<UserInfo>> {
        let mut res = vec![];
        let users = self.api_provider.get_users()?;
        for user in users {
            res.push(user.1);
        }
        Ok(res)
    }

    // Add a new user info.
    pub fn add_user(&self, user_info: UserInfo) -> Result<u64> {
        self.api_provider.add_user(user_info)
    }

    // Drop a user by name.
    pub fn drop_user(&self, user: &str) -> Result<()> {
        self.api_provider.drop_user(user.to_string(), None)
    }
}
