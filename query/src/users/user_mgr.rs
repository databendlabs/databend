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

use std::sync::Arc;

use common_exception::Result;
use common_management::UserInfo;
use common_management::UserMgr;
use common_management::UserMgrApi;
use common_meta_api::KVApi;
use common_meta_types::AuthType;
use sha2::Digest;

use crate::common::MetaClientProvider;
use crate::configs::Config;
use crate::users::User;

pub type UserManagerRef = Arc<UserManager>;

pub struct UserManager {
    api_provider: Arc<dyn UserMgrApi>,
}

impl UserManager {
    async fn create_kv_client(cfg: &Config) -> Result<Arc<dyn KVApi>> {
        match MetaClientProvider::new(cfg).try_get_kv_client().await {
            Ok(client) => Ok(client),
            Err(cause) => Err(cause.add_message_back("(while create user api).")),
        }
    }

    pub async fn create_global(cfg: Config) -> Result<UserManagerRef> {
        let tenant = &cfg.query.tenant;
        let kv_client = UserManager::create_kv_client(&cfg).await?;

        Ok(Arc::new(UserManager {
            api_provider: Arc::new(UserMgr::new(kv_client, tenant)),
        }))
    }

    // Get one user from by tenant.
    pub async fn get_user(&self, user: &str) -> Result<UserInfo> {
        match user {
            // TODO(BohuTANG): Mock, need removed.
            "default" | "" | "root" => {
                let user = User::new(user, "%", "", AuthType::None);
                Ok(user.into())
            }
            _ => {
                let get_user = self.api_provider.get_user(user.to_string(), None);
                Ok(get_user.await?.data)
            }
        }
    }

    // Auth the user and password for different Auth type.
    pub async fn auth_user(&self, info: CertifiedInfo) -> Result<bool> {
        let user = self.get_user(&info.user_name).await?;

        match user.auth_type {
            AuthType::None => Ok(true),
            AuthType::PlainText => Ok(user.password == info.user_password),
            // MySQL already did x = sha1(x)
            // so we just check double sha1(x)
            AuthType::DoubleSha1 => {
                let mut m = sha1::Sha1::new();
                m.update(&info.user_password);

                let bs = m.digest().bytes();
                let mut m = sha1::Sha1::new();
                m.update(&bs[..]);

                Ok(user.password == m.digest().bytes().to_vec())
            }
            AuthType::Sha256 => {
                let result = sha2::Sha256::digest(&info.user_password);
                Ok(user.password == result.to_vec())
            }
        }
    }

    // Get the tenant all users list.
    pub async fn get_users(&self) -> Result<Vec<UserInfo>> {
        let get_users = self.api_provider.get_users();

        let mut res = vec![];
        match get_users.await {
            Err(failure) => Err(failure.add_message_back("(while get users).")),
            Ok(seq_users_info) => {
                for seq_user_info in seq_users_info {
                    res.push(seq_user_info.data);
                }

                Ok(res)
            }
        }
    }

    // Add a new user info.
    pub async fn add_user(&self, user_info: UserInfo) -> Result<u64> {
        let add_user = self.api_provider.add_user(user_info);
        match add_user.await {
            Ok(res) => Ok(res),
            Err(failure) => Err(failure.add_message_back("(while add user).")),
        }
    }

    // Drop a user by name.
    pub async fn drop_user(&self, user: &str) -> Result<()> {
        let drop_user = self.api_provider.drop_user(user.to_string(), None);
        match drop_user.await {
            Ok(res) => Ok(res),
            Err(failure) => Err(failure.add_message_back("(while drop user).")),
        }
    }
}

pub struct CertifiedInfo {
    pub user_name: String,
    pub user_password: Vec<u8>,
    pub user_client_address: String,
}

impl CertifiedInfo {
    pub fn create(user: &str, password: impl AsRef<[u8]>, address: &str) -> CertifiedInfo {
        CertifiedInfo {
            user_name: user.to_string(),
            user_password: password.as_ref().to_vec(),
            user_client_address: address.to_string(),
        }
    }
}
