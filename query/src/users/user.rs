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

use common_meta_types::AuthInfo;
use common_meta_types::UserGrantSet;
use common_meta_types::UserInfo;
use common_meta_types::UserQuota;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct User {
    name: String,
    hostname: String,
    auth_data: AuthInfo,
}

impl User {
    pub fn new(name: impl Into<String>, hostname: impl Into<String>, auth_info: AuthInfo) -> Self {
        User {
            name: name.into(),
            hostname: hostname.into(),
            auth_data: auth_info,
        }
    }
}

impl From<&User> for UserInfo {
    fn from(user: &User) -> Self {
        let grants = UserGrantSet::empty();
        let quota = UserQuota::no_limit();

        UserInfo {
            name: user.name.clone(),
            hostname: user.hostname.clone(),
            auth_info: user.auth_data.clone(),
            grants,
            quota,
        }
    }
}

impl From<User> for UserInfo {
    fn from(user: User) -> Self {
        UserInfo::from(&user)
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
