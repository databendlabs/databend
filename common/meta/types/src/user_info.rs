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

use std::convert::TryFrom;

use common_exception::ErrorCode;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use crate::user_grant::UserGrantSet;
use crate::AuthInfo;
use crate::UserIdentity;
use crate::UserQuota;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct UserInfo {
    pub name: String,

    pub hostname: String,

    pub auth_info: AuthInfo,

    pub grants: UserGrantSet,

    pub quota: UserQuota,
}

impl UserInfo {
    pub fn new(name: String, hostname: String, auth_info: AuthInfo) -> Self {
        // Default is no privileges.
        let grants = UserGrantSet::default();
        let quota = UserQuota::no_limit();

        UserInfo {
            name,
            hostname,
            auth_info,
            grants,
            quota,
        }
    }

    pub fn new_no_auth(name: String, hostname: String) -> Self {
        UserInfo::new(name, hostname, AuthInfo::None)
    }

    pub fn identity(&self) -> UserIdentity {
        UserIdentity {
            username: self.name.clone(),
            hostname: self.hostname.clone(),
        }
    }
}

impl TryFrom<Vec<u8>> for UserInfo {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(user_info) => Ok(user_info),
            Err(serialize_error) => Err(ErrorCode::IllegalUserInfoFormat(format!(
                "Cannot deserialize user info from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}
