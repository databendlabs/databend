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
//

use std::convert::TryFrom;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::user_grant::UserGrantSet;
use crate::AuthType;
use crate::UserPrivilege;
use crate::UserQuota;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct UserInfo {
    #[serde(default)]
    pub name: String,

    #[serde(default)]
    pub hostname: String,

    #[serde(default)]
    pub password: Vec<u8>,

    #[serde(default)]
    pub auth_type: AuthType,

    #[serde(default)]
    pub privileges: UserPrivilege, // TODO: remove this field after the grants field take effects

    #[serde(default)]
    pub grants: UserGrantSet,

    #[serde(default)]
    pub quota: UserQuota,
}

impl UserInfo {
    pub fn new(name: String, hostname: String, password: Vec<u8>, auth_type: AuthType) -> Self {
        // Default is no privileges.
        let privileges = UserPrivilege::empty();
        let grants = UserGrantSet::default();
        let quota = UserQuota::no_limit();

        UserInfo {
            name,
            hostname,
            password,
            auth_type,
            privileges,
            grants,
            quota,
        }
    }

    // TODO: remove this after grants field take effects
    pub fn set_privileges(&mut self, privileges: UserPrivilege) {
        self.privileges |= privileges;
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
