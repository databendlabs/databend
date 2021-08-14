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
//

use sha2::Digest;

use crate::user::user_api::UserInfo;
use crate::user::user_mgr::USER_API_KEY_PREFIX;

pub(crate) fn prepend(v: impl AsRef<str>) -> String {
    let mut res = USER_API_KEY_PREFIX.to_string();
    res.push_str(v.as_ref());
    res
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct NewUser {
    name: String,
    password: String,
    salt: String,
}
impl NewUser {
    pub(crate) fn new(
        name: impl Into<String>,
        password: impl Into<String>,
        salt: impl Into<String>,
    ) -> Self {
        NewUser {
            name: name.into(),
            password: password.into(),
            salt: salt.into(),
        }
    }
}

impl From<&NewUser> for UserInfo {
    fn from(new_user: &NewUser) -> Self {
        UserInfo {
            name: new_user.name.clone(),
            password_sha256: sha2::Sha256::digest(new_user.password.as_bytes()).into(),
            salt_sha256: sha2::Sha256::digest(new_user.salt.as_bytes()).into(),
        }
    }
}

impl From<NewUser> for UserInfo {
    fn from(new_user: NewUser) -> Self {
        UserInfo::from(&new_user)
    }
}
