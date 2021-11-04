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

use common_management::UserInfo;
use common_meta_types::AuthType;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct User {
    name: String,
    host_name: String,
    password: String,
    auth_type: AuthType,
}

impl User {
    pub fn new(
        name: impl Into<String>,
        host_name: impl Into<String>,
        password: impl Into<String>,
        auth_type: AuthType,
    ) -> Self {
        User {
            name: name.into(),
            host_name: host_name.into(),
            password: password.into(),
            auth_type,
        }
    }
}

impl From<&User> for UserInfo {
    fn from(user: &User) -> Self {
        UserInfo {
            name: user.name.clone(),
            host_name: user.host_name.clone(),
            password: Vec::from(user.password.clone()),
            auth_type: user.auth_type.clone(),
        }
    }
}

impl From<User> for UserInfo {
    fn from(user: User) -> Self {
        UserInfo::from(&user)
    }
}
