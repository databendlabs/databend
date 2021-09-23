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

use std::convert::TryFrom;

use common_exception::ErrorCode;
use common_exception::Result;
use common_metatypes::SeqValue;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum AuthType {
    None = 0,
    PlainText = 1,
    DoubleSha1 = 2,
    Sha256 = 3,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct UserInfo {
    pub name: String,
    pub password: Vec<u8>,
    pub auth_type: AuthType,
}

impl UserInfo {
    pub(crate) fn new(name: String, password: Vec<u8>, auth_type: AuthType) -> Self {
        UserInfo {
            name,
            password,
            auth_type,
        }
    }
}

pub trait UserMgrApi {
    fn add_user(&self, user_info: UserInfo) -> common_exception::Result<u64>;

    fn get_user(
        &self,
        username: String,
        seq: Option<u64>,
    ) -> common_exception::Result<SeqValue<UserInfo>>;

    fn get_users(&self) -> Result<Vec<SeqValue<UserInfo>>>;

    fn update_user(
        &self,
        username: String,
        new_password: Option<Vec<u8>>,
        new_auth: Option<AuthType>,
        seq: Option<u64>,
    ) -> Result<Option<u64>>;

    fn drop_user(&self, username: String, seq: Option<u64>) -> Result<()>;
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
