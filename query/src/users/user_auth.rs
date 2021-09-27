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

use common_management::AuthType;
use common_management::UserInfo;
use sha2::Digest;

pub fn encode_password(password: impl AsRef<[u8]>, auth_type: &AuthType) -> Vec<u8> {
    match auth_type {
        AuthType::None => vec![],
        AuthType::PlainText => password.as_ref().to_vec(),
        AuthType::DoubleSha1 => {
            let mut m = sha1::Sha1::new();
            m.update(password.as_ref());

            let bs = m.digest().bytes();
            let mut m = sha1::Sha1::new();
            m.update(&bs[..]);

            m.digest().bytes().to_vec()
        }
        AuthType::Sha256 => {
            let result = sha2::Sha256::digest(password.as_ref());
            result[..].to_vec()
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct NewUser {
    name: String,
    password: String,
    auth_type: AuthType,
}

impl NewUser {
    pub fn new(name: impl Into<String>, password: impl Into<String>, auth_type: AuthType) -> Self {
        NewUser {
            name: name.into(),
            password: password.into(),
            auth_type,
        }
    }
}

impl From<&NewUser> for UserInfo {
    fn from(new_user: &NewUser) -> Self {
        let encode_password = encode_password(&new_user.password, &new_user.auth_type);
        UserInfo {
            name: new_user.name.clone(),
            password: encode_password,
            auth_type: new_user.auth_type.clone(),
        }
    }
}

impl From<NewUser> for UserInfo {
    fn from(new_user: NewUser) -> Self {
        UserInfo::from(&new_user)
    }
}
