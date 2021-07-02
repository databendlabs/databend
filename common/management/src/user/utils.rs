// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
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
