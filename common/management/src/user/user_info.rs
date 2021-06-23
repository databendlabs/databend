// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use sha2::Digest;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct UserInfo {
    pub name: String,
    pub password_sha256: [u8; 32],
    pub salt_sha256: [u8; 32],
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct NewUser<'a> {
    name: &'a str,
    password: &'a str,
    salt: &'a str,
}
impl<'a> NewUser<'a> {
    pub(crate) fn new(name: &'a str, password: &'a str, salt: &'a str) -> Self {
        NewUser {
            name,
            password,
            salt,
        }
    }
}

impl From<&NewUser<'_>> for UserInfo {
    fn from(new_user: &NewUser) -> Self {
        UserInfo {
            name: new_user.name.to_string(),
            password_sha256: sha2::Sha256::digest(new_user.password.as_bytes()).into(),
            salt_sha256: sha2::Sha256::digest(&new_user.salt.as_bytes()).into(),
        }
    }
}

impl From<NewUser<'_>> for UserInfo {
    fn from(new_user: NewUser) -> Self {
        UserInfo::from(&new_user)
    }
}
