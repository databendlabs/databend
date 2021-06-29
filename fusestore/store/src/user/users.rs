// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::collections::hash_map::Entry::Occupied;
use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;

use common_exception::ErrorCode;
use common_flights::AddUserActionResult;
use common_flights::DropUserActionResult;
use common_flights::GetAllUsersActionResult;
use common_flights::GetUserActionResult;
use common_flights::GetUsersActionResult;
use common_flights::UpdateUserActionResult;
use common_flights::UserInfo;
use common_infallible::RwLock;
use sha2::Digest;
use sha2::Sha256;

pub struct UserMgr {
    users: RwLock<HashMap<String, UserInfo>>,
}

impl UserMgr {
    pub fn new() -> Self {
        UserMgr {
            users: Default::default(),
        }
    }

    #[allow(dead_code)]
    pub fn add_user(
        &self,
        username: impl Into<String>,
        password: impl Into<String>,
        salt: impl Into<String>,
    ) -> common_exception::Result<AddUserActionResult> {
        let mut users = self.users.write();
        let entry = users.entry(username.into());
        match entry {
            Occupied(e) => Err(ErrorCode::UserAlreadyExists(format!(
                "user name occupied: {}",
                e.key()
            ))),
            Vacant(item) => {
                let name = item.key().to_string();
                item.insert(UserInfo {
                    name,
                    password_sha256: Sha256::digest(password.into().as_bytes()).into(),
                    salt_sha256: Sha256::digest(salt.into().as_bytes()).into(),
                });
                Ok(AddUserActionResult)
            }
        }
    }

    #[allow(dead_code)]
    pub fn drop_user(
        &self,
        username: impl AsRef<str>,
    ) -> common_exception::Result<DropUserActionResult> {
        let mut users = self.users.write();
        match users.remove(username.as_ref()) {
            None => Err(ErrorCode::UnknownUser(format!(
                "user name {}, not found ",
                username.as_ref()
            ))),
            Some(_) => Ok(DropUserActionResult),
        }
    }

    #[allow(dead_code)]
    pub fn update_user(
        &self,
        username: impl Into<String>,
        new_password: Option<String>,
        new_salt: Option<String>,
    ) -> common_exception::Result<UpdateUserActionResult> {
        let mut users = self.users.write();
        let entry = users.entry(username.into());
        match entry {
            Vacant(entry) => Err(ErrorCode::UnknownUser(format!(
                "user name {}, not found ",
                entry.key()
            ))),
            Occupied(mut item) => {
                let user = item.get_mut();
                if let Some(new) = new_password {
                    user.password_sha256 = Sha256::digest(new.as_bytes()).into();
                }
                if let Some(new) = new_salt {
                    user.salt_sha256 = Sha256::digest(new.as_bytes()).into();
                }
                Ok(UpdateUserActionResult)
            }
        }
    }

    #[allow(dead_code)]
    pub fn get_all_users(&self) -> common_exception::Result<GetAllUsersActionResult> {
        let users = self.users.read();
        Ok(GetAllUsersActionResult {
            users_info: users.values().cloned().collect(),
        })
    }

    #[allow(dead_code)]
    pub fn get_users<T: AsRef<str>>(
        &self,
        usernames: &[T],
    ) -> common_exception::Result<GetUsersActionResult> {
        let users = self.users.read();
        let res = usernames
            .iter()
            .map(|name| users.get(name.as_ref()).cloned())
            .collect();
        Ok(GetUsersActionResult { users_info: res })
    }

    #[allow(dead_code)]
    pub fn get_user(
        &self,
        username: impl AsRef<str>,
    ) -> common_exception::Result<GetUserActionResult> {
        let users = self.users.read();
        let res = users.get(username.as_ref()).map(Clone::clone);
        Ok(GetUserActionResult { user_info: res })
    }
}
