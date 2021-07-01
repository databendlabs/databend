// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::ErrorCode;
use common_exception::Result;
use common_store_api::KVApi;
use common_store_api::UpsertKVActionResult;
use sha2::Digest;

use crate::user::user_info::NewUser;
use crate::user::user_info::UserInfo;

static USER_API_KEY_PREFIX: &str = "__fd_user_kv_/";

struct UserMgr<KV: KVApi> {
    kv_api: KV,
}

impl<T: KVApi> UserMgr<T> {
    pub(crate) async fn upsert_user(
        &mut self,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
        salt: impl AsRef<str>,
        seq: Option<u64>,
    ) -> common_exception::Result<UpsertKVActionResult> {
        let new_user = NewUser::new(username.as_ref(), password.as_ref(), salt.as_ref());
        self.upsert_user_info(&(&new_user).into(), seq).await
    }

    pub(crate) async fn upsert_user_info(
        &mut self,
        user_info: &UserInfo,
        seq: Option<u64>,
    ) -> common_exception::Result<UpsertKVActionResult> {
        let value = serde_json::to_vec(user_info)?;
        let key = prepend(&user_info.name);
        self.kv_api.upsert_kv(&key, seq, value).await
    }
}

impl<T: KVApi> UserMgr<T> {
    #[allow(dead_code)]
    pub async fn add_user(
        &mut self,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
        salt: impl AsRef<str>,
    ) -> common_exception::Result<()> {
        let res = self.upsert_user(username, password, salt, Some(0)).await?;
        match (res.prev, res.result) {
            (None, Some(_)) => Ok(()), // do we need to check the seq returned?
            (Some((s, _)), None) => Err(ErrorCode::UserAlreadyExists(format!(
                "user already exists, seq [{}]",
                s
            ))),
            r @ (_, _) => Err(ErrorCode::UnknownException(format!(
                "upsert result not expected (using version 0, got {:?})",
                r
            ))),
        }
    }

    #[allow(dead_code)]
    pub async fn drop_user(&mut self, _username: impl AsRef<str>) -> Result<()> {
        todo!()
    }

    #[allow(dead_code)]
    pub async fn update_user(
        &mut self,
        username: impl AsRef<str>,
        new_password: Option<impl AsRef<str>>,
        new_salt: Option<impl AsRef<str>>,
        seq: u64,
    ) -> Result<Option<u64>> {
        let partial_update = new_salt.is_none() || new_password.is_none();
        let user_info = if partial_update {
            let user_info = self
                .get_user(username.as_ref(), Some(seq))
                .await?
                .ok_or_else(|| ErrorCode::UnknownUser(""))?;
            UserInfo {
                password_sha256: new_password.map_or(user_info.password_sha256, |v| {
                    sha2::Sha256::digest(v.as_ref().as_bytes()).into()
                }),
                salt_sha256: new_salt.map_or(user_info.salt_sha256, |v| {
                    sha2::Sha256::digest(v.as_ref().as_bytes()).into()
                }),
                name: username.as_ref().to_string(),
            }
        } else {
            NewUser::new(
                username.as_ref(),
                new_password.unwrap().as_ref(),
                new_salt.unwrap().as_ref(),
            )
            .into()
        };
        let res = self.upsert_user_info(&user_info, Some(seq)).await?;

        match (&(res.prev), &(res.result)) {
            (Some(_), Some((v, _))) => Ok(Some(*v)),
            (_, None) => Err(ErrorCode::UnknownUser(format!(
                "user not found, name [{}],  seq[{}]",
                username.as_ref(),
                seq
            ))),
            (_, _) => Err(ErrorCode::UnknownException(format!(
                "upsert result not expected, response: {:?})",
                res,
            ))),
        }
    }

    #[allow(dead_code)]
    pub async fn get_all_users(&mut self) -> Result<Vec<UserInfo>> {
        todo!()
    }

    #[allow(dead_code)]
    pub async fn get_users(
        &mut self,
        _usernames: &[impl AsRef<str>],
    ) -> Result<Vec<Option<UserInfo>>> {
        todo!()
    }

    #[allow(dead_code)]
    pub async fn get_user(
        &mut self,
        _username: impl AsRef<str>,
        _seq: Option<u64>,
    ) -> Result<Option<UserInfo>> {
        todo!()
    }
}

fn prepend(v: impl AsRef<str>) -> String {
    let mut res = USER_API_KEY_PREFIX.to_string();
    res.push_str(v.as_ref());
    res
}
