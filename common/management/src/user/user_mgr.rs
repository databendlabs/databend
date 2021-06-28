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
pub static USER_API_KEY_PREFIX: &str = "__fd_users/";

pub struct UserMgrImpl<KV> {
    kv_api: KV,
}

impl<T> UserMgrImpl<T>
where T: KVApi
{
    #[allow(dead_code)]
    pub fn new(kv_api: T) -> Self {
        UserMgrImpl { kv_api }
    }
}

impl<T: KVApi> UserMgrImpl<T> {
    async fn upsert_user(
        &mut self,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
        salt: impl AsRef<str>,
        seq: Option<u64>,
    ) -> common_exception::Result<UpsertKVActionResult> {
        let new_user = NewUser::new(username.as_ref(), password.as_ref(), salt.as_ref());
        self.upsert_user_info(&(&new_user).into(), seq).await
    }

    async fn upsert_user_info(
        &mut self,
        user_info: &UserInfo,
        seq: Option<u64>,
    ) -> common_exception::Result<UpsertKVActionResult> {
        let value = serde_json::to_vec(user_info)?;
        let key = prepend(&user_info.name);
        self.kv_api.upsert_kv(&key, seq, value).await
    }
}

impl<T: KVApi> UserMgrImpl<T> {
    #[allow(dead_code)]
    pub async fn add_user(
        &mut self,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
        salt: impl AsRef<str>,
    ) -> common_exception::Result<u64> {
        let res = self.upsert_user(username, password, salt, Some(0)).await?;
        match (res.prev, res.result) {
            (None, Some((s, _))) => Ok(s), // do we need to check the seq returned?
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
    pub async fn drop_user(&mut self, username: impl AsRef<str>, seq: Option<u64>) -> Result<()> {
        let key = prepend(username.as_ref());
        let r = self.kv_api.delete_kv(&key, seq).await?;
        if r.is_some() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownUser(format!(
                "unknown user {}",
                username.as_ref()
            )))
        }
    }

    #[allow(dead_code)]
    pub async fn update_user(
        &mut self,
        username: impl AsRef<str>,
        new_password: Option<impl AsRef<str>>,
        new_salt: Option<impl AsRef<str>>,
        seq: Option<u64>,
    ) -> Result<Option<u64>> {
        if new_password.is_none() && new_salt.is_none() {
            return Ok(seq);
        }
        let partial_update = new_salt.is_none() || new_password.is_none();
        let user_info = if partial_update {
            let user_val_seq = self
                .get_user(username.as_ref(), seq)
                .await?
                .ok_or_else(|| ErrorCode::UnknownUser(""))?;
            let user_info = user_val_seq.1;
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

        let value = serde_json::to_vec(&user_info)?;
        let key = prepend(&user_info.name);
        let res = self.kv_api.update_kv(&key, seq, value).await?;
        match res {
            Some((s, _)) => Ok(Some(s)),
            None => Err(ErrorCode::UnknownUser(format!(
                "unknown user, or seq not match {}",
                username.as_ref()
            ))),
        }
    }

    #[allow(dead_code)]
    pub async fn get_user(
        &mut self,
        username: impl AsRef<str>,
        seq: Option<u64>,
    ) -> Result<Option<(u64, UserInfo)>> {
        let key = prepend(username.as_ref());
        let value = self.kv_api.get_kv(&key).await?;
        let res = value.result;
        let f = |s, val| {
            let user_info = serde_json::from_slice(val);
            let user_info =
                user_info.map_err(|e| ErrorCode::IllegalUserInfoFormat(e.to_string()))?;
            Ok(Some((s, user_info)))
        };
        match res {
            Some((s, val)) if seq.is_none() => f(s, val.as_slice()),
            Some((s, val)) if seq.unwrap() == s => f(s, val.as_slice()),
            _ => Err(ErrorCode::UnknownUser(format!(
                "unknown user {}",
                username.as_ref()
            ))),
        }
    }

    #[allow(dead_code)]
    pub async fn get_all_users(&mut self) -> Result<Vec<(u64, UserInfo)>> {
        let values = self.kv_api.prefix_list_kv(USER_API_KEY_PREFIX).await?;
        let mut r = vec![];
        for v in values {
            let u = serde_json::from_slice::<UserInfo>(&v.1);
            let val = match u {
                Err(e) => {
                    return Err(ErrorCode::IllegalUserInfoFormat(e.to_string()));
                }
                Ok(v) => v,
            };
            r.push((v.0, val));
        }
        Ok(r)
    }

    #[allow(dead_code)]
    pub async fn get_users<V: AsRef<str>>(
        &mut self,
        usernames: &[V],
    ) -> Result<Vec<Option<(u64, UserInfo)>>> {
        let keys = usernames.iter().map(prepend).collect::<Vec<String>>();
        let values = self.kv_api.mget_kv(&keys).await?;
        let mut r = vec![];
        for v in values.result {
            match v {
                Some(v) => {
                    let u = match serde_json::from_slice::<UserInfo>(&v.1) {
                        Err(e) => {
                            return Err(ErrorCode::IllegalUserInfoFormat(e.to_string()));
                        }
                        Ok(val) => val,
                    };
                    r.push(Some((v.0, u)));
                }
                None => r.push(None),
            }
        }
        Ok(r)
    }
}

pub(crate) fn prepend(v: impl AsRef<str>) -> String {
    let mut res = USER_API_KEY_PREFIX.to_string();
    res.push_str(v.as_ref());
    res
}
