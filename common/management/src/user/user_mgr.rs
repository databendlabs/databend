// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_metatypes::MatchSeq;
use common_metatypes::SeqValue;
use common_store_api::KVApi;
use sha2::Digest;

use crate::user::user_api::UserInfo;
use crate::user::user_api::UserMgrApi;
use crate::user::utils;
use crate::user::utils::NewUser;

pub static USER_API_KEY_PREFIX: &str = "__fd_users/";

pub struct UserMgr<KV> {
    kv_api: KV,
}

impl<T> UserMgr<T>
where T: KVApi
{
    #[allow(dead_code)]
    pub fn new(kv_api: T) -> Self {
        UserMgr { kv_api }
    }
}

#[async_trait]
impl<T: KVApi + Send> UserMgrApi for UserMgr<T> {
    async fn add_user<U, V, W>(
        &mut self,
        username: U,
        password: V,
        salt: W,
    ) -> common_exception::Result<u64>
    where
        U: AsRef<str> + Send,
        V: AsRef<str> + Send,
        W: AsRef<str> + Send,
    {
        let new_user = NewUser::new(username.as_ref(), password.as_ref(), salt.as_ref());
        let ui: UserInfo = new_user.into();
        let value = serde_json::to_vec(&ui)?;
        let key = utils::prepend(&ui.name);

        // Only when there are no record, i.e. seq=0
        let match_seq = MatchSeq::Exact(0);

        let res = self.kv_api.upsert_kv(&key, match_seq, value).await?;

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

    async fn get_user<V: AsRef<str> + Send>(
        &mut self,
        username: V,
        seq: Option<u64>,
    ) -> Result<SeqValue<UserInfo>> {
        let key = utils::prepend(username.as_ref());
        let resp = self.kv_api.get_kv(&key).await?;

        let seq_value = resp
            .result
            .ok_or_else(|| ErrorCode::UnknownUser(format!("unknown user {}", username.as_ref())))?;

        let curr_seq = seq_value.0;

        let ms: MatchSeq = seq.into();
        ms.match_seq(curr_seq)
            .map_err_to_code(ErrorCode::UnknownUser, || {
                format!("username: {}", username.as_ref(),)
            })?;

        let user_info = serde_json::from_slice(&seq_value.1)
            .map_err_to_code(ErrorCode::IllegalUserInfoFormat, || "")?;

        Ok((curr_seq, user_info))
    }

    async fn get_all_users(&mut self) -> Result<Vec<SeqValue<UserInfo>>> {
        let values = self.kv_api.prefix_list_kv(USER_API_KEY_PREFIX).await?;
        let mut r = vec![];
        for v in values {
            let (_key, (s, val)) = v;
            let u = serde_json::from_slice::<UserInfo>(&val)
                .map_err_to_code(ErrorCode::IllegalUserInfoFormat, || "")?;

            r.push((s, u));
        }
        Ok(r)
    }

    async fn get_users<V: AsRef<str> + Sync>(
        &mut self,
        usernames: &[V],
    ) -> Result<Vec<Option<SeqValue<UserInfo>>>> {
        let keys = usernames
            .iter()
            .map(utils::prepend)
            .collect::<Vec<String>>();
        let values = self.kv_api.mget_kv(&keys).await?;
        let mut r = vec![];
        for v in values.result {
            match v {
                Some(v) => {
                    let u = serde_json::from_slice::<UserInfo>(&v.1)
                        .map_err_to_code(ErrorCode::IllegalUserInfoFormat, || "")?;
                    r.push(Some((v.0, u)));
                }
                None => r.push(None),
            }
        }
        Ok(r)
    }

    async fn update_user<V: AsRef<str> + Sync + Send>(
        &mut self,
        username: V,
        new_password: Option<V>,
        new_salt: Option<V>,
        seq: Option<u64>,
    ) -> Result<Option<u64>> {
        if new_password.is_none() && new_salt.is_none() {
            return Ok(seq);
        }
        let partial_update = new_salt.is_none() || new_password.is_none();
        let user_info = if partial_update {
            let user_val_seq = self.get_user(username.as_ref(), seq).await?;
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
        let key = utils::prepend(&user_info.name);

        let match_seq = seq.into();
        let res = self.kv_api.upsert_kv(&key, match_seq, value).await?;
        match res.result {
            Some((s, _)) => Ok(Some(s)),
            None => Err(ErrorCode::UnknownUser(format!(
                "unknown user, or seq not match {}",
                username.as_ref()
            ))),
        }
    }

    async fn drop_user<V: AsRef<str> + Send>(
        &mut self,
        username: V,
        seq: Option<u64>,
    ) -> Result<()> {
        let key = utils::prepend(username.as_ref());
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
}
