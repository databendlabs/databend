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

use std::convert::TryInto;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_metatypes::MatchSeq;
use common_metatypes::MatchSeqExt;
use common_metatypes::SeqValue;
use common_store_api::KVApi;

use super::user_api::AuthType;
use crate::user::user_api::UserInfo;
use crate::user::user_api::UserMgrApi;
use crate::user::utils;

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
    async fn add_user(&mut self, user_info: UserInfo) -> common_exception::Result<u64> {
        let value = serde_json::to_vec(&user_info)?;
        let key = utils::prepend(&user_info.name);

        // Only when there are no record, i.e. seq=0
        let match_seq = MatchSeq::Exact(0);

        let res = self
            .kv_api
            .upsert_kv(&key, match_seq, Some(value), None)
            .await?;

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

        match MatchSeq::from(seq).match_seq(&seq_value) {
            Ok(_) => Ok((seq_value.0, seq_value.1.value.try_into()?)),
            Err(_) => Err(ErrorCode::UnknownUser(format!(
                "username: {}",
                username.as_ref()
            ))),
        }
    }

    async fn get_all_users(&mut self) -> Result<Vec<SeqValue<UserInfo>>> {
        let values = self.kv_api.prefix_list_kv(USER_API_KEY_PREFIX).await?;
        let mut r = vec![];
        for (_key, (s, val)) in values {
            let u = serde_json::from_slice::<UserInfo>(&val.value)
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
                    let u = serde_json::from_slice::<UserInfo>(&v.1.value)
                        .map_err_to_code(ErrorCode::IllegalUserInfoFormat, || "")?;
                    r.push(Some((v.0, u)));
                }
                None => r.push(None),
            }
        }
        Ok(r)
    }

    async fn update_user<U, V>(
        &mut self,
        username: U,
        new_password: Option<V>,
        new_auth: Option<AuthType>,
        seq: Option<u64>,
    ) -> Result<Option<u64>>
    where
        U: AsRef<str> + Sync + Send,
        V: AsRef<[u8]> + Sync + Send,
    {
        if new_password.is_none() && new_auth.is_none() {
            return Ok(seq);
        }
        let partial_update = new_auth.is_none() || new_password.is_none();
        let user_info = if partial_update {
            let user_val_seq = self.get_user(username.as_ref(), seq).await?;
            let user_info = user_val_seq.1;
            UserInfo::new(
                username.as_ref(),
                new_password.map_or(user_info.password, |v| v.as_ref().to_vec()),
                new_auth.map_or(user_info.auth_type, |v| v),
            )
        } else {
            UserInfo::new(
                username.as_ref(),
                new_password.unwrap().as_ref(),
                new_auth.unwrap(),
            )
        };

        let value = serde_json::to_vec(&user_info)?;
        let key = utils::prepend(&user_info.name);

        let match_seq = match seq {
            None => MatchSeq::GE(1),
            Some(s) => MatchSeq::Exact(s),
        };
        let res = self
            .kv_api
            .upsert_kv(&key, match_seq, Some(value), None)
            .await?;
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
        let r = self.kv_api.upsert_kv(&key, seq.into(), None, None).await?;
        if r.prev.is_some() && r.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownUser(format!(
                "unknown user {}",
                username.as_ref()
            )))
        }
    }
}
