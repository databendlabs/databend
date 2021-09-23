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
use std::sync::Arc;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_metatypes::MatchSeq;
use common_metatypes::MatchSeqExt;
use common_metatypes::SeqValue;
use common_store_api::KVApi;
use common_store_api::SyncKVApi;

use super::user_api::AuthType;
use crate::user::user_api::UserInfo;
use crate::user::user_api::UserMgrApi;

pub static USER_API_KEY_PREFIX: &str = "__fd_users";

pub struct UserMgr {
    kv_api: Arc<dyn KVApi>,
    user_prefix: String,
}

impl UserMgr {
    pub fn new(kv_api: Arc<dyn KVApi>, tenant: &str) -> Self {
        UserMgr {
            kv_api,
            user_prefix: format!("{}/{}", USER_API_KEY_PREFIX, tenant),
        }
    }
}

#[async_trait]
impl UserMgrApi for UserMgr {
    async fn add_user(&self, user_info: UserInfo) -> common_exception::Result<u64> {
        let match_seq = MatchSeq::Exact(0);
        let key = format!("{}/{}", self.user_prefix, user_info.name);
        let value = serde_json::to_vec(&user_info)?;

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

    async fn get_user(&self, username: String, seq: Option<u64>) -> Result<SeqValue<UserInfo>> {
        let key = format!("{}/{}", self.user_prefix, username);
        let res = self.kv_api.get_kv(&key).await?;

        let seq_value = res
            .result
            .ok_or_else(|| ErrorCode::UnknownUser(format!("unknown user {}", username)))?;

        match MatchSeq::from(seq).match_seq(&seq_value) {
            Ok(_) => Ok((seq_value.0, seq_value.1.value.try_into()?)),
            Err(_) => Err(ErrorCode::UnknownUser(format!("username: {}", username))),
        }
    }

    async fn get_users(&self) -> Result<Vec<SeqValue<UserInfo>>> {
        let values = self
            .kv_api
            .prefix_list_kv(self.user_prefix.as_str())
            .await?;
        let mut r = vec![];
        for (_key, (s, val)) in values {
            let u = serde_json::from_slice::<UserInfo>(&val.value)
                .map_err_to_code(ErrorCode::IllegalUserInfoFormat, || "")?;

            r.push((s, u));
        }

        self.kv_api.sync_update_kv_meta("k", MatchSeq::Any, None);
        Ok(r)
    }

    async fn update_user(
        &self,
        username: String,
        new_password: Option<Vec<u8>>,
        new_auth: Option<AuthType>,
        seq: Option<u64>,
    ) -> Result<Option<u64>> {
        if new_password.is_none() && new_auth.is_none() {
            return Ok(seq);
        }
        let partial_update = new_auth.is_none() || new_password.is_none();
        let user_info = if partial_update {
            let user_val_seq = self.get_user(username.clone(), seq).await?;
            let user_info = user_val_seq.1;
            UserInfo::new(
                username.clone(),
                new_password.map_or(user_info.password, |v| v.to_vec()),
                new_auth.map_or(user_info.auth_type, |v| v),
            )
        } else {
            UserInfo::new(username.clone(), new_password.unwrap(), new_auth.unwrap())
        };

        let key = format!("{}/{}", self.user_prefix, user_info.name);
        let value = serde_json::to_vec(&user_info)?;

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
                username
            ))),
        }
    }

    async fn drop_user(&self, username: String, seq: Option<u64>) -> Result<()> {
        let key = format!("{}/{}", self.user_prefix, username);
        let res = self.kv_api.upsert_kv(&key, seq.into(), None, None).await?;
        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownUser(format!("unknown user {}", username)))
        }
    }
}
