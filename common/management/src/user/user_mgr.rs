// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_meta_api::KVApi;
use common_meta_types::AuthInfo;
use common_meta_types::GrantObject;
use common_meta_types::IntoSeqV;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::OkOrExist;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::UpsertKVAction;
use common_meta_types::UserInfo;
use common_meta_types::UserPrivilegeSet;

use crate::user::user_api::UserApi;

static USER_API_KEY_PREFIX: &str = "__fd_users";

pub struct UserMgr {
    kv_api: Arc<dyn KVApi>,
    user_prefix: String,
}

impl UserMgr {
    pub fn create(kv_api: Arc<dyn KVApi>, tenant: &str) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while user mgr create)",
            ));
        }

        Ok(UserMgr {
            kv_api,
            user_prefix: format!("{}/{}", USER_API_KEY_PREFIX, tenant),
        })
    }

    async fn upsert_user_info(
        &self,
        user_info: &UserInfo,
        seq: Option<u64>,
    ) -> common_exception::Result<u64> {
        let user_key = format_user_key(&user_info.name, &user_info.hostname);
        let key = format!("{}/{}", self.user_prefix, user_key);
        let value = serde_json::to_vec(&user_info)?;

        let match_seq = match seq {
            None => MatchSeq::GE(1),
            Some(s) => MatchSeq::Exact(s),
        };

        let kv_api = self.kv_api.clone();
        let res = kv_api
            .upsert_kv(UpsertKVAction::new(
                &key,
                match_seq,
                Operation::Update(value),
                None,
            ))
            .await?;
        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownUser(format!(
                "unknown user, or seq not match {}",
                user_info.name
            ))),
        }
    }
}

#[async_trait::async_trait]
impl UserApi for UserMgr {
    async fn add_user(&self, user_info: UserInfo) -> common_exception::Result<u64> {
        let match_seq = MatchSeq::Exact(0);
        let user_key = format_user_key(&user_info.name, &user_info.hostname);
        let key = format!("{}/{}", self.user_prefix, user_key);
        let value = serde_json::to_vec(&user_info)?;

        let kv_api = self.kv_api.clone();
        let upsert_kv = kv_api.upsert_kv(UpsertKVAction::new(
            &key,
            match_seq,
            Operation::Update(value),
            None,
        ));
        let res = upsert_kv.await?.into_add_result()?;
        match res.res {
            OkOrExist::Ok(v) => Ok(v.seq),
            OkOrExist::Exists(v) => Err(ErrorCode::UserAlreadyExists(format!(
                "User already exists, seq [{}]",
                v.seq
            ))),
        }
    }

    async fn get_user(
        &self,
        username: String,
        hostname: String,
        seq: Option<u64>,
    ) -> Result<SeqV<UserInfo>> {
        let user_key = format_user_key(&username, &hostname);
        let key = format!("{}/{}", self.user_prefix, user_key);
        let res = self.kv_api.get_kv(&key).await?;
        let seq_value =
            res.ok_or_else(|| ErrorCode::UnknownUser(format!("unknown user {}", user_key)))?;

        match MatchSeq::from(seq).match_seq(&seq_value) {
            Ok(_) => Ok(seq_value.into_seqv()?),
            Err(_) => Err(ErrorCode::UnknownUser(format!("unknown user {}", user_key))),
        }
    }

    async fn get_users(&self) -> Result<Vec<SeqV<UserInfo>>> {
        let user_prefix = self.user_prefix.clone();
        let values = self.kv_api.prefix_list_kv(user_prefix.as_str()).await?;

        let mut r = vec![];
        for (_key, val) in values {
            let u = serde_json::from_slice::<UserInfo>(&val.data)
                .map_err_to_code(ErrorCode::IllegalUserInfoFormat, || "")?;

            r.push(SeqV::new(val.seq, u));
        }

        Ok(r)
    }

    async fn update_user(
        &self,
        username: String,
        hostname: String,
        new_auth_info: AuthInfo,
        seq: Option<u64>,
    ) -> Result<Option<u64>> {
        let user_val_seq = self.get_user(username.clone(), hostname.clone(), seq);
        let user_info = user_val_seq.await?.data;

        let mut new_user_info = UserInfo::new(username.clone(), hostname.clone(), new_auth_info);
        new_user_info.grants = user_info.grants;

        let user_key = format_user_key(&new_user_info.name, &new_user_info.hostname);
        let key = format!("{}/{}", self.user_prefix, user_key);
        let value = serde_json::to_vec(&new_user_info)?;

        let match_seq = match seq {
            None => MatchSeq::GE(1),
            Some(s) => MatchSeq::Exact(s),
        };

        let res = self
            .kv_api
            .upsert_kv(UpsertKVAction::new(
                &key,
                match_seq,
                Operation::Update(value),
                None,
            ))
            .await?;
        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(Some(s)),
            None => Err(ErrorCode::UnknownUser(format!(
                "unknown user, or seq not match {}",
                username
            ))),
        }
    }

    async fn grant_user_privileges(
        &self,
        username: String,
        hostname: String,
        object: GrantObject,
        privileges: UserPrivilegeSet,
        seq: Option<u64>,
    ) -> Result<Option<u64>> {
        let user_val_seq = self.get_user(username.clone(), hostname.clone(), seq);
        let mut user_info = user_val_seq.await?.data;
        user_info
            .grants
            .grant_privileges(&username, &hostname, &object, privileges);
        let seq = self.upsert_user_info(&user_info, seq).await?;
        Ok(Some(seq))
    }

    async fn revoke_user_privileges(
        &self,
        username: String,
        hostname: String,
        object: GrantObject,
        privileges: UserPrivilegeSet,
        seq: Option<u64>,
    ) -> Result<Option<u64>> {
        let user_val_seq = self.get_user(username.clone(), hostname.clone(), seq);
        let mut user_info = user_val_seq.await?.data;
        user_info.grants.revoke_privileges(&object, privileges);
        let seq = self.upsert_user_info(&user_info, seq).await?;
        Ok(Some(seq))
    }

    async fn drop_user(&self, username: String, hostname: String, seq: Option<u64>) -> Result<()> {
        let user_key = format_user_key(&username, &hostname);
        let key = format!("{}/{}", self.user_prefix, user_key);
        let res = self
            .kv_api
            .upsert_kv(UpsertKVAction::new(
                &key,
                seq.into(),
                Operation::Delete,
                None,
            ))
            .await?;
        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownUser(format!("unknown user {}", user_key)))
        }
    }
}

fn format_user_key(username: &str, hostname: &str) -> String {
    format!("'{}'@'{}'", username, hostname)
}
