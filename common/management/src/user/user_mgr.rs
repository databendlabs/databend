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

use std::sync::Arc;
use std::time::Duration;

use common_base::BlockingWait;
use common_base::Runtime;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_meta_api::KVApi;
use common_meta_types::IntoSeqV;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::SeqV;
use common_meta_types::UpsertKVActionReply;

use super::user_api::AuthType;
use crate::user::user_api::UserInfo;
use crate::user::user_api::UserMgrApi;

pub static USER_API_KEY_PREFIX: &str = "__fd_users";

pub struct UserMgr {
    kv_api: Arc<dyn KVApi>,
    user_prefix: String,

    rt: Arc<Runtime>,
    rpc_time_out: Option<Duration>,
}

impl UserMgr {
    pub fn new(kv_api: Arc<dyn KVApi>, tenant: &str) -> Self {
        let rt = Runtime::with_worker_threads(1).expect("UserMgr initialization failure");

        UserMgr {
            kv_api,
            user_prefix: format!("{}/{}", USER_API_KEY_PREFIX, tenant),
            rt: Arc::new(rt),
            // TODO(bh): add config.
            rpc_time_out: Some(Duration::from_secs(5)),
        }
    }
}

impl UserMgrApi for UserMgr {
    fn add_user(&self, user_info: UserInfo) -> common_exception::Result<u64> {
        let match_seq = MatchSeq::Exact(0);
        let key = format!("{}/{}", self.user_prefix, user_info.name);
        let value = serde_json::to_vec(&user_info)?;

        let kv_api = self.kv_api.clone();
        let upsert_kv = async move { kv_api.upsert_kv(&key, match_seq, Some(value), None).await };
        let res = upsert_kv.wait_in(&self.rt, self.rpc_time_out)??;
        match res {
            UpsertKVActionReply {
                prev: None,
                result: Some(SeqV { seq: s, .. }),
            } => Ok(s),
            UpsertKVActionReply {
                prev: Some(SeqV { seq: s, .. }),
                result: _,
            } => Err(ErrorCode::UserAlreadyExists(format!(
                "User already exists, seq [{}]",
                s
            ))),
            catch_result @ UpsertKVActionReply { .. } => Err(ErrorCode::UnknownException(format!(
                "upsert result not expected (using version 0, got {:?})",
                catch_result
            ))),
        }
    }

    fn get_user(&self, username: String, seq: Option<u64>) -> Result<SeqV<UserInfo>> {
        let key = format!("{}/{}", self.user_prefix, username);
        let kv_api = self.kv_api.clone();
        let get_kv = async move { kv_api.get_kv(&key).await };
        let res = get_kv.wait_in(&self.rt, self.rpc_time_out)??;
        let seq_value = res
            .result
            .ok_or_else(|| ErrorCode::UnknownUser(format!("unknown user {}", username)))?;

        match MatchSeq::from(seq).match_seq(&seq_value) {
            // Ok(_) => Ok(SeqV::new(seq_value.seq, seq_value.data.try_into()?)),
            Ok(_) => Ok(seq_value.into_seqv()?),
            Err(_) => Err(ErrorCode::UnknownUser(format!("username: {}", username))),
        }
    }

    fn get_users(&self) -> Result<Vec<SeqV<UserInfo>>> {
        let user_prefix = self.user_prefix.clone();
        let kv_api = self.kv_api.clone();
        let prefix_list_kv = async move { kv_api.prefix_list_kv(user_prefix.as_str()).await };
        let values = prefix_list_kv.wait_in(&self.rt, self.rpc_time_out)??;

        let mut r = vec![];
        for (_key, val) in values {
            let u = serde_json::from_slice::<UserInfo>(&val.data)
                .map_err_to_code(ErrorCode::IllegalUserInfoFormat, || "")?;

            r.push(SeqV::new(val.seq, u));
        }

        Ok(r)
    }

    fn update_user(
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
            let user_val_seq = self.get_user(username.clone(), seq)?;
            let user_info = user_val_seq.data;
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

        let kv_api = self.kv_api.clone();
        let upsert_kv = async move { kv_api.upsert_kv(&key, match_seq, Some(value), None).await };
        let res = upsert_kv.wait_in(&self.rt, self.rpc_time_out)??;
        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(Some(s)),
            None => Err(ErrorCode::UnknownUser(format!(
                "unknown user, or seq not match {}",
                username
            ))),
        }
    }

    fn drop_user(&self, username: String, seq: Option<u64>) -> Result<()> {
        let key = format!("{}/{}", self.user_prefix, username);
        let kv_api = self.kv_api.clone();
        let upsert_kv = async move { kv_api.upsert_kv(&key, seq.into(), None, None).await };
        let res = upsert_kv.wait_in(&self.rt, self.rpc_time_out)??;
        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownUser(format!("unknown user {}", username)))
        }
    }
}
