// Copyright 2021 Datafuse Labs
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

use databend_common_base::base::escape_for_key;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::PasswordPolicy;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;

use crate::password_policy::password_policy_api::PasswordPolicyApi;
use crate::serde::deserialize_struct;
use crate::serde::serialize_struct;

static PASSWORD_POLICY_API_KEY_PREFIX: &str = "__fd_password_policies";

pub struct PasswordPolicyMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    password_policy_prefix: String,
}

impl PasswordPolicyMgr {
    pub fn create(
        kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
        tenant: &str,
    ) -> Result<Self, ErrorCode> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty (while create password policy)",
            ));
        }

        Ok(PasswordPolicyMgr {
            kv_api,
            password_policy_prefix: format!("{}/{}", PASSWORD_POLICY_API_KEY_PREFIX, tenant),
        })
    }

    fn make_password_policy_key(&self, name: &str) -> Result<String> {
        Ok(format!(
            "{}/{}",
            self.password_policy_prefix,
            escape_for_key(name)?
        ))
    }
}

#[async_trait::async_trait]
impl PasswordPolicyApi for PasswordPolicyMgr {
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn add_password_policy(&self, password_policy: PasswordPolicy) -> Result<u64> {
        let match_seq = MatchSeq::Exact(0);
        let key = self.make_password_policy_key(password_policy.name.as_str())?;
        let value = Operation::Update(serialize_struct(
            &password_policy,
            ErrorCode::IllegalPasswordPolicy,
            || "",
        )?);

        let kv_api = self.kv_api.clone();
        let upsert_kv = kv_api.upsert_kv(UpsertKVReq::new(&key, match_seq, value, None));

        let res_seq = upsert_kv.await?.added_seq_or_else(|_v| {
            ErrorCode::PasswordPolicyAlreadyExists(format!(
                "Password policy '{}' already exists.",
                password_policy.name
            ))
        })?;

        Ok(res_seq)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn update_password_policy(
        &self,
        password_policy: PasswordPolicy,
        match_seq: MatchSeq,
    ) -> Result<u64> {
        let key = self.make_password_policy_key(password_policy.name.as_str())?;
        let value = Operation::Update(serialize_struct(
            &password_policy,
            ErrorCode::IllegalPasswordPolicy,
            || "",
        )?);

        let kv_api = self.kv_api.clone();
        let upsert_kv = kv_api
            .upsert_kv(UpsertKVReq::new(&key, match_seq, value, None))
            .await?;

        match upsert_kv.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownPasswordPolicy(format!(
                "Password policy '{}' not found.",
                password_policy.name
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn drop_password_policy(&self, name: &str, seq: MatchSeq) -> Result<()> {
        let key = self.make_password_policy_key(name)?;
        let kv_api = self.kv_api.clone();
        let res = kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Delete, None))
            .await?;
        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownPasswordPolicy(format!(
                "Cannot delete password policy '{}'. It may not exist.",
                name
            )))
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_password_policy(&self, name: &str, seq: MatchSeq) -> Result<SeqV<PasswordPolicy>> {
        let key = self.make_password_policy_key(name)?;
        let res = self.kv_api.get_kv(&key).await?;
        let seq_value = res.ok_or_else(|| {
            ErrorCode::UnknownPasswordPolicy(format!("Password policy '{}' not found.", name))
        })?;

        match seq.match_seq(&seq_value) {
            Ok(_) => Ok(SeqV::new(
                seq_value.seq,
                deserialize_struct(&seq_value.data, ErrorCode::IllegalPasswordPolicy, || "")?,
            )),
            Err(_) => Err(ErrorCode::UnknownPasswordPolicy(format!(
                "Password policy '{}' not found.",
                name
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_password_policies(&self) -> Result<Vec<PasswordPolicy>> {
        let values = self
            .kv_api
            .prefix_list_kv(&self.password_policy_prefix)
            .await?;

        let mut password_policies = Vec::with_capacity(values.len());
        for (_, value) in values {
            let password_policy =
                deserialize_struct(&value.data, ErrorCode::IllegalPasswordPolicy, || "")?;
            password_policies.push(password_policy);
        }
        Ok(password_policies)
    }
}
