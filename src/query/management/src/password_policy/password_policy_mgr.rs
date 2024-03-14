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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_app::principal::PasswordPolicy;
use databend_common_meta_app::principal::PasswordPolicyIdent;
use databend_common_meta_app::schema::OnExist;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::NonEmptyString;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::With;
use futures::TryStreamExt;

use crate::password_policy::password_policy_api::PasswordPolicyApi;

pub struct PasswordPolicyMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
}

impl PasswordPolicyMgr {
    pub fn create(
        kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
        tenant: &NonEmptyString,
    ) -> Self {
        PasswordPolicyMgr {
            kv_api,
            tenant: Tenant::new_nonempty(tenant.clone()),
        }
    }

    fn ident(&self, name: &str) -> PasswordPolicyIdent {
        PasswordPolicyIdent::new(self.tenant.clone(), name)
    }
}

#[async_trait::async_trait]
impl PasswordPolicyApi for PasswordPolicyMgr {
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn add(&self, password_policy: PasswordPolicy, create_option: &OnExist) -> Result<()> {
        let ident = self.ident(&password_policy.name);

        let seq = MatchSeq::from(*create_option);
        let upsert = UpsertPB::insert(ident, password_policy.clone()).with(seq);

        let res = self.kv_api.upsert_pb(&upsert).await?;

        if let OnExist::Error = create_option {
            if res.prev.is_some() {
                return Err(ErrorCode::PasswordPolicyAlreadyExists(format!(
                    "Password policy '{}' already exists.",
                    password_policy.name
                )));
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn update(&self, password_policy: PasswordPolicy, match_seq: MatchSeq) -> Result<u64> {
        let ident = self.ident(&password_policy.name);

        let upsert = UpsertPB::update(ident, password_policy.clone()).with(match_seq);
        let res = self.kv_api.upsert_pb(&upsert).await?;

        let Some(SeqV { seq, .. }) = res.result else {
            return Err(ErrorCode::UnknownPasswordPolicy(format!(
                "Password policy '{}' does not exist.",
                password_policy.name
            )));
        };

        Ok(seq)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn remove(&self, name: &str, seq: MatchSeq) -> Result<()> {
        let ident = self.ident(name);

        let upsert = UpsertPB::delete(ident).with(seq);
        let res = self.kv_api.upsert_pb(&upsert).await?;

        res.removed_or_else(|_| {
            ErrorCode::UnknownPasswordPolicy(format!(
                "Cannot delete password policy '{}'. It may not exist.",
                name
            ))
        })?;

        Ok(())
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get(&self, name: &str, seq: MatchSeq) -> Result<SeqV<PasswordPolicy>> {
        let ident = self.ident(name);

        let seqv = self.kv_api.get_pb(&ident).await?;

        match seq.match_seq(&seqv) {
            Ok(_) => seqv.ok_or_else(|| {
                ErrorCode::UnknownPasswordPolicy(format!(
                    "Password policy '{}' does not exist.",
                    name
                ))
            }),
            Err(_) => Err(ErrorCode::UnknownPasswordPolicy(format!(
                "Password policy '{}' does not exist.",
                name
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn list(&self) -> Result<Vec<PasswordPolicy>> {
        let dir_name = DirName::new(self.ident("dummy"));

        let values = self.kv_api.list_pb_values(&dir_name).await?;
        let values = values.try_collect().await?;

        Ok(values)
    }
}
