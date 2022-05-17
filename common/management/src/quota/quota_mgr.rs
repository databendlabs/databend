// Copyright 2022 Datafuse Labs.
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

use common_base::base::escape_for_key;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::KVApi;
use common_meta_types::IntoSeqV;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::TenantQuota;
use common_meta_types::UpsertKVAction;

use super::quota_api::QuotaApi;

static QUOTA_API_KEY_PREFIX: &str = "__fd_quotas";

pub struct QuotaMgr {
    kv_api: Arc<dyn KVApi>,
    key: String,
}

impl QuotaMgr {
    pub fn create(kv_api: Arc<dyn KVApi>, tenant: &str) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while quota mgr create)",
            ));
        }
        Ok(QuotaMgr {
            kv_api,
            key: format!("{}/{}", QUOTA_API_KEY_PREFIX, escape_for_key(tenant)?),
        })
    }
}

#[async_trait::async_trait]
impl QuotaApi for QuotaMgr {
    async fn get_quota(&self, seq: Option<u64>) -> Result<SeqV<TenantQuota>> {
        let res = self.kv_api.get_kv(&self.key).await?;
        match res {
            Some(seq_value) => match MatchSeq::from(seq).match_seq(&seq_value) {
                Ok(_) => Ok(seq_value.into_seqv()?),
                Err(_) => Err(ErrorCode::TenantQuotaUnknown("seq not match")),
            },
            None => Ok(SeqV::new(0, TenantQuota::default())),
        }
    }

    async fn set_quota(&self, quota: &TenantQuota, seq: Option<u64>) -> Result<u64> {
        let value = serde_json::to_vec(quota)?;
        let match_seq = match seq {
            None => MatchSeq::Any,
            Some(seq) => MatchSeq::Exact(seq),
        };
        let res = self
            .kv_api
            .upsert_kv(UpsertKVAction::new(
                &self.key,
                match_seq,
                Operation::Update(value),
                None,
            ))
            .await?;

        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::TenantQuotaUnknown(
                "quota not found, or seq not match",
            )),
        }
    }
}
