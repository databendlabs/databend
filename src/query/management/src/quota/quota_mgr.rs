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
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_meta_app::tenant::TenantQuotaIdent;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_types::IntoSeqV;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::With;
use databend_common_proto_conv::FromToProto;
use prost;

use super::quota_api::QuotaApi;

pub struct QuotaMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    ident: TenantQuotaIdent,
}

impl QuotaMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &Tenant) -> Self {
        QuotaMgr {
            kv_api,
            ident: TenantQuotaIdent::new(tenant.clone()),
        }
    }

    fn key(&self) -> String {
        self.ident.to_string_key()
    }
}

#[async_trait::async_trait]
impl QuotaApi for QuotaMgr {
    #[async_backtrace::framed]
    async fn get_quota(&self, seq: MatchSeq) -> Result<SeqV<TenantQuota>> {
        let res = self.kv_api.get_kv(&self.key()).await?;
        match res {
            Some(seq_value) => match seq.match_seq(&seq_value) {
                Ok(_) => {
                    match prost::Message::decode(seq_value.data.as_slice()) {
                        Ok(pb) => {
                            let v = FromToProto::from_pb(pb)
                                .map_err(|e| ErrorCode::from_string(e.reason))?;
                            Ok(SeqV::with_meta(seq_value.seq, seq_value.meta, v))
                        }
                        Err(_) => Ok(seq_value.into_seqv()?),
                    }
                }
                Err(_) => Err(ErrorCode::TenantQuotaUnknown("Tenant does not exist.")),
            },
            None => Ok(SeqV::new(0, TenantQuota::default())),
        }
    }

    // TODO: use pb to replace json
    #[async_backtrace::framed]
    async fn set_quota(&self, quota: &TenantQuota, seq: MatchSeq) -> Result<u64> {
        let value = serde_json::to_vec(quota)?;
        let res = self
            .kv_api
            .upsert_kv(UpsertKV::update(&self.key(), &value).with(seq))
            .await?;

        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::TenantQuotaUnknown("Quota does not exist.")),
        }
    }
}
