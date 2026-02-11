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
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_meta_app::tenant::TenantQuotaIdent;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_types::MatchSeq;
use databend_meta_types::MatchSeqExt;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::UpsertKV;
use databend_meta_types::With;
use fastrace::func_name;

use super::quota_api::QuotaApi;
use crate::errors::meta_service_error;
use crate::serde::Quota;
use crate::serde::check_and_upgrade_to_pb;

pub struct QuotaMgr<const WRITE_PB: bool = false> {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    ident: TenantQuotaIdent,
}

impl<const WRITE_PB: bool> QuotaMgr<WRITE_PB> {
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
impl<const WRITE_PB: bool> QuotaApi for QuotaMgr<WRITE_PB> {
    #[async_backtrace::framed]
    async fn get_quota(&self, seq: MatchSeq) -> Result<SeqV<TenantQuota>> {
        let res = self
            .kv_api
            .get_kv(&self.key())
            .await
            .map_err(meta_service_error)?;
        match res {
            None => Ok(SeqV::new(0, TenantQuota::default())),
            Some(seq_value) => match seq.match_seq(&seq_value) {
                Err(_) => Err(ErrorCode::TenantQuotaUnknown("Tenant does not exist.")),
                Ok(_) => {
                    let mut quota = if WRITE_PB {
                        Quota::new(func_name!())
                    } else {
                        // Do not serialize to protobuf format
                        Quota::new_limit(func_name!(), 0)
                    };

                    // Now WRITE_PB control quota upgrade json to pb.
                    // And in set_quota default is write with json.
                    // So we directly use false, until in set_quota write date with PB
                    let u = check_and_upgrade_to_pb(
                        &mut quota,
                        &self.key(),
                        &seq_value,
                        self.kv_api.as_ref(),
                    )
                    .await
                    .map_err(meta_service_error)?;

                    // Keep the original seq.
                    Ok(SeqV::new_with_meta(seq_value.seq, seq_value.meta, u.data))
                }
            },
        }
    }

    #[async_backtrace::framed]
    async fn set_quota(&self, quota: &TenantQuota, seq: MatchSeq) -> Result<u64> {
        if WRITE_PB {
            let res = self
                .kv_api
                .upsert_pb(&UpsertPB::update(self.ident.clone(), quota.clone()).with(seq))
                .await
                .map_err(meta_service_error)?;
            match res.result {
                Some(SeqV { seq: s, .. }) => Ok(s),
                None => Err(ErrorCode::TenantQuotaUnknown("Quota does not exist.")),
            }
        } else {
            let value = serde_json::to_vec(quota)?;
            let res = self
                .kv_api
                .upsert_kv(UpsertKV::update(self.key(), &value).with(seq))
                .await
                .map_err(meta_service_error)?;
            match res.result {
                Some(SeqV { seq: s, .. }) => Ok(s),
                None => Err(ErrorCode::TenantQuotaUnknown("Quota does not exist.")),
            }
        }
    }
}
