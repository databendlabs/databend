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
use common_meta_api::KVApi;
use common_meta_types::IntoSeqV;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::OkOrExist;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::UpsertKVAction;
use common_meta_types::UserStageInfo;

use crate::stage::StageApi;

static USER_STAGE_API_KEY_PREFIX: &str = "__fd_stages";

pub struct StageMgr {
    kv_api: Arc<dyn KVApi>,
    stage_prefix: String,
}

impl StageMgr {
    pub fn create(kv_api: Arc<dyn KVApi>, tenant: &str) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while role mgr create)",
            ));
        }

        Ok(StageMgr {
            kv_api,
            stage_prefix: format!("{}/{}", USER_STAGE_API_KEY_PREFIX, tenant),
        })
    }
}

#[async_trait::async_trait]
impl StageApi for StageMgr {
    async fn add_stage(&self, info: UserStageInfo) -> Result<u64> {
        let seq = MatchSeq::Exact(0);
        let val = Operation::Update(serde_json::to_vec(&info)?);
        let key = format!("{}/{}", self.stage_prefix, info.stage_name);
        let upsert_info = self
            .kv_api
            .upsert_kv(UpsertKVAction::new(&key, seq, val, None));

        let res = upsert_info.await?.into_add_result()?;

        match res.res {
            OkOrExist::Ok(v) => Ok(v.seq),
            OkOrExist::Exists(v) => Err(ErrorCode::StageAlreadyExists(format!(
                "Stage already exists, seq [{}]",
                v.seq
            ))),
        }
    }

    async fn get_stage(&self, name: &str, seq: Option<u64>) -> Result<SeqV<UserStageInfo>> {
        let key = format!("{}/{}", self.stage_prefix, name);
        let kv_api = self.kv_api.clone();
        let get_kv = async move { kv_api.get_kv(&key).await };
        let res = get_kv.await?;
        let seq_value =
            res.ok_or_else(|| ErrorCode::UnknownStage(format!("Unknown stage {}", name)))?;

        match MatchSeq::from(seq).match_seq(&seq_value) {
            Ok(_) => Ok(seq_value.into_seqv()?),
            Err(_) => Err(ErrorCode::UnknownStage(format!("Unknown stage {}", name))),
        }
    }

    async fn get_stages(&self) -> Result<Vec<UserStageInfo>> {
        let values = self.kv_api.prefix_list_kv(&self.stage_prefix).await?;

        let mut stage_infos = Vec::with_capacity(values.len());
        for (_, value) in values {
            let stage_info = serde_json::from_slice::<UserStageInfo>(&value.data)?;
            stage_infos.push(stage_info);
        }
        Ok(stage_infos)
    }

    async fn drop_stage(&self, name: &str, seq: Option<u64>) -> Result<()> {
        let key = format!("{}/{}", self.stage_prefix, name);
        let kv_api = self.kv_api.clone();
        let upsert_kv = async move {
            kv_api
                .upsert_kv(UpsertKVAction::new(
                    &key,
                    seq.into(),
                    Operation::Delete,
                    None,
                ))
                .await
        };
        let res = upsert_kv.await?;
        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownStage(format!("Unknown stage {}", name)))
        }
    }
}
