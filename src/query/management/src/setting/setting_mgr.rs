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
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::IntoSeqV;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::NonEmptyString;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;

use crate::setting::SettingApi;

static USER_SETTING_API_KEY_PREFIX: &str = "__fd_settings";

pub struct SettingMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    setting_prefix: String,
}

impl SettingMgr {
    pub fn create(
        kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
        tenant: &NonEmptyString,
    ) -> Self {
        SettingMgr {
            kv_api,
            setting_prefix: format!("{}/{}", USER_SETTING_API_KEY_PREFIX, tenant.as_str()),
        }
    }
}

#[async_trait::async_trait]
impl SettingApi for SettingMgr {
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn set_setting(&self, setting: UserSetting) -> Result<u64> {
        // Upsert.
        let seq = MatchSeq::GE(0);
        let val = Operation::Update(serde_json::to_vec(&setting)?);
        let key = format!("{}/{}", self.setting_prefix, setting.name);
        let upsert = self
            .kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, val, None));

        let (_prev, curr) = upsert.await?.unpack();
        let res_seq = curr.seq();
        Ok(res_seq)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_settings(&self) -> Result<Vec<UserSetting>> {
        let values = self.kv_api.prefix_list_kv(&self.setting_prefix).await?;

        let mut settings = Vec::with_capacity(values.len());
        for (_, value) in values {
            let setting = serde_json::from_slice::<UserSetting>(&value.data)?;
            settings.push(setting);
        }
        Ok(settings)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_setting(&self, name: &str, seq: MatchSeq) -> Result<SeqV<UserSetting>> {
        let key = format!("{}/{}", self.setting_prefix, name);
        let kv_api = self.kv_api.clone();
        let get_kv = async move { kv_api.get_kv(&key).await };
        let res = get_kv.await?;
        let seq_value = res.ok_or_else(|| {
            ErrorCode::UnknownVariable(format!("Setting '{}' does not exist.", name))
        })?;

        match seq.match_seq(&seq_value) {
            Ok(_) => Ok(seq_value.into_seqv()?),
            Err(_) => Err(ErrorCode::UnknownVariable(format!(
                "Setting '{}' does not exist.",
                name
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn try_drop_setting(&self, name: &str, seq: MatchSeq) -> Result<()> {
        let key = format!("{}/{}", self.setting_prefix, name);
        let kv_api = self.kv_api.clone();
        let upsert_kv = async move {
            kv_api
                .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Delete, None))
                .await
        };
        upsert_kv.await?;
        Ok(())
    }
}
