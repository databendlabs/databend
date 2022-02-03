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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::KVApi;
use common_meta_types::MatchSeq;
use common_meta_types::OkOrExist;
use common_meta_types::Operation;
use common_meta_types::UpsertKVAction;
use common_meta_types::UserSetting;

use crate::setting::SettingApi;

static USER_SETTING_API_KEY_PREFIX: &str = "__fd_settings";

pub struct SettingMgr {
    kv_api: Arc<dyn KVApi>,
    setting_prefix: String,
}

impl SettingMgr {
    #[allow(dead_code)]
    pub fn new(kv_api: Arc<dyn KVApi>, tenant: &str) -> Self {
        SettingMgr {
            kv_api,
            setting_prefix: format!("{}/{}", USER_SETTING_API_KEY_PREFIX, tenant),
        }
    }
}

#[async_trait::async_trait]
impl SettingApi for SettingMgr {
    async fn set_setting(&self, setting: UserSetting) -> Result<u64> {
        // Upsert.
        let seq = MatchSeq::Any;
        let val = Operation::Update(serde_json::to_vec(&setting)?);
        let key = format!("{}/{}", self.setting_prefix, setting.name);
        let upsert = self
            .kv_api
            .upsert_kv(UpsertKVAction::new(&key, seq, val, None));

        let res = upsert.await?.into_add_result()?;

        match res.res {
            OkOrExist::Ok(v) => Ok(v.seq),
            OkOrExist::Exists(v) => Ok(v.seq),
        }
    }

    async fn get_settings(&self) -> Result<Vec<UserSetting>> {
        let values = self.kv_api.prefix_list_kv(&self.setting_prefix).await?;

        let mut settings = Vec::with_capacity(values.len());
        for (_, value) in values {
            let setting = serde_json::from_slice::<UserSetting>(&value.data)?;
            settings.push(setting);
        }
        Ok(settings)
    }

    async fn drop_setting(&self, name: &str, seq: Option<u64>) -> Result<()> {
        let key = format!("{}/{}", self.setting_prefix, name);
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
            Err(ErrorCode::UnknownVariable(format!(
                "Unknown setting {}",
                name
            )))
        }
    }
}
