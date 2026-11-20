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

use databend_common_exception::Result;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::principal::SettingIdent;
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::ClientHandle;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::UpsertKV;
use databend_meta_plugin_cache::Cache;
use databend_meta_runtime::DatabendRuntime;
use futures::TryStreamExt;
use seq_marked::SeqValue;

use crate::errors::meta_service_error;

pub struct SettingMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
    /// The global settings of the tenant, if watched: see `get_watched_settings`.
    watched: Option<Arc<Cache>>,
}

impl SettingMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &Tenant) -> Self {
        SettingMgr {
            kv_api,
            tenant: tenant.clone(),
            watched: None,
        }
    }

    /// Watch the global settings of `tenant`, for `with_watched`.
    pub async fn watch(client: Arc<ClientHandle<DatabendRuntime>>, tenant: &Tenant) -> Arc<Cache> {
        let cache = Cache::new(client, Self::settings_dir(tenant), "global settings").await;
        Arc::new(cache)
    }

    /// Read the settings by `get_watched_settings` from `watched`, a `watch` of the tenant.
    pub fn with_watched(mut self, watched: Arc<Cache>) -> Self {
        self.watched = Some(watched);
        self
    }

    fn setting_ident(&self, name: &str) -> SettingIdent {
        SettingIdent::new(self.tenant.clone(), name)
    }

    fn setting_key(&self, name: &str) -> String {
        self.setting_ident(name).to_string_key()
    }

    fn setting_prefix(&self) -> String {
        SettingIdent::new(self.tenant.clone(), "").to_string_key()
    }

    /// The directory of the global settings of `tenant`, which a watch of them takes.
    fn settings_dir(tenant: &Tenant) -> String {
        SettingIdent::new(tenant.clone(), "").tenant_dir()
    }
}

// TODO: do not use json for setting value
impl SettingMgr {
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn set_setting(&self, setting: UserSetting) -> Result<u64> {
        // Upsert.
        let val = serde_json::to_vec(&setting)?;
        let key = self.setting_key(&setting.name);
        let upsert = self.kv_api.upsert_kv(UpsertKV::update(&key, &val));

        let (_prev, curr) = upsert.await.map_err(meta_service_error)?.unpack();
        let res_seq = curr.seq();
        Ok(res_seq)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn get_settings(&self) -> Result<Vec<UserSetting>> {
        let prefix = self.setting_prefix();
        let mut strm = self
            .kv_api
            .list_kv(ListOptions::unlimited(&prefix))
            .await
            .map_err(meta_service_error)?;

        let mut settings = Vec::new();
        while let Some(item) = strm.try_next().await.map_err(meta_service_error)? {
            if let Some(value) = item.value {
                settings.push(Self::decode_setting(&value.data)?);
            }
        }

        Ok(settings)
    }

    /// The global settings, from the watched copy if there is one and it can answer now, or else
    /// from meta-service, see `Cache::try_list_dir_ready`. Settings from the copy may lag a
    /// change by the watch delay. Use `get_settings` where a change must be seen at once.
    #[async_backtrace::framed]
    pub async fn get_watched_settings(&self) -> Result<Vec<UserSetting>> {
        let dir = Self::settings_dir(&self.tenant);
        let local = self
            .watched
            .as_ref()
            .and_then(|w| w.try_list_dir_ready(&dir));
        let Some(items) = local else {
            return self.get_settings().await;
        };
        items
            .iter()
            .map(|(_, value)| Self::decode_setting(&value.data))
            .collect()
    }

    /// A setting stored under the setting prefix.
    fn decode_setting(data: &[u8]) -> Result<UserSetting> {
        Ok(serde_json::from_slice(data)?)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn get_setting(&self, name: &str) -> Result<Option<SeqV<UserSetting>>> {
        let key = self.setting_key(name);
        let res = self.kv_api.get_kv(&key).await.map_err(meta_service_error)?;

        let Some(seqv) = res else {
            return Ok(None);
        };

        let seqv = seqv.try_map(|d| d.try_into())?;
        Ok(Some(seqv))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn try_drop_setting(&self, name: &str) -> Result<()> {
        let key = self.setting_key(name);
        let _res = self
            .kv_api
            .upsert_kv(UpsertKV::delete(&key))
            .await
            .map_err(meta_service_error)?;

        Ok(())
    }
}
