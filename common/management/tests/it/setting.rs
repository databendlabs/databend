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

use common_base::tokio;
use common_datavalues::DataValue;
use common_exception::Result;
use common_management::*;
use common_meta_api::KVApi;
use common_meta_embedded::MetaEmbedded;
use common_meta_types::SeqV;
use common_meta_types::UserSetting;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_set_setting() -> Result<()> {
    let (kv_api, mgr) = new_setting_api().await?;

    {
        let setting = UserSetting::create("max_threads", DataValue::UInt64(3));
        mgr.set_setting(setting.clone()).await?;
        let value = kv_api.get_kv("__fd_settings/admin/max_threads").await?;

        match value {
            Some(SeqV {
                seq: 1,
                meta: _,
                data: value,
            }) => {
                assert_eq!(value, serde_json::to_vec(&setting)?);
            }
            catch => panic!("GetKVActionReply{:?}", catch),
        }
    }

    // Set again.
    {
        let setting = UserSetting::create("max_threads", DataValue::UInt64(1));
        mgr.set_setting(setting.clone()).await?;
        let value = kv_api.get_kv("__fd_settings/admin/max_threads").await?;

        match value {
            Some(SeqV {
                seq: 2,
                meta: _,
                data: value,
            }) => {
                assert_eq!(value, serde_json::to_vec(&setting)?);
            }
            catch => panic!("GetKVActionReply{:?}", catch),
        }
    }

    // Get settings.
    {
        let expect = vec![UserSetting::create("max_threads", DataValue::UInt64(1))];
        let actual = mgr.get_settings().await?;
        assert_eq!(actual, expect);
    }

    // Get setting.
    {
        let expect = UserSetting::create("max_threads", DataValue::UInt64(1));
        let actual = mgr.get_setting("max_threads", None).await?;
        assert_eq!(actual.data, expect);
    }

    // Drop setting.
    {
        mgr.drop_setting("max_threads", None).await?;
    }

    // Get settings.
    {
        let actual = mgr.get_settings().await?;
        assert_eq!(0, actual.len());
    }

    // Get setting.
    {
        let res = mgr.get_setting("max_threads", None).await;
        assert!(res.is_err());
    }

    // Drop setting not exists.
    {
        let res = mgr.drop_setting("max_threads_not", None).await;
        assert!(res.is_err());
    }

    Ok(())
}

async fn new_setting_api() -> Result<(Arc<MetaEmbedded>, SettingMgr)> {
    let test_api = Arc::new(MetaEmbedded::new_temp().await?);
    let mgr = SettingMgr::create(test_api.clone(), "admin")?;
    Ok((test_api, mgr))
}
