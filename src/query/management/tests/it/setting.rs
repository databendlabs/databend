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

use anyhow::Result;
use databend_common_management::*;
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::SeqV;
use fastrace::func_name;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_set_setting() -> anyhow::Result<()> {
    let (kv_api, mgr) = new_setting_api().await?;

    {
        let setting = UserSetting::create("max_threads", UserSettingValue::UInt64(3));
        mgr.set_setting(setting.clone()).await?;
        let value = kv_api
            .get_kv("__fd_settings/databend_query/max_threads")
            .await?;

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
        let setting = UserSetting::create("max_threads", UserSettingValue::UInt64(1));
        mgr.set_setting(setting.clone()).await?;
        let value = kv_api
            .get_kv("__fd_settings/databend_query/max_threads")
            .await?;

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
        let expect = vec![UserSetting::create(
            "max_threads",
            UserSettingValue::UInt64(1),
        )];
        let actual = mgr.get_settings().await?;
        assert_eq!(actual, expect);
    }

    // Get setting.
    {
        let expect = UserSetting::create("max_threads", UserSettingValue::UInt64(1));
        let actual = mgr.get_setting("max_threads").await?;
        assert_eq!(actual.unwrap().data, expect);
    }

    // Drop setting.
    {
        mgr.try_drop_setting("max_threads").await?;
    }

    // Get settings.
    {
        let actual = mgr.get_settings().await?;
        assert_eq!(0, actual.len());
    }

    // Get setting.
    {
        let res = mgr.get_setting("max_threads").await?;
        assert!(res.is_none());
    }

    // Drop setting not exists.
    {
        let res = mgr.try_drop_setting("max_threads_not").await;
        assert!(res.is_ok());
    }

    Ok(())
}

async fn new_setting_api() -> Result<(Arc<MetaStore>, SettingMgr)> {
    let test_api = MetaStore::new_local_testing::<DatabendRuntime>().await;
    let test_api = Arc::new(test_api);

    let mgr = SettingMgr::create(
        test_api.clone(),
        &Tenant::new_or_err("databend_query", func_name!()).unwrap(),
    );
    Ok((test_api, mgr))
}
