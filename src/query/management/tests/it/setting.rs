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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_management::*;
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_embedded::MetaEmbedded;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqV;
use minitrace::func_name;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_set_setting() -> Result<()> {
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
        let actual = mgr.get_setting("max_threads", MatchSeq::GE(0)).await?;
        assert_eq!(actual.data, expect);
    }

    // Drop setting.
    {
        mgr.try_drop_setting("max_threads", MatchSeq::GE(1)).await?;
    }

    // Get settings.
    {
        let actual = mgr.get_settings().await?;
        assert_eq!(0, actual.len());
    }

    // Get setting.
    {
        let res = mgr.get_setting("max_threads", MatchSeq::GE(0)).await;
        assert!(res.is_err());
    }

    // Drop setting not exists.
    {
        let res = mgr
            .try_drop_setting("max_threads_not", MatchSeq::GE(1))
            .await;
        assert!(res.is_ok());
    }

    Ok(())
}

async fn new_setting_api() -> Result<(Arc<MetaEmbedded>, SettingMgr)> {
    let test_api = Arc::new(MetaEmbedded::new_temp().await?);
    let mgr = SettingMgr::create(
        test_api.clone(),
        &Tenant::new_or_err("databend_query", func_name!()).unwrap(),
    );
    Ok((test_api, mgr))
}
