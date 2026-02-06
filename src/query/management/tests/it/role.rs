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

use anyhow::Result;
use databend_common_management::*;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::UpsertKV;
use tokio::sync::Mutex;

fn make_role_key(role: &str) -> String {
    format!("__fd_roles/admin/{}", role)
}

mod add {

    use databend_common_meta_app::principal::RoleInfo;
    use databend_meta_kvapi::kvapi::KVApi;
    use databend_meta_types::MatchSeq;
    use databend_meta_types::Operation;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_json_upgrade_to_pb() -> anyhow::Result<()> {
        let role_name = "role1";
        {
            let (kv_api, role_api) = new_role_api(true).await?;

            let role_key = make_role_key(role_name);
            let role_info = RoleInfo::new(role_name, None);

            let v = serde_json::to_vec(&role_info)?;
            let kv_api = kv_api.clone();
            let _upsert_kv = kv_api
                .upsert_kv(UpsertKV::new(
                    &role_key,
                    MatchSeq::Exact(0),
                    Operation::Update(v),
                    None,
                ))
                .await?;

            let get = role_api.get_role(role_name, MatchSeq::GE(1)).await?.data;
            assert_eq!("role1".to_string(), get.name);
        }

        {
            let (kv_api, role_api) = new_role_api(false).await?;

            let role_key = make_role_key(role_name);
            let role_info = RoleInfo::new(role_name, None);

            let v = serde_json::to_vec(&role_info)?;
            let kv_api = kv_api.clone();
            let _upsert_kv = kv_api
                .upsert_kv(UpsertKV::new(
                    &role_key,
                    MatchSeq::Exact(0),
                    Operation::Update(v),
                    None,
                ))
                .await?;
            let get = role_api.get_role(role_name, MatchSeq::GE(1)).await?.data;
            assert_eq!("role1".to_string(), get.name);
        }

        Ok(())
    }
}

async fn new_role_api(
    enable_meta_data_upgrade_json_to_pb_from_v307: bool,
) -> Result<(Arc<MetaStore>, RoleMgr)> {
    let test_api = MetaStore::new_local_testing::<DatabendRuntime>().await;
    let client = test_api.inner().clone();

    let test_api = Arc::new(test_api);

    let cache = RoleMgr::new_cache(client.clone()).await;

    let tenant = Tenant::new_literal("admin");
    let mgr = RoleMgr::create(
        test_api.clone(),
        &tenant,
        enable_meta_data_upgrade_json_to_pb_from_v307,
        Some(Arc::new(Mutex::new(cache))),
    );
    Ok((test_api, mgr))
}
