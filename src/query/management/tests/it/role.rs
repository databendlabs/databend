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
use databend_meta_client::types::UpsertKV;
use databend_meta_runtime::DatabendRuntime;
use tokio::sync::Mutex;

fn make_role_key(role: &str) -> String {
    format!("__fd_roles/admin/{}", role)
}

mod add {

    use databend_common_meta_app::principal::RoleInfo;
    use databend_meta_client::kvapi::KVApi;
    use databend_meta_client::types::MatchSeq;
    use databend_meta_client::types::Operation;

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

            let get = role_api.get_role(role_name).await?.unwrap().data;
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
            let get = role_api.get_role(role_name).await?.unwrap().data;
            assert_eq!("role1".to_string(), get.name);
        }

        Ok(())
    }
}

mod ownership_cache {
    use std::time::Duration;

    use databend_common_meta_app::principal::BUILTIN_ROLE_ACCOUNT_ADMIN;
    use databend_common_meta_app::principal::OwnershipObject;
    use databend_common_meta_app::principal::RoleInfo;

    use super::*;

    fn udf(name: &str) -> OwnershipObject {
        OwnershipObject::UDF {
            name: name.to_string(),
        }
    }

    /// The cache holds only its own tenant's records; tenant `t10` shares the string prefix `t1`.
    /// DROP ROLE's ownership transfer reads meta-service, so it covers a grant made just before it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_ownership_cache_is_tenant_scoped() -> anyhow::Result<()> {
        let meta = Arc::new(MetaStore::new_local_testing::<DatabendRuntime>().await);
        let client = meta.inner().clone();

        let t1 = Tenant::new_literal("t1");
        let t10 = Tenant::new_literal("t10");
        let cache = Arc::new(Mutex::new(RoleMgr::new_cache(client, &t1).await));
        let t1_api = RoleMgr::create(meta.clone(), &t1, false, Some(cache.clone()));
        let t10_api = RoleMgr::create(meta.clone(), &t10, false, None);

        for api in [&t1_api, &t10_api] {
            api.create_role(RoleInfo::new("r1", None), false).await?;
        }
        t1_api.grant_ownership(&udf("f1"), "r1").await?;
        t10_api.grant_ownership(&udf("f10"), "r1").await?;

        // Wait until the cache has received t1's record.
        let mut cached = vec![];
        for _ in 0..500 {
            cached = cache
                .lock()
                .await
                .try_list_dir("__fd_object_owners")
                .await?;
            if !cached.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            1,
            cached.len(),
            "t10's record must not be cached: {cached:?}"
        );

        let listed = t1_api.list_ownerships().await?;
        assert_eq!(1, listed.len());
        assert_eq!(udf("f1"), listed[0].data.object);

        t1_api.grant_ownership(&udf("f2"), "r1").await?;
        t1_api.transfer_ownership_to_admin("r1").await?;
        for name in ["f1", "f2"] {
            let owner = t1_api.get_ownership(&udf(name)).await?.unwrap();
            assert_eq!(BUILTIN_ROLE_ACCOUNT_ADMIN, owner.role, "owner of {name}");
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

    let tenant = Tenant::new_literal("admin");
    let cache = RoleMgr::new_cache(client.clone(), &tenant).await;
    let mgr = RoleMgr::create(
        test_api.clone(),
        &tenant,
        enable_meta_data_upgrade_json_to_pb_from_v307,
        Some(Arc::new(Mutex::new(cache))),
    );
    Ok((test_api, mgr))
}
