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
use databend_common_meta_api::deserialize_struct;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_meta_store::MetaStore;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::MatchSeq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_update_quota_from_json_to_pb() -> anyhow::Result<()> {
    let (kv_api, quota_api_json, quota_api_pb) = new_quota_api().await?;

    // when disable write pb

    let quota0 = TenantQuota {
        max_databases: 2,
        max_tables_per_database: 3,
        max_stages: 4,
        max_files_per_stage: 5,
        max_users: 6,
    };
    quota_api_json.set_quota(&quota0, MatchSeq::GE(0)).await?;

    let value = kv_api.get_kv("__fd_quotas/admin").await?;
    let s: String = String::from_utf8(value.unwrap().data)?;
    assert_eq!(
        s,
        "{\"max_databases\":2,\"max_tables_per_database\":3,\"max_stages\":4,\"max_files_per_stage\":5,\"max_users\":6}"
    );

    let quota1 = quota_api_json.get_quota(MatchSeq::GE(0)).await?.data;
    assert_eq!(quota1, quota0);

    let value = kv_api.get_kv("__fd_quotas/admin").await?;
    let s: String = String::from_utf8(value.unwrap().data)?;
    assert_eq!(
        s,
        "{\"max_databases\":2,\"max_tables_per_database\":3,\"max_stages\":4,\"max_files_per_stage\":5,\"max_users\":6}"
    );

    // when enable write pb

    let quota2 = quota_api_pb.get_quota(MatchSeq::GE(0)).await?.data;
    assert_eq!(quota2, quota0);

    let value = kv_api.get_kv("__fd_quotas/admin").await?;
    let res = deserialize_struct::<TenantQuota>(&value.unwrap().data);
    assert_eq!(res.unwrap(), quota0);

    let quota3 = quota_api_json.get_quota(MatchSeq::GE(0)).await?.data;
    assert_eq!(quota3, quota0);

    let quota4 = TenantQuota {
        max_databases: 3,
        ..quota0
    };
    quota_api_pb.set_quota(&quota4, MatchSeq::GE(0)).await?;

    let quota5 = quota_api_pb.get_quota(MatchSeq::GE(0)).await?.data;
    assert_eq!(quota5, quota4);

    let value = kv_api.get_kv("__fd_quotas/admin").await?;
    let res = deserialize_struct::<TenantQuota>(&value.unwrap().data);
    assert_eq!(res.unwrap(), quota4);

    Ok(())
}

async fn new_quota_api() -> Result<(Arc<MetaStore>, QuotaMgr<false>, QuotaMgr<true>)> {
    let test_api = MetaStore::new_local_testing::<DatabendRuntime>().await;
    let test_api = Arc::new(test_api);
    let mgr_json = QuotaMgr::<false>::create(test_api.clone(), &Tenant::new_literal("admin"));
    let mgr_pb = QuotaMgr::<true>::create(test_api.clone(), &Tenant::new_literal("admin"));
    Ok((test_api, mgr_json, mgr_pb))
}
