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

//! CREATE writes ownership in the same Meta transaction as the object's
//! metadata. SQL tests cannot leave a stale ownership behind, so it is seeded here.

use databend_common_management::udf::UdfMgr;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::principal::UdfIdent;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::StructKey;
use databend_meta_runtime::DatabendRuntime;

const NAME: &str = "stale_owned_object";

/// CREATE over a stale ownership left by a dropped object replaces it with the
/// creator, or removes it without a current role.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_replaces_stale_owner() -> anyhow::Result<()> {
    for role in [Some("new_owner"), None] {
        let tenant = Tenant::new_literal("stale_owned_object");
        let object = OwnershipObject::UDF { name: NAME.into() };
        let owner_key = TenantOwnershipObjectIdent::new(&tenant, object.clone());
        let kv = MetaStore::new_local_testing::<DatabendRuntime>()
            .await
            .arc();
        kv.upsert_pb(&UpsertPB::update(owner_key.clone(), OwnershipInfo {
            object,
            role: "old_owner".into(),
        }))
        .await?;

        let udf = UserDefinedFunction::create_lambda_udf(NAME, vec!["p".into()], "p + 1", "");
        UdfMgr::create(kv.clone(), &tenant)
            .add_udf_with_owner(udf, false, role)
            .await??;

        let udf_key = UdfIdent::new(&tenant, NAME).to_string_key();
        assert!(kv.get_kv(&udf_key).await?.is_some());
        let owner = kv.get_pb(&owner_key).await?.map(|owner| owner.data.role);
        assert_eq!(owner.as_deref(), role, "{role:?}");
    }
    Ok(())
}
