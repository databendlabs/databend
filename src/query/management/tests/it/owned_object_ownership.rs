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
//! metadata. SQL tests cannot inject a failure between the two writes or leave
//! a stale ownership behind, so these cases are covered here.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_management::udf::UdfMgr;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::txn_put_pb;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::principal::UdfIdent;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KVStream;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::Change;
use databend_meta_client::types::InvalidReply;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::TxnOp;
use databend_meta_client::types::TxnReply;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::UpsertKV;
use databend_meta_client::types::txn_op::Request;
use databend_meta_runtime::DatabendRuntime;
use futures::stream::BoxStream;

const NAME: &str = "atomic_owned_object";

/// Fails the next transaction that writes the object's ownership key.
struct FailingKV {
    inner: Arc<dyn KVApi<Error = MetaError>>,
    ownership_key: String,
    fail: AtomicBool,
}

#[async_trait::async_trait]
impl KVApi for FailingKV {
    type Error = MetaError;

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, MetaError>>,
    ) -> Result<KVStream<MetaError>, MetaError> {
        self.inner.get_many_kv(keys).await
    }

    async fn list_kv(&self, opts: ListOptions<'_, str>) -> Result<KVStream<MetaError>, MetaError> {
        self.inner.list_kv(opts).await
    }

    async fn upsert_kv(&self, req: UpsertKV) -> Result<Change<Vec<u8>>, MetaError> {
        self.inner.upsert_kv(req).await
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, MetaError> {
        let writes_owner = txn.if_then.iter().any(|op| match &op.request {
            Some(Request::Delete(delete)) => delete.key == self.ownership_key,
            Some(Request::Put(put)) => put.key == self.ownership_key,
            _ => false,
        });
        if writes_owner && self.fail.swap(false, Ordering::SeqCst) {
            let error = std::io::Error::other("test");
            return Err(InvalidReply::new("injected ownership transaction failure", &error).into());
        }
        self.inner.transaction(txn).await
    }
}

/// A fresh store for one object, seeded with `old_owner` as its owner.
struct Case {
    tenant: Tenant,
    owner_key: TenantOwnershipObjectIdent,
    kv: Arc<FailingKV>,
}

impl Case {
    async fn new(object: OwnershipObject, records: impl FnOnce(&Tenant) -> Vec<TxnOp>) -> Self {
        let tenant = Tenant::new_literal("atomic_owned_object");
        let owner_key = TenantOwnershipObjectIdent::new(&tenant, object.clone());
        let inner = MetaStore::new_local_testing::<DatabendRuntime>()
            .await
            .arc();
        let mut seed = records(&tenant);
        seed.push(txn_put_pb(&owner_key, &OwnershipInfo {
            object,
            role: "old_owner".into(),
        }));
        assert!(
            inner
                .transaction(TxnRequest::new(vec![], seed))
                .await
                .unwrap()
                .success
        );
        let kv = Arc::new(FailingKV {
            inner,
            ownership_key: owner_key.to_string_key(),
            fail: AtomicBool::new(false),
        });
        Case {
            tenant,
            owner_key,
            kv,
        }
    }

    fn fail_next_ownership_write(&self) {
        self.kv.fail.store(true, Ordering::SeqCst);
    }

    async fn owner(&self) -> Option<String> {
        let owner = self.kv.get_pb(&self.owner_key).await.unwrap();
        owner.map(|owner| owner.data.role)
    }

    async fn exists(&self, key: &str) -> bool {
        self.kv.get_kv(key).await.unwrap().is_some()
    }
}

fn udf_key(tenant: &Tenant) -> UdfIdent {
    UdfIdent::new(tenant, NAME)
}

fn udf() -> UserDefinedFunction {
    UserDefinedFunction::create_lambda_udf(NAME, vec!["p".into()], "p + 1", "")
}

/// CREATE over a stale ownership left by a dropped object replaces it with the
/// creator, or removes it without a current role. A failed transaction writes
/// neither the object nor the owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_replaces_stale_owner_atomically() -> anyhow::Result<()> {
    for (role, fail) in [
        (Some("new_owner"), false),
        (None, false),
        (Some("new_owner"), true),
    ] {
        let case = Case::new(OwnershipObject::UDF { name: NAME.into() }, |_| vec![]).await;
        if fail {
            case.fail_next_ownership_write();
        }
        let result = UdfMgr::create(case.kv.clone(), &case.tenant)
            .add_udf_with_owner(udf(), false, role)
            .await;
        let created = case.exists(&udf_key(&case.tenant).to_string_key()).await;
        if fail {
            assert!(result.is_err());
            assert!(!created);
            assert_eq!(case.owner().await.as_deref(), Some("old_owner"));
        } else {
            result??;
            assert!(created);
            assert_eq!(case.owner().await.as_deref(), role, "{role:?}");
        }
    }
    Ok(())
}
