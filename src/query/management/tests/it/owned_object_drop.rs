// Copyright 2026 Datafuse Labs
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
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_management::ConnectionMgr;
use databend_common_management::StageApi;
use databend_common_management::StageMgr;
use databend_common_management::udf::UdfMgr;
use databend_common_meta_api::SequenceApi;
use databend_common_meta_api::kv::ownership::ownership_grant_object;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::txn_put_pb;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleIdent;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::StageFileIdent;
use databend_common_meta_app::principal::StageIdent;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::principal::UdfIdent;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::principal::connection_ident::ConnectionIdent;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::SequenceMeta;
use databend_common_meta_app::schema::sequence_storage::SequenceStorageIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KVStream;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::Change;
use databend_meta_client::types::InvalidReply;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::TxnOp;
use databend_meta_client::types::TxnReply;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::UpsertKV;
use databend_meta_client::types::txn_op::Request;
use databend_meta_runtime::DatabendRuntime;
use futures::stream::BoxStream;

const NAME: &str = "atomic_drop";

enum Hook {
    Fail,
    Before(TxnRequest),
    After(TxnRequest),
}

/// Inject a failure or a concurrent writer exactly at the drop transaction.
struct DropKV {
    inner: Arc<dyn KVApi<Error = MetaError>>,
    ownership_key: String,
    hook: Mutex<Option<Hook>>,
    drops: AtomicUsize,
}

#[async_trait::async_trait]
impl KVApi for DropKV {
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
        let is_drop = txn.if_then.iter().any(|op| {
            matches!(&op.request, Some(Request::Delete(delete)) if delete.key == self.ownership_key)
        });
        let hook = if is_drop {
            self.drops.fetch_add(1, Ordering::SeqCst);
            self.hook.lock().unwrap().take()
        } else {
            None
        };
        match hook {
            Some(Hook::Fail) => Err(InvalidReply::new(
                "injected drop failure",
                &std::io::Error::other("test"),
            )
            .into()),
            Some(Hook::Before(write)) => {
                assert!(self.inner.transaction(write).await?.success);
                self.inner.transaction(txn).await
            }
            Some(Hook::After(write)) => {
                let reply = self.inner.transaction(txn).await?;
                assert!(reply.success);
                assert!(self.inner.transaction(write).await?.success);
                Ok(reply)
            }
            None => self.inner.transaction(txn).await,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Kind {
    Sequence,
    Connection,
    Stage,
    Udf,
}

impl Kind {
    fn object(self) -> OwnershipObject {
        let name = NAME.to_string();
        match self {
            Self::Sequence => OwnershipObject::Sequence { name },
            Self::Connection => OwnershipObject::Connection { name },
            Self::Stage => OwnershipObject::Stage { name },
            Self::Udf => OwnershipObject::UDF { name },
        }
    }

    fn record(self, tenant: &Tenant) -> (String, TxnOp) {
        match self {
            Self::Sequence => {
                let key = SequenceIdent::new(tenant, NAME);
                let meta = SequenceMeta::from(CreateSequenceReq {
                    override_existing: false,
                    ident: key.clone(),
                    create_on: chrono::Utc::now(),
                    start: 1,
                    increment: 1,
                    comment: None,
                    storage_version: 0,
                });
                (key.to_string_key(), txn_put_pb(&key, &meta))
            }
            Self::Connection => {
                let key = ConnectionIdent::new(tenant, NAME);
                let meta = UserDefinedConnection {
                    name: NAME.into(),
                    ..Default::default()
                };
                (key.to_string_key(), txn_put_pb(&key, &meta))
            }
            Self::Stage => {
                let key = StageIdent::new(tenant, NAME);
                let meta = StageInfo {
                    stage_name: NAME.into(),
                    ..Default::default()
                };
                (key.to_string_key(), txn_put_pb(&key, &meta))
            }
            Self::Udf => {
                let key = UdfIdent::new(tenant, NAME);
                let meta = UserDefinedFunction::create_lambda_udf(NAME, vec!["p".into()], "p", "");
                (key.to_string_key(), txn_put_pb(&key, &meta))
            }
        }
    }

    async fn drop(self, tenant: &Tenant, kv: Arc<DropKV>) -> anyhow::Result<()> {
        match self {
            Self::Sequence => assert!(
                kv.drop_sequence(DropSequenceReq {
                    ident: SequenceIdent::new(tenant, NAME),
                    if_exists: false,
                })
                .await?
                .success
            ),
            Self::Connection => assert!(
                ConnectionMgr::create(kv, tenant)
                    .remove_with_ownership(NAME, MatchSeq::GE(1), self.object())
                    .await?
            ),
            Self::Stage => StageMgr::create(kv, tenant).drop_stage(NAME).await?,
            Self::Udf => assert!(
                UdfMgr::create(kv, tenant)
                    .drop_udf(NAME, MatchSeq::GE(1))
                    .await?
                    .is_some()
            ),
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_owned_object_drop_transactions() -> anyhow::Result<()> {
    for kind in [Kind::Sequence, Kind::Connection, Kind::Stage, Kind::Udf] {
        // These cases exercise Meta transaction boundaries, not SQL/RBAC behavior.
        for scenario in [
            "missing_owner",
            "legacy",
            "failure",
            "transfer",
            "role_update",
            "recreate",
        ] {
            let tenant = Tenant::new_literal("atomic_drop");
            let inner = MetaStore::new_local_testing::<DatabendRuntime>()
                .await
                .arc();
            let owner_key = TenantOwnershipObjectIdent::new(&tenant, kind.object());
            let role_key = RoleIdent::new(&tenant, "old_owner");
            let owner = OwnershipInfo {
                object: kind.object(),
                role: "old_owner".into(),
            };
            let grant = ownership_grant_object(&kind.object());
            let mut role = RoleInfo::new("old_owner", None);
            if scenario != "missing_owner" {
                role.grants
                    .grant_privileges(&grant, UserPrivilegeType::Ownership.into());
            }
            role.grants
                .grant_privileges(&GrantObject::Global, UserPrivilegeType::CreateUser.into());
            let (key, object_put) = kind.record(&tenant);
            let mut seed = vec![object_put.clone(), txn_put_pb(&role_key, &role)];
            let auxiliary = match kind {
                Kind::Sequence => Some(
                    SequenceStorageIdent::new_from(SequenceIdent::new(&tenant, NAME))
                        .to_string_key(),
                ),
                Kind::Stage => Some(
                    StageFileIdent::new(StageIdent::new(&tenant, NAME), "file.csv").to_string_key(),
                ),
                _ => None,
            };
            if let Some(key) = &auxiliary {
                // DROP only needs the auxiliary key, not its encoded value.
                seed.push(TxnOp::put(key, b"auxiliary".to_vec()));
            }
            if scenario != "missing_owner" {
                // Exercise the JSON fallback without upgrading records outside the transaction.
                seed.push(TxnOp::put(
                    owner_key.to_string_key(),
                    serde_json::to_vec(&owner)?,
                ));
                seed.push(TxnOp::put(
                    role_key.to_string_key(),
                    serde_json::to_vec(&role)?,
                ));
            }
            assert!(
                inner
                    .transaction(TxnRequest::new(vec![], seed))
                    .await?
                    .success
            );
            let original = inner.get_kv(&key).await?;
            let original_owner = inner.get_kv(&owner_key.to_string_key()).await?;
            let original_role = inner.get_kv(&role_key.to_string_key()).await?;
            let replacement = OwnershipInfo {
                object: kind.object(),
                role: "new_owner".into(),
            };
            let hook = match scenario {
                "failure" => Some(Hook::Fail),
                "transfer" => {
                    role.grants
                        .revoke_privileges(&grant, UserPrivilegeType::Ownership.into());
                    Some(Hook::Before(TxnRequest::new(vec![], vec![
                        txn_put_pb(&owner_key, &replacement),
                        txn_put_pb(&role_key, &role),
                    ])))
                }
                "role_update" => {
                    role.grants.grant_privileges(
                        &GrantObject::Global,
                        UserPrivilegeType::CreateRole.into(),
                    );
                    Some(Hook::Before(TxnRequest::new(vec![], vec![txn_put_pb(
                        &role_key, &role,
                    )])))
                }
                "recreate" => {
                    let mut writes = vec![object_put, txn_put_pb(&owner_key, &replacement)];
                    if let Some(key) = &auxiliary {
                        writes.push(TxnOp::put(key, b"replacement".to_vec()));
                    }
                    Some(Hook::After(TxnRequest::new(vec![], writes)))
                }
                _ => None,
            };
            let kv = Arc::new(DropKV {
                inner,
                ownership_key: owner_key.to_string_key(),
                hook: Mutex::new(hook),
                drops: AtomicUsize::new(0),
            });
            let result = kind.drop(&tenant, kv.clone()).await;
            if scenario == "failure" {
                assert!(result.is_err(), "{kind:?}");
                assert_eq!(kv.get_kv(&key).await?, original);
                assert_eq!(kv.get_kv(&owner_key.to_string_key()).await?, original_owner);
                assert_eq!(kv.get_kv(&role_key.to_string_key()).await?, original_role);
                if let Some(key) = &auxiliary {
                    assert_eq!(kv.get_kv(key).await?.unwrap().data, b"auxiliary");
                }
                continue;
            }
            result?;
            if scenario == "recreate" {
                assert!(kv.get_kv(&key).await?.unwrap().seq > original.unwrap().seq);
                assert_eq!(kv.get_pb(&owner_key).await?.unwrap().data, replacement);
            } else {
                assert!(kv.get_kv(&key).await?.is_none(), "{kind:?}: {scenario}");
                assert!(kv.get_pb(&owner_key).await?.is_none());
            }
            if let Some(key) = &auxiliary {
                if scenario == "recreate" {
                    assert_eq!(kv.get_kv(key).await?.unwrap().data, b"replacement");
                } else {
                    assert!(kv.get_kv(key).await?.is_none());
                }
            }
            if matches!(scenario, "legacy" | "role_update" | "recreate") {
                let remaining = kv.get_pb(&role_key).await?.unwrap().data;
                assert!(
                    !remaining
                        .grants
                        .verify_privilege(&grant, UserPrivilegeType::Ownership)
                );
                assert!(
                    remaining
                        .grants
                        .verify_privilege(&GrantObject::Global, UserPrivilegeType::CreateUser)
                );
            }
            if scenario == "role_update" {
                let remaining = kv.get_pb(&role_key).await?.unwrap().data;
                assert!(
                    remaining
                        .grants
                        .verify_privilege(&GrantObject::Global, UserPrivilegeType::CreateRole)
                );
            }
            assert_eq!(
                kv.drops.load(Ordering::SeqCst),
                if matches!(scenario, "transfer" | "role_update") {
                    2
                } else {
                    1
                }
            );
        }
    }
    Ok(())
}
