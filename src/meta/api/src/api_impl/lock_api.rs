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

use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::TableLockExpired;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::id_generator::IdGeneratorValue;
use databend_common_meta_app::schema::CreateLockRevReply;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListLocksReq;
use databend_common_meta_app::schema::LockInfo;
use databend_common_meta_app::schema::LockMeta;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::ConditionResult::Eq;
use databend_meta_types::MetaError;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;
use log::info;

use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_crud_api::KVPbCrudApi;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;
use crate::txn_put_pb;
use crate::util::IdempotentKVTxnResponse;
use crate::util::IdempotentKVTxnSender;

/// LockApi defines APIs for lock management.
///
/// This trait handles:
/// - Lock revision management (create, extend, delete)
/// - Lock listing operations
#[async_trait::async_trait]
pub trait LockApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    #[fastrace::trace]
    async fn list_lock_revisions(
        &self,
        req: ListLockRevReq,
    ) -> Result<Vec<(u64, LockMeta)>, KVAppError> {
        let dir = req.lock_key.gen_prefix();
        let strm = self.list_pb(ListOptions::unlimited(&dir)).await?;

        let list = strm
            .map_ok(|itm| (itm.key.revision(), itm.seqv.data))
            .try_collect::<Vec<_>>()
            .await?;

        Ok(list)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_lock_revision(
        &self,
        req: CreateLockRevReq,
    ) -> Result<CreateLockRevReply, KVAppError> {
        let ctx = func_name!();
        debug!(req :? =(&req); "LockApi: {}", ctx);

        let lock_key = &req.lock_key;
        let id_generator = IdGenerator::table_lock_id();

        let mut trials = txn_backoff(None, ctx);
        let txn_sender = IdempotentKVTxnSender::with_ttl(req.ttl);
        loop {
            trials.next().unwrap()?.await;

            let current_rev = self.get_seq(&id_generator).await?;
            let revision = current_rev + 1;
            let key = lock_key.gen_key(revision);
            let lock_meta = LockMeta {
                user: req.user.clone(),
                node: req.node.clone(),
                query_id: req.query_id.clone(),
                created_on: Utc::now(),
                acquired_on: None,
                lock_type: lock_key.lock_type(),
                extra_info: lock_key.get_extra_info(),
            };

            let condition = vec![
                txn_cond_seq(&id_generator, Eq, current_rev),
                // assumes lock are absent.
                txn_cond_seq(&key, Eq, 0),
            ];
            let if_then = vec![
                txn_put_pb(&id_generator, &IdGeneratorValue)?,
                txn_put_pb_with_ttl(&key, &lock_meta, Some(req.ttl))?,
            ];
            let txn_req = TxnRequest::new(condition, if_then);
            let txn_response = txn_sender.send_txn(self, txn_req).await?;
            match txn_response {
                IdempotentKVTxnResponse::Success(_) => {
                    return Ok(CreateLockRevReply { revision });
                }
                IdempotentKVTxnResponse::AlreadyCommitted => {
                    info!(
                        "Transaction ID {} exists, the lock revision has been created successfully",
                        txn_sender.get_txn_id()
                    );
                    return Ok(CreateLockRevReply { revision });
                }
                _ => {
                    // continue looping
                }
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "LockApi: {}", func_name!());

        let ctx = func_name!();

        let lock_key = &req.lock_key;
        let table_id = lock_key.get_table_id();
        let key = lock_key.gen_key(req.revision);

        self.crud_update_existing(
            &key,
            |mut lock_meta| {
                // Set `acquire_lock = true` to initialize `acquired_on` when the
                // first time this lock is acquired. Before the lock is
                // acquired(becoming the first in lock queue), or after being
                // acquired, this argument is always `false`.
                if req.acquire_lock {
                    lock_meta.acquired_on = Some(Utc::now());
                }
                Some((lock_meta, Some(req.ttl)))
            },
            || {
                Err(AppError::TableLockExpired(TableLockExpired::new(
                    table_id, ctx,
                )))
            },
        )
        .await??;
        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "LockApi: {}", func_name!());

        let lock_key = &req.lock_key;

        let revision = req.revision;
        let key = lock_key.gen_key(revision);

        self.crud_remove(&key, || Ok::<(), ()>(())).await?.unwrap();

        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_locks(&self, req: ListLocksReq) -> Result<Vec<LockInfo>, KVAppError> {
        let mut reply = vec![];
        for dir in &req.prefixes {
            let strm = self.list_pb(ListOptions::unlimited(dir)).await?;
            let locks = strm
                .map_ok(|itm| LockInfo {
                    table_id: itm.key.table_id(),
                    revision: itm.key.revision(),
                    meta: itm.seqv.data,
                })
                .try_collect::<Vec<_>>()
                .await?;

            reply.extend(locks);
        }
        Ok(reply)
    }
}

#[async_trait::async_trait]
impl<KV> LockApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
