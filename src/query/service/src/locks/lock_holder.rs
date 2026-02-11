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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use backoff::backoff::Backoff;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::TableLockIdent;
use databend_common_metrics::lock::record_acquired_lock_nums;
use databend_common_metrics::lock::record_created_lock_nums;
use databend_common_storages_fuse::operations::set_backoff;
use databend_common_users::UserApiProvider;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_types::protobuf::WatchRequest;
use databend_meta_types::protobuf::watch_request::FilterType;
use futures::future::Either;
use futures::future::select;
use futures_util::StreamExt;
use rand::Rng;
use rand::thread_rng;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio::time::timeout;

use crate::meta_service_error;
use crate::sessions::SessionManager;

#[derive(Default)]
pub struct LockHolder {
    shutdown_flag: AtomicBool,
    shutdown_notify: Notify,
}

impl LockHolder {
    #[async_backtrace::framed]
    pub(crate) async fn try_acquire_lock(
        self: &Arc<Self>,
        catalog: Arc<dyn Catalog>,
        req: CreateLockRevReq,
        should_retry: bool,
        acquire_timeout: Duration,
    ) -> Result<u64> {
        let start = Instant::now();

        let ttl = req.ttl;

        let lock_key = req.lock_key.clone();
        let lock_type = lock_key.lock_type().to_string();
        let table_id = lock_key.get_table_id();
        let tenant = lock_key.get_tenant();

        let revision = self.start(catalog.clone(), req).await?;

        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let list_table_lock_req = ListLockRevReq::new(lock_key.clone());

        loop {
            // List all revisions and check if the current is the minimum.
            let mut rev_list = catalog
                .list_lock_revisions(list_table_lock_req.clone())
                .await?
                .into_iter()
                .map(|(x, _)| x)
                .collect::<Vec<_>>();
            // list_lock_revisions are returned in big-endian order,
            // we need to sort them in ascending numeric order.
            rev_list.sort();
            let position = rev_list.iter().position(|x| *x == revision).ok_or_else(||
                // If the current is not found in list,  it means that the current has been expired.
                ErrorCode::TableLockExpired(format!(
                    "The acquired table lock with revision '{}' maybe expired(elapsed: {:?})",
                    revision,
                    start.elapsed(),
                )))?;

            if position == 0 {
                // The lock is acquired by current session.
                let extend_table_lock_req =
                    ExtendLockRevReq::new(lock_key.clone(), revision, ttl, true);

                catalog.extend_lock_revision(extend_table_lock_req).await?;
                // metrics.
                record_acquired_lock_nums(lock_type.clone(), 1);
                break;
            }

            let prev_revision = rev_list[position - 1];
            let elapsed = start.elapsed();
            // if no need retry, return error directly.
            if !should_retry || elapsed >= acquire_timeout {
                return Err(ErrorCode::TableAlreadyLocked(format!(
                    "Table is locked by other session(rev: {}, prev: {}, elapsed: {:?})",
                    revision,
                    prev_revision,
                    start.elapsed()
                )));
            }

            let watch_delete_ident = TableLockIdent::new(tenant, table_id, prev_revision);

            // Get the previous revision, watch the delete event.
            let req = WatchRequest::new(watch_delete_ident.to_string_key(), None)
                .with_filter(FilterType::Delete);
            let mut watch_stream = meta_api.watch(req).await.map_err(meta_service_error)?;

            let lock_meta = meta_api
                .get_pb(&watch_delete_ident)
                .await
                .map_err(meta_service_error)?;
            if lock_meta.is_none() {
                log::warn!(
                    "Lock revision '{}' already does not exist, skipping",
                    prev_revision
                );
                continue;
            }

            // Add a timeout period for watch.
            if let Err(_cause) = timeout(acquire_timeout.abs_diff(elapsed), async move {
                while let Some(Ok(resp)) = watch_stream.next().await {
                    if let Some(event) = resp.event {
                        if event.current.is_none() {
                            break;
                        }
                    }
                }
            })
            .await
            {
                return Err(ErrorCode::TableAlreadyLocked(format!(
                    "Table is locked by other session(rev: {}, prev: {}, elapsed: {:?})",
                    revision,
                    prev_revision,
                    start.elapsed()
                )));
            }
        }

        log::info!(
            "Acquired table lock successfully(table_id: {}, lock_type: {}, revision: {}, elapsed: {:?})",
            table_id,
            lock_type,
            revision,
            start.elapsed()
        );
        Ok(revision)
    }

    #[async_backtrace::framed]
    async fn start(
        self: &Arc<Self>,
        catalog: Arc<dyn Catalog>,
        req: CreateLockRevReq,
    ) -> Result<u64> {
        let lock_key = req.lock_key.clone();
        let query_id = req.query_id.clone();
        let ttl = req.ttl;
        let sleep_range = (ttl / 3)..=(ttl * 2 / 3);

        // get a new table lock revision.
        let res = catalog.create_lock_revision(req).await?;
        let revision = res.revision;
        // metrics.
        record_created_lock_nums(lock_key.lock_type().to_string(), 1);
        log::debug!("create table lock success, revision={}", revision);

        let delete_table_lock_req = DeleteLockRevReq::new(lock_key.clone(), revision);
        let extend_table_lock_req = ExtendLockRevReq::new(lock_key.clone(), revision, ttl, false);

        GlobalIORuntime::instance().spawn({
            let self_clone = self.clone();
            async move {
                let mut notified = Box::pin(self_clone.shutdown_notify.notified());
                while !self_clone.shutdown_flag.load(Ordering::SeqCst) {
                    let rand_sleep_duration = {
                        let mut rng = thread_rng();
                        rng.gen_range(sleep_range.clone())
                    };

                    let sleep_range = Box::pin(sleep(rand_sleep_duration));
                    match select(notified, sleep_range).await {
                        Either::Left((_, _)) => {
                            // shutdown.
                            break;
                        }
                        Either::Right((_, new_notified)) => {
                            notified = new_notified;
                            if let Err(e) = self_clone
                                .try_extend_lock(
                                    catalog.clone(),
                                    extend_table_lock_req.clone(),
                                    Some(ttl - rand_sleep_duration),
                                )
                                .await
                            {
                                // Force kill the query if extend lock failure.
                                if let Some(session) =
                                    SessionManager::instance().get_session_by_id(&query_id)
                                {
                                    session.force_kill_query(e.clone());
                                }
                                return Err(e);
                            }
                        }
                    }
                }

                Self::try_delete_lock(catalog, delete_table_lock_req, Some(ttl)).await
            }
        });

        Ok(revision)
    }

    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_one();
    }
}

impl LockHolder {
    async fn try_extend_lock(
        self: &Arc<Self>,
        catalog: Arc<dyn Catalog>,
        req: ExtendLockRevReq,
        max_retry_elapsed: Option<Duration>,
    ) -> Result<()> {
        let mut backoff = set_backoff(Some(Duration::from_millis(2)), None, max_retry_elapsed);
        let mut extend_notified = Box::pin(self.shutdown_notify.notified());
        while !self.shutdown_flag.load(Ordering::SeqCst) {
            match catalog.extend_lock_revision(req.clone()).await {
                Ok(_) => {
                    break;
                }
                Err(e) if e.code() == ErrorCode::TABLE_LOCK_EXPIRED => {
                    log::error!("failed to extend the lock. cause {:?}", e);
                    return Err(e);
                }
                Err(e) => match backoff.next_backoff() {
                    Some(duration) => {
                        log::debug!(
                            "failed to extend the lock, tx will be retried {} ms later. table id {}, revision {}",
                            duration.as_millis(),
                            req.lock_key.get_table_id(),
                            req.revision,
                        );
                        let sleep_gap = Box::pin(sleep(duration));
                        match select(extend_notified, sleep_gap).await {
                            Either::Left((_, _)) => {
                                // shutdown.
                                break;
                            }
                            Either::Right((_, new_notified)) => {
                                extend_notified = new_notified;
                            }
                        }
                    }
                    None => {
                        let error_info = format!(
                            "failed to extend the lock after retries {} ms, aborted. cause {:?}",
                            Instant::now()
                                .duration_since(backoff.start_time)
                                .as_millis(),
                            e,
                        );
                        log::error!("{}", error_info);
                        return Err(ErrorCode::OCCRetryFailure(error_info));
                    }
                },
            }
        }

        Ok(())
    }

    async fn try_delete_lock(
        catalog: Arc<dyn Catalog>,
        req: DeleteLockRevReq,
        max_retry_elapsed: Option<Duration>,
    ) -> Result<()> {
        let mut backoff = set_backoff(Some(Duration::from_millis(2)), None, max_retry_elapsed);
        loop {
            match catalog.delete_lock_revision(req.clone()).await {
                Ok(_) => {
                    log::debug!("delete table lock success, revision={}", req.revision);
                    break;
                }
                Err(e) => match backoff.next_backoff() {
                    Some(duration) => {
                        log::debug!(
                            "failed to delete the lock, tx will be retried {} ms later. table id {}, revision {}",
                            duration.as_millis(),
                            req.lock_key.get_table_id(),
                            req.revision,
                        );
                        sleep(duration).await;
                    }
                    None => {
                        let error_info = format!(
                            "failed to delete the lock after retries {} ms, aborted. cause {:?}",
                            Instant::now()
                                .duration_since(backoff.start_time)
                                .as_millis(),
                            e,
                        );
                        log::error!("{}", error_info);
                        return Err(ErrorCode::OCCRetryFailure(error_info));
                    }
                },
            }
        }
        Ok(())
    }
}
