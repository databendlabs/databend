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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::tokio::time::sleep;
use common_base::base::tokio::time::timeout;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::WatchRequest;
use common_users::UserApiProvider;
use futures::future::select;
use futures::future::Either;
use futures_util::StreamExt;
use rand::thread_rng;
use rand::Rng;

use crate::table_context::TableContext;

pub struct MutationLockHeartbeat {
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    shutdown_handler: Option<JoinHandle<Result<()>>>,
}

impl MutationLockHeartbeat {
    pub async fn try_create(
        ctx: Arc<dyn TableContext>,
        table_info: TableInfo,
    ) -> Result<MutationLockHeartbeat> {
        let revision = Self::acquire_lock(ctx.clone(), table_info.clone()).await?;

        let shutdown_notify = Arc::new(Notify::new());
        let shutdown_flag = Arc::new(AtomicBool::new(false));

        let expire_secs = ctx.get_settings().get_mutation_lock_expire_secs()?;
        let sleep_range = (expire_secs * 1000 / 3)..=((expire_secs * 1000 / 3) * 2);

        let shutdown_handler: JoinHandle<Result<()>> = GlobalIORuntime::instance().spawn({
            let shutdown_flag = shutdown_flag.clone();
            let shutdown_notify = shutdown_notify.clone();

            async move {
                let catalog = ctx.get_catalog(table_info.catalog())?;
                let mut notified = Box::pin(shutdown_notify.notified());
                while !shutdown_flag.load(Ordering::Relaxed) {
                    let mills = {
                        let mut rng = thread_rng();
                        rng.gen_range(sleep_range.clone())
                    };
                    let sleep = Box::pin(sleep(Duration::from_millis(mills)));
                    match select(notified, sleep).await {
                        Either::Left((_, _)) => {
                            catalog
                                .delete_mutation_lock_rev(&table_info, revision)
                                .await?;
                            break;
                        }
                        Either::Right((_, new_notified)) => {
                            notified = new_notified;
                            catalog
                                .upsert_mutation_lock_rev(expire_secs, &table_info, Some(revision))
                                .await?;
                        }
                    }
                }
                Ok(())
            }
        });
        Ok(MutationLockHeartbeat {
            shutdown_flag,
            shutdown_notify,
            shutdown_handler: Some(shutdown_handler),
        })
    }

    async fn acquire_lock(ctx: Arc<dyn TableContext>, table_info: TableInfo) -> Result<u64> {
        let catalog = ctx.get_catalog(table_info.catalog())?;
        let expire_secs = ctx.get_settings().get_mutation_lock_expire_secs()?;
        let res = catalog
            .upsert_mutation_lock_rev(expire_secs, &table_info, None)
            .await?;
        let revision = res.revision;

        let table_id = table_info.ident.table_id;
        let duration = Duration::from_secs(expire_secs);
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        loop {
            let reply = catalog.list_table_mutation_lock_revs(table_id).await?;
            let position =
                reply
                    .revisions
                    .iter()
                    .position(|x| *x == revision)
                    .ok_or(ErrorCode::Internal(
                        "Cannot get mutation lock revision".to_string(),
                    ))?;

            if position == 0 {
                // The lock is acquired by current session.
                break;
            }

            let key = format!("{}/{}", reply.prefix, reply.revisions[position - 1]);
            let req = WatchRequest {
                key,
                key_end: None,
                filter_type: FilterType::Delete.into(),
            };
            let mut watch_stream = meta_api.watch(req).await?;
            match timeout(duration, async move {
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
                Ok(_) => Ok(()),
                Err(_) => {
                    catalog
                        .delete_mutation_lock_rev(&table_info, revision)
                        .await?;
                    Err(ErrorCode::TableMutationAlreadyLocked(
                        "table is locked by other session, please try later".to_string(),
                    ))
                }
            }?;
        }
        Ok(revision)
    }

    #[async_backtrace::framed]
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown_handler) = self.shutdown_handler.take() {
            self.shutdown_flag.store(true, Ordering::Relaxed);
            self.shutdown_notify.notify_waiters();
            if let Err(shutdown_failure) = shutdown_handler.await {
                return Err(ErrorCode::TokioError(format!(
                    "Cannot shutdown mutation lock heartbeat, cause {:?}",
                    shutdown_failure
                )));
            }
        }
        Ok(())
    }
}
