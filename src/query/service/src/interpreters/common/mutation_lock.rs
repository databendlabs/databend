// Copyright 2023 Datafuse Labs.
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
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_storages_fuse::TableContext;
use futures::future::select;
use futures::future::Either;

use crate::sessions::QueryContext;

pub struct MutationLockHeartbeat {
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    shutdown_handler: Option<JoinHandle<Result<()>>>,
}

impl MutationLockHeartbeat {
    pub fn create(ctx: Arc<QueryContext>, table_info: TableInfo) -> Self {
        let shutdown_notify = Arc::new(Notify::new());
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_handler: JoinHandle<Result<()>> = GlobalIORuntime::instance().spawn({
            let shutdown_flag = shutdown_flag.clone();
            let shutdown_notify = shutdown_notify.clone();

            async move {
                let catalog = ctx.get_catalog(table_info.catalog())?;
                let mut notified = Box::pin(shutdown_notify.notified());
                while !shutdown_flag.load(Ordering::Relaxed) {
                    let interval = Box::pin(sleep(Duration::from_secs(1)));
                    match select(notified, interval).await {
                        Either::Left((_, _)) => {
                            catalog.drop_table_mutation_lock(&table_info).await?;
                            break;
                        }
                        Either::Right((_, new_notified)) => {
                            notified = new_notified;
                            catalog
                                .upsert_table_mutation_lock(10, &table_info, false)
                                .await?;
                        }
                    }
                }
                Ok(())
            }
        });
        MutationLockHeartbeat {
            shutdown_flag,
            shutdown_notify,
            shutdown_handler: Some(shutdown_handler),
        }
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
