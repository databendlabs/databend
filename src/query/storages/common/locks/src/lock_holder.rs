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
use common_base::base::tokio::time::sleep;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_catalog::catalog::Catalog;
use common_catalog::lock::Lock;
use common_catalog::lock::LockExt;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::select;
use futures::future::Either;
use rand::thread_rng;
use rand::Rng;

#[derive(Default)]
pub struct LockHolder {
    shutdown_flag: AtomicBool,
    shutdown_notify: Notify,
}

impl LockHolder {
    #[async_backtrace::framed]
    pub async fn start<T: Lock + ?Sized>(
        self: &Arc<Self>,
        catalog: Arc<dyn Catalog>,
        lock: &T,
        revision: u64,
        expire_secs: u64,
    ) -> Result<()> {
        let sleep_range: std::ops::RangeInclusive<u64> =
            (expire_secs * 1000 / 3)..=((expire_secs * 1000 / 3) * 2);
        let delete_table_lock_req = lock.gen_delete_lock_req(revision);
        let extend_table_lock_req = lock.gen_extend_lock_req(revision, expire_secs, false);

        GlobalIORuntime::instance().spawn({
            let self_clone = self.clone();
            async move {
                let mut notified = Box::pin(self_clone.shutdown_notify.notified());
                while !self_clone.shutdown_flag.load(Ordering::SeqCst) {
                    let mills = {
                        let mut rng = thread_rng();
                        rng.gen_range(sleep_range.clone())
                    };
                    let sleep = Box::pin(sleep(Duration::from_millis(mills)));
                    match select(notified, sleep).await {
                        Either::Left((_, _)) => {
                            break;
                        }
                        Either::Right((_, new_notified)) => {
                            notified = new_notified;
                            catalog
                                .extend_lock_revision(extend_table_lock_req.clone())
                                .await?;
                        }
                    }
                }
                catalog.delete_lock_revision(delete_table_lock_req).await?;
                Ok::<_, ErrorCode>(())
            }
        });

        Ok(())
    }

    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
    }
}
