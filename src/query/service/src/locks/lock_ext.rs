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
use std::time::Duration;

use backoff::backoff::Backoff;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::lock::Lock;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_storages_fuse::operations::set_backoff;

#[async_trait::async_trait]
pub trait LockExt: Lock {
    /// Return true if the lock is expired.
    async fn wait_lock_expired(&self, catalog: Arc<dyn Catalog>) -> Result<bool> {
        let req = ListLockRevReq {
            lock_key: self.gen_lock_key(),
        };

        let max_retry_delay = Some(Duration::from_millis(500));
        let max_retry_elapsed = Some(Duration::from_secs(3));
        let mut backoff = set_backoff(None, max_retry_delay, max_retry_elapsed);
        loop {
            let reply = catalog.list_lock_revisions(req.clone()).await?;
            if reply.is_empty() {
                return Ok(true);
            }

            match backoff.next_backoff() {
                Some(d) => {
                    log::debug!(
                        "wait lock expired, check lock will be retried {} ms later.",
                        d.as_millis()
                    );
                    databend_common_base::base::tokio::time::sleep(d).await;
                    continue;
                }
                None => {
                    return Ok(false);
                }
            }
        }
    }

    fn gen_create_lock_req(
        &self,
        user: String,
        node: String,
        query_id: String,
        expire_secs: u64,
    ) -> CreateLockRevReq {
        CreateLockRevReq {
            lock_key: self.gen_lock_key(),
            user,
            node,
            query_id,
            expire_secs,
        }
    }

    fn gen_list_lock_req(&self) -> ListLockRevReq {
        ListLockRevReq {
            lock_key: self.gen_lock_key(),
        }
    }

    fn gen_delete_lock_req(&self, revision: u64) -> DeleteLockRevReq {
        DeleteLockRevReq {
            lock_key: self.gen_lock_key(),
            revision,
        }
    }

    fn gen_extend_lock_req(
        &self,
        revision: u64,
        expire_secs: u64,
        acquire_lock: bool,
    ) -> ExtendLockRevReq {
        ExtendLockRevReq {
            lock_key: self.gen_lock_key(),
            revision,
            acquire_lock,
            expire_secs,
        }
    }
}

impl<T: ?Sized> LockExt for T where T: Lock {}
