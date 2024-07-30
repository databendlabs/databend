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
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::LockKey;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_fuse::operations::set_backoff;
use fastrace::func_name;

#[async_trait::async_trait]
pub trait LockExt: Lock {
    /// Return true if the lock is expired.
    async fn wait_lock_expired(&self, catalog: Arc<dyn Catalog>) -> Result<bool> {
        let tenant_name = self.tenant_name();
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let lock_key = LockKey::Table {
            tenant: tenant.clone(),
            table_id: self.get_table_id(),
        };

        let req = ListLockRevReq::new(lock_key);

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
}

impl<T: ?Sized> LockExt for T where T: Lock {}
