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

use std::collections::HashMap;
use std::sync::Arc;

use async_channel::Sender;
use common_base::base::GlobalInstance;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_license::license::Feature;
use common_license::license_manager::get_license_manager;
use common_pipeline_core::TableLock;
use parking_lot::RwLock;

use crate::table_lock_handler::DummyTableLock;
use crate::TableLockHandler;
use crate::TableLockHeartbeat;

pub struct TableLockManager {
    active_lock: Arc<RwLock<HashMap<u64, TableLockHeartbeat>>>,
    handler: Box<dyn TableLockHandler>,
    tx: Sender<u64>,
}

impl TableLockManager {
    pub fn create(handler: Box<dyn TableLockHandler>) -> Self {
        let (tx, rx) = async_channel::unbounded();
        let active_lock = Arc::new(RwLock::new(HashMap::<u64, TableLockHeartbeat>::new()));
        GlobalIORuntime::instance().spawn({
            let active_lock = active_lock.clone();
            async move {
                while let Ok(revision) = rx.recv().await {
                    let lock = active_lock.write().remove(&revision);
                    if let Some(mut lock) = lock {
                        if let Err(cause) = lock.shutdown().await {
                            log::warn!("Cannot shutdown table lock heartbeat, cause {:?}", cause);
                        }
                    }
                }
            }
        });
        TableLockManager {
            active_lock,
            handler,
            tx,
        }
    }

    pub async fn try_lock(
        self: &Arc<Self>,
        ctx: Arc<dyn TableContext>,
        lock: &mut dyn TableLock,
    ) -> Result<()> {
        let heartbeat = self.handler.try_lock(ctx, lock).await?;

        let revision = lock.revision();
        assert!(revision > 0);

        let mut active_lock = self.active_lock.write();
        active_lock.insert(revision, heartbeat);
        Ok(())
    }

    pub fn unlock(&self, revision: u64) {
        let _ = self.tx.send_blocking(revision);
    }

    pub fn instance(ctx: Arc<dyn TableContext>) -> Arc<TableLockManager> {
        let enabled_table_lock = ctx.get_settings().get_enable_table_lock().unwrap_or(false);

        let dummy = Arc::new(TableLockManager::create(Box::new(DummyTableLock {})));

        if !enabled_table_lock {
            // dummy lock does nothing
            return dummy;
        }

        let enterprise_enabled = get_license_manager()
            .manager
            .check_enterprise_enabled(ctx.get_license_key(), Feature::TableLock)
            .is_ok();
        if enterprise_enabled {
            GlobalInstance::get()
        } else {
            dummy
        }
    }
}
