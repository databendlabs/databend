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

use common_base::base::GlobalInstance;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_license::license::Feature;
use common_license::license_manager::get_license_manager;
use common_pipeline_core::TableLock;

#[async_trait::async_trait]
pub trait TableLockManager: Sync + Send {
    async fn try_lock(
        &self,
        ctx: Arc<dyn TableContext>,
        lock: &mut dyn TableLock,
        catalog: &str,
    ) -> Result<()>;

    fn unlock(&self, revision: u64);
}

pub struct DummyTableLock {}

#[async_trait::async_trait]
impl TableLockManager for DummyTableLock {
    #[async_backtrace::framed]
    async fn try_lock(
        &self,
        _ctx: Arc<dyn TableContext>,
        _lock: &mut dyn TableLock,
        _catalog: &str,
    ) -> Result<()> {
        Ok(())
    }

    fn unlock(&self, _revision: u64) {}
}

pub struct TableLockManagerWrapper {
    handler: Box<dyn TableLockManager>,
}

impl TableLockManagerWrapper {
    pub fn new(handler: Box<dyn TableLockManager>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn try_lock(
        &self,
        ctx: Arc<dyn TableContext>,
        lock: &mut dyn TableLock,
        catalog: &str,
    ) -> Result<()> {
        self.handler.try_lock(ctx, lock, catalog).await
    }

    pub fn unlock(&self, revision: u64) {
        self.handler.unlock(revision)
    }

    pub fn instance(ctx: Arc<dyn TableContext>) -> Arc<TableLockManagerWrapper> {
        let enabled_table_lock = ctx.get_settings().get_enable_table_lock().unwrap_or(false);

        let dummy = Arc::new(TableLockManagerWrapper::new(Box::new(DummyTableLock {})));

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
