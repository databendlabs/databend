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

use databend_common_catalog::lock::Lock;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::LockType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::LockGuard;

use crate::locks::LockManager;

pub struct TableLock {
    lock_mgr: Arc<LockManager>,
    table_info: TableInfo,
}

impl TableLock {
    pub fn create(lock_mgr: Arc<LockManager>, table_info: TableInfo) -> Arc<dyn Lock> {
        Arc::new(TableLock {
            lock_mgr,
            table_info,
        })
    }
}

#[async_trait::async_trait]
impl Lock for TableLock {
    fn lock_type(&self) -> LockType {
        LockType::TABLE
    }

    fn get_catalog(&self) -> &str {
        self.table_info.catalog()
    }

    fn get_table_id(&self) -> u64 {
        self.table_info.ident.table_id
    }

    fn tenant_name(&self) -> &str {
        &self.table_info.tenant
    }

    async fn try_lock(&self, ctx: Arc<dyn TableContext>) -> Result<Option<LockGuard>> {
        self.lock_mgr.try_lock(ctx, self).await
    }

    async fn try_lock_no_retry(&self, ctx: Arc<dyn TableContext>) -> Result<Option<LockGuard>> {
        self.lock_mgr.try_lock_no_retry(ctx, self).await
    }
}
