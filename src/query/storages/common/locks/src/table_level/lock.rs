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

use common_catalog::lock_api::LockApi;
use common_catalog::lock_api::LockLevel;
use common_catalog::lock_api::LockRequest;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableLockKey;
use common_meta_kvapi::kvapi::Key;
use common_pipeline_core::lock_guard::LockGuard;

use crate::table_level::CreateTableLockReq;
use crate::table_level::DeleteTableLockReq;
use crate::table_level::ExtendTableLockReq;
use crate::table_level::ListTableLockReq;
use crate::LockManager;

pub struct TableLock {
    lock_mgr: Arc<LockManager>,
    table_info: TableInfo,
}

impl TableLock {
    pub fn create(lock_mgr: Arc<LockManager>, table_info: TableInfo) -> Self {
        TableLock {
            lock_mgr,
            table_info,
        }
    }
}

#[async_trait::async_trait]
impl LockApi for TableLock {
    fn level(&self) -> LockLevel {
        LockLevel::Table
    }

    fn catalog(&self) -> &str {
        self.table_info.catalog()
    }

    fn table_id(&self) -> u64 {
        self.table_info.ident.table_id
    }

    fn watch_delete_key(&self, revision: u64) -> String {
        let lock_key = TableLockKey {
            table_id: self.table_id(),
            revision,
        };
        lock_key.to_string_key()
    }

    fn create_table_lock_req(&self, expire_secs: u64) -> Box<dyn LockRequest> {
        Box::new(CreateTableLockReq {
            table_id: self.table_id(),
            expire_secs,
        })
    }

    fn extend_table_lock_req(&self, expire_secs: u64, revision: u64) -> Box<dyn LockRequest> {
        Box::new(ExtendTableLockReq {
            table_id: self.table_id(),
            expire_secs,
            revision,
        })
    }

    fn delete_table_lock_req(&self, revision: u64) -> Box<dyn LockRequest> {
        Box::new(DeleteTableLockReq {
            table_id: self.table_id(),
            revision,
        })
    }

    fn list_table_lock_req(&self) -> Box<dyn LockRequest> {
        Box::new(ListTableLockReq {
            table_id: self.table_id(),
        })
    }

    async fn try_lock(&self, ctx: Arc<dyn TableContext>) -> Result<Option<LockGuard>> {
        let enabled_table_lock = ctx.get_settings().get_enable_table_lock().unwrap_or(false);
        if enabled_table_lock {
            self.lock_mgr.try_lock(ctx, self).await
        } else {
            Ok(None)
        }
    }
}
