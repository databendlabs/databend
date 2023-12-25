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
use databend_common_meta_app::schema::LockKey;
use databend_common_meta_app::schema::LockType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableLockKey;
use databend_common_meta_kvapi::kvapi::Key;
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

    fn gen_lock_key(&self) -> LockKey {
        LockKey::Table {
            table_id: self.table_info.ident.table_id,
        }
    }

    fn get_catalog(&self) -> &str {
        self.table_info.catalog()
    }

    fn get_table_id(&self) -> u64 {
        self.table_info.ident.table_id
    }

    fn watch_delete_key(&self, revision: u64) -> String {
        let lock_key = TableLockKey {
            table_id: self.table_info.ident.table_id,
            revision,
        };
        lock_key.to_string_key()
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
