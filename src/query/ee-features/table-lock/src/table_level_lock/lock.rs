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

use common_meta_app::schema::TableLockKey;
use common_meta_kvapi::kvapi::Key;
use common_pipeline_core::table_lock::TableLockReq;
use common_pipeline_core::TableLock;

use crate::table_level_lock::req::*;
use crate::TableLockManagerWrapper;

pub struct TableLevelLock {
    lock_mgr: Arc<TableLockManagerWrapper>,
    table_id: u64,
    revision: u64,
}

impl TableLevelLock {
    pub fn create(lock_mgr: Arc<TableLockManagerWrapper>, table_id: u64) -> Self {
        TableLevelLock {
            lock_mgr,
            table_id,
            revision: 0,
        }
    }
}

#[async_trait::async_trait]
impl TableLock for TableLevelLock {
    fn set_revision(&mut self, revision: u64) {
        self.revision = revision;
    }

    fn revision(&self) -> u64 {
        self.revision
    }

    fn watch_delete_key(&self, revision: u64) -> String {
        let lock_key = TableLockKey {
            table_id: self.table_id,
            revision,
        };
        lock_key.to_string_key()
    }

    fn create_table_lock_req(&self, expire_secs: u64) -> Box<dyn TableLockReq> {
        Box::new(CreateTableLockReq {
            table_id: self.table_id,
            expire_secs,
        })
    }

    fn extend_table_lock_req(&self, expire_secs: u64) -> Box<dyn TableLockReq> {
        Box::new(ExtendTableLockReq {
            table_id: self.table_id,
            expire_secs,
            revision: self.revision,
        })
    }

    fn delete_table_lock_req(&self) -> Box<dyn TableLockReq> {
        Box::new(DeleteTableLockReq {
            table_id: self.table_id,
            revision: self.revision,
        })
    }

    fn list_table_lock_req(&self) -> Box<dyn TableLockReq> {
        Box::new(ListTableLockReq {
            table_id: self.table_id,
        })
    }
}

impl Drop for TableLevelLock {
    fn drop(&mut self) {
        let revision = self.revision();
        if revision > 0 {
            self.lock_mgr.unlock(revision);
        }
    }
}
