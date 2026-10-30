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
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::time::Instant;

use databend_common_catalog::table::Table;

use super::size::table_schema;
use crate::CachedItem;
use crate::TTLValue;

pub struct IcebergTableCacheValue {
    table: Arc<dyn Table>,
    refreshing: AtomicBool,
    loaded_at: Instant,
    credential_refresh_at: Option<Instant>,
}

impl IcebergTableCacheValue {
    pub fn new(table: Arc<dyn Table>, credential_refresh_at: Option<Instant>) -> Self {
        Self {
            table,
            refreshing: AtomicBool::new(false),
            loaded_at: Instant::now(),
            credential_refresh_at,
        }
    }

    pub fn table(&self) -> Arc<dyn Table> {
        self.table.clone()
    }

    pub(crate) fn loaded_at(&self) -> Instant {
        self.loaded_at
    }

    pub(crate) fn credential_refresh_at(&self) -> Option<Instant> {
        self.credential_refresh_at
    }

    pub(crate) fn is_refreshing(&self) -> bool {
        self.refreshing.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn set_refreshing(&self) {
        self.refreshing
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// The table is opaque `dyn Table`; its schema is the part that grows with the table.
impl CachedItem for IcebergTableCacheValue {
    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + 2 * table_schema(&self.table().get_table_info().meta.schema)
    }
}

impl TTLValue for IcebergTableCacheValue {
    fn is_expired(&self, ttl: &Duration) -> bool {
        Instant::now() > self.loaded_at() + *ttl || self.requires_sync_refresh()
    }

    fn requires_sync_refresh(&self) -> bool {
        self.credential_refresh_at()
            .is_some_and(|refresh_at| Instant::now() >= refresh_at)
    }

    fn is_refreshing(&self) -> bool {
        self.is_refreshing()
    }

    fn set_refreshing(&self) {
        self.set_refreshing();
    }
}
