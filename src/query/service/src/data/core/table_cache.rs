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
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

use databend_common_catalog::table::Table;
use parking_lot::Mutex;

/// The tables resolved by data requests, shared by every session of the node, so a
/// request may skip the meta reads of resolving its table: each request decides, by its
/// `data_api_table_cache_ttl_ms`, how old a resolved table it accepts.
///
/// Only the resolution is cached; privileges are checked on every request. A reused table
/// is the table as of its resolution: its snapshot, and the row access and masking
/// policies that [`super::TableTarget`] rejects.
pub struct TableCache {
    capacity: usize,
    entries: Mutex<HashMap<TableKey, Entry>>,
}

/// A table by the names a request gives, in a tenant.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableKey {
    pub tenant: String,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

struct Entry {
    table: Arc<dyn Table>,
    resolved_at: Instant,
}

static INSTANCE: LazyLock<TableCache> = LazyLock::new(|| TableCache::new(4096));

impl TableCache {
    pub fn instance() -> &'static TableCache {
        &INSTANCE
    }

    pub fn new(capacity: usize) -> Self {
        TableCache {
            capacity: capacity.max(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// The table of `key` if resolved within `max_age`.
    pub fn get(&self, key: &TableKey, max_age: Duration) -> Option<Arc<dyn Table>> {
        let entries = self.entries.lock();
        let entry = entries.get(key)?;
        (entry.resolved_at.elapsed() <= max_age).then(|| entry.table.clone())
    }

    /// Records `table` as resolved now. A full cache drops its oldest entry: misses are
    /// rare once the tables of a workload are resolved, so the scan is not on a hot path.
    pub fn insert(&self, key: TableKey, table: Arc<dyn Table>) {
        let mut entries = self.entries.lock();
        if entries.len() >= self.capacity && !entries.contains_key(&key) {
            let oldest = entries
                .iter()
                .min_by_key(|(_, entry)| entry.resolved_at)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                entries.remove(&oldest);
            }
        }
        entries.insert(key, Entry {
            table,
            resolved_at: Instant::now(),
        });
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
