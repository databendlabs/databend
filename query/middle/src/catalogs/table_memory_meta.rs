//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::MetaId;

use crate::storages::Table;

pub struct InMemoryMetas {
    next_id: AtomicU64,
    name_to_table: RwLock<HashMap<String, Arc<dyn Table>>>,
    id_to_table: RwLock<HashMap<MetaId, Arc<dyn Table>>>,
}

impl InMemoryMetas {
    pub fn create(next_id: u64) -> Self {
        InMemoryMetas {
            next_id: AtomicU64::new(next_id),
            name_to_table: RwLock::new(HashMap::default()),
            id_to_table: RwLock::new(HashMap::default()),
        }
    }

    /// Get the next id.
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed);
        self.next_id.load(Ordering::Relaxed) as u64
    }

    pub fn insert(&self, tbl_ref: Arc<dyn Table>) {
        let name = tbl_ref.name().to_owned();
        self.name_to_table.write().insert(name, tbl_ref.clone());
        self.id_to_table.write().insert(tbl_ref.get_id(), tbl_ref);
    }

    pub fn get_by_name(&self, name: &str) -> Option<Arc<dyn Table>> {
        self.name_to_table.read().get(name).cloned()
    }

    pub fn get_by_id(&self, id: &MetaId) -> Option<Arc<dyn Table>> {
        self.id_to_table.read().get(id).cloned()
    }

    pub fn get_all_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        Ok(self.name_to_table.read().values().cloned().collect())
    }
}
