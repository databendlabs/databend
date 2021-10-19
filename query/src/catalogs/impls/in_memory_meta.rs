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
use std::sync::Arc;

use common_meta_types::MetaId;

use crate::catalogs::Table;

pub struct InMemoryMetas {
    pub(crate) name_to_table: HashMap<String, Arc<dyn Table>>,
    pub(crate) id_to_table: HashMap<MetaId, Arc<dyn Table>>,
}

impl InMemoryMetas {
    pub fn create() -> Self {
        InMemoryMetas {
            name_to_table: HashMap::default(),
            id_to_table: HashMap::default(),
        }
    }

    pub fn insert(&mut self, tbl_ref: Arc<dyn Table>) {
        let name = tbl_ref.name().to_owned();
        self.name_to_table.insert(name, tbl_ref.clone());
        self.id_to_table.insert(tbl_ref.get_id(), tbl_ref);
    }

    pub fn get_by_name(&self, name: &str) -> Option<Arc<dyn Table>> {
        self.name_to_table.get(name).cloned()
    }

    pub fn get_by_id(&self, id: &MetaId) -> Option<Arc<dyn Table>> {
        self.id_to_table.get(id).cloned()
    }
}
