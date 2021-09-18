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

use common_metatypes::MetaId;

use crate::catalogs::TableMeta;

pub struct InMemoryMetas {
    pub(crate) name2meta: HashMap<String, Arc<TableMeta>>,
    pub(crate) id2meta: HashMap<MetaId, Arc<TableMeta>>,
}

impl InMemoryMetas {
    pub fn create() -> Self {
        InMemoryMetas {
            name2meta: HashMap::default(),
            id2meta: HashMap::default(),
        }
    }

    pub fn insert(&mut self, tbl_meta: TableMeta) {
        let met_ref = Arc::new(tbl_meta);
        let name = met_ref.raw().name().to_owned();
        self.name2meta
            .insert(met_ref.raw().name().to_owned(), met_ref.clone());
        self.id2meta.insert(met_ref.meta_id(), met_ref);
        self.get_by_name(&name);
    }

    pub fn get_by_name(&self, name: &str) -> Option<Arc<TableMeta>> {
        let res = self.name2meta.get(name).cloned();
        res
    }

    pub fn get_by_id(&self, id: &MetaId) -> Option<Arc<TableMeta>> {
        self.id2meta.get(id).cloned()
    }
}
