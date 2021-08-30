// Copyright 2020 Datafuse Labs.
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
//

use std::collections::HashMap;
use std::sync::Arc;

use common_metatypes::MetaId;

use crate::catalogs::Meta;
use crate::catalogs::Table;
use crate::catalogs::TableFunction;

pub type TableMeta = Meta<Arc<dyn Table>>;
pub type TableFunctionMeta = Meta<Arc<dyn TableFunction>>;

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
        self.name2meta
            .insert(met_ref.raw().name().to_owned(), met_ref.clone());
        self.id2meta.insert(met_ref.meta_id(), met_ref);
    }
}
