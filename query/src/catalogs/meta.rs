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
use common_metatypes::MetaVersion;

use crate::catalogs::Table;
use crate::catalogs::TableFunction;

pub type TableMeta = DatasourceMeta<Arc<dyn Table>>;
pub type TableFunctionMeta = DatasourceMeta<Arc<dyn TableFunction>>;

#[derive(Debug)]
pub struct DatasourceMeta<T> {
    datasource: T,
    id: MetaId,
    version: Option<MetaVersion>,
}

impl<T> DatasourceMeta<T> {
    pub fn create(datasource: T, id: MetaId) -> Self {
        Self::with_version(datasource, id, None)
    }

    pub fn with_version(datasource: T, id: MetaId, version: Option<MetaVersion>) -> Self {
        Self {
            datasource,
            id,
            version,
        }
    }

    pub fn meta_ver(&self) -> Option<MetaVersion> {
        self.version
    }

    pub fn meta_id(&self) -> MetaId {
        self.id
    }

    pub fn datasource(&self) -> &T {
        &self.datasource
    }
}

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
            .insert(met_ref.datasource().name().to_owned(), met_ref.clone());
        self.id2meta.insert(met_ref.meta_id(), met_ref);
    }
}
