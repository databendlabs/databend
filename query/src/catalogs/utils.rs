// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::collections::HashMap;

use common_datavalues::prelude::Arc;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;

use crate::datasources::Table;
use crate::datasources::TableFunction;

pub type TableMeta = DatasourceMeta<Arc<dyn Table>>;
pub type TableFunctionMeta = DatasourceMeta<Arc<dyn TableFunction>>;

#[derive(Debug)]
pub struct DatasourceMeta<T> {
    datasource: T,
    id: MetaId,
    version: Option<MetaVersion>,
}

impl<T> DatasourceMeta<T> {
    pub fn new(datasource: T, id: MetaId) -> Self {
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
    pub fn new() -> Self {
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
