// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_datavalues::prelude::Arc;

use crate::datasources::Table;
use crate::datasources::TableFunction;

pub type MetaId = u64;
pub type MetaVersion = u64;

pub type TableMeta = DatasourceWrapper<Arc<dyn Table>>;
pub type TableFunctionMeta = DatasourceWrapper<Arc<dyn TableFunction>>;

pub struct DatasourceWrapper<T> {
    inner: T,
    id: MetaId,
    version: Option<MetaVersion>,
}

impl<T> DatasourceWrapper<T> {
    pub fn new(inner: T, id: MetaId) -> Arc<DatasourceWrapper<T>> {
        Arc::new(DatasourceWrapper {
            inner,
            id,
            version: None,
        })
    }
}

impl<T> DatasourceWrapper<T> {
    pub fn get_id(&self) -> MetaId {
        self.id
    }

    pub fn get_version(&self) -> Option<MetaVersion> {
        self.version
    }

    pub fn get_inner(&self) -> &T {
        &self.inner
    }
}
