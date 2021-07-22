// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::CreateDatabasePlan;
use common_planners::DatabaseEngineType;
use common_planners::DropDatabasePlan;
use common_store_api::MetaApi;

use crate::catalogs::versioned::VersionedTabImpl;
use crate::catalogs::versioned::VersionedTable;
use crate::configs::Config;
use crate::datasources::local::LocalDatabase;
use crate::datasources::local::LocalFactory;
use crate::datasources::remote::RemoteDatabase;
use crate::datasources::remote::RemoteFactory;
use crate::datasources::system::SystemFactory;
use crate::datasources::DataSource;
use crate::datasources::Database;
use crate::datasources::Table;
use crate::datasources::TableFunction;

/// For SystemDB and Local DBs
pub struct LocalCatalog {
    // We keep datasource as it is, and transform it as a catalog
    datasource: DataSource,
}

impl LocalCatalog {
    pub fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn VersionedTable>> {
        let tbl = self.datasource.get_table(db_name, table_name)?;
        let r = VersionedTabImpl::new(1, 1, tbl);
        Ok(Arc::new(r))
    }
}

struct RemoteMetaSynchronizer {}

struct DatabaseMeta;

impl RemoteMetaSynchronizer {
    fn getRemoteDbMea() -> DatabaseMeta {
        todo!()
    }
}
