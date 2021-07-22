// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;

use crate::catalogs::catalog::DatabaseMeta;
use crate::catalogs::catalog::TableMeta;
use crate::datasources::Database;
use crate::datasources::RemoteFactory;
use crate::datasources::Table;
use crate::datasources::TableFunction;

pub trait CatalogReader {
    fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseMeta>>;

    fn get_databases(&self) -> Result<Vec<String>>;

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>>;

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<TableMeta>)>>;
}

pub struct OverlaidReader<U, L> {
    upper: Arc<U>,
    lower: Arc<L>,
}

impl<U, L> OverlaidReader<U, L> {
    pub fn new(upper: Arc<U>, lower: Arc<L>) -> Self {
        OverlaidReader { upper, lower }
    }
}

impl<U, L> CatalogReader for OverlaidReader<U, L>
where
    U: CatalogReader,
    L: CatalogReader,
{
    fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseMeta>> {
        // TODO check 'e' is Database not found
        self.upper
            .get_database(db_name)
            .or_else(|_e| self.lower.get_database(db_name))
    }

    fn get_databases(&self) -> Result<Vec<String>> {
        // TODO union
        self.upper
            .get_databases()
            .or_else(|_e| self.lower.get_databases())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>> {
        self.upper
            .get_table(db_name, table_name)
            .or_else(|_e| self.lower.get_table(db_name, table_name))
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<TableMeta>)>> {
        // TODO union (upper shadows lower)
        self.upper
            .get_all_tables()
            .or_else(|_e| self.lower.get_all_tables())
    }
}

pub struct RemoteDbMeta {
    remote_factory: RemoteFactory,
}

impl RemoteDbMeta {
    pub fn new() -> Self {
        todo!()
    }

    pub fn take_current_snapshot(&self) -> Result<Arc<InMemory>> {
        todo!()
    }
}

impl CatalogReader for RemoteDbMeta {
    fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseMeta>> {
        let snap = self.take_current_snapshot()?;
        snap.get_database(db_name)
    }

    fn get_databases(&self) -> Result<Vec<String>> {
        let snap = self.take_current_snapshot()?;
        snap.get_databases()
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>> {
        let snap = self.take_current_snapshot()?;
        snap.get_table(db_name, table_name)
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<TableMeta>)>> {
        let snap = self.take_current_snapshot()?;
        snap.get_all_tables()
    }
}

pub struct InMemory {
    databases: HashMap<String, Arc<DatabaseMeta>>,
}

impl CatalogReader for InMemory {
    fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseMeta>> {
        self.databases
            .get(db_name)
            .map(Clone::clone)
            .ok_or_else(|| ErrorCode::UnknownDatabase(""))
    }

    fn get_databases(&self) -> Result<Vec<String>> {
        Ok(self.databases.keys().map(Clone::clone).collect())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>> {
        let db = self.get_database(db_name)?;
        db.get_table(table_name)
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<TableMeta>)>> {
        todo!()
    }
}
