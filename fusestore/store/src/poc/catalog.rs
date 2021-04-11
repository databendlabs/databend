// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use common_infallible::RwLock;

use crate::meta::Catalog;
use crate::meta::DatabaseMeta;
use crate::meta::TableSnapshot;

struct DbTables {
    db_meta: DatabaseMeta,
    tables: HashMap<String, TableSnapshot>,
}

impl DbTables {
    fn new(db_meta: DatabaseMeta) -> Self {
        DbTables {
            db_meta,
            tables: HashMap::new(),
        }
    }
}

/// a file system based Catalog
pub struct LibAlexandria {
    databases: Box<RwLock<HashMap<String, DbTables>>>,
}

impl LibAlexandria {
    pub fn new() -> Self {
        // scan file system & build cache
        todo!()
    }
}

#[async_trait]
impl Catalog for LibAlexandria {
    async fn list_databases(&self) -> Result<Vec<DatabaseMeta>> {
        let res = self
            .databases
            .read()
            .values()
            .map(|dt| dt.db_meta.clone())
            .collect();
        Ok(res)
    }

    async fn list_tables(&self, db_name: &str) -> Result<Vec<TableSnapshot>> {
        Ok(self
            .databases
            .read()
            .get(db_name)
            .map_or_else(|| vec![], |item| item.tables.values().cloned().collect()))
    }

    async fn get_table(&self, db_name: &str, tbl_name: &str) -> Result<Option<TableSnapshot>> {
        Ok(self
            .databases
            .read()
            .get(db_name)
            .map_or_else(|| None, |item| item.tables.get(tbl_name).cloned()))
    }

    async fn get_db(&self, db_name: &str) -> Result<Option<DatabaseMeta>> {
        Ok(self
            .databases
            .read()
            .get(db_name)
            .map(|item| item.db_meta.clone()))
    }

    async fn commit_db_meta(&self, meta: DatabaseMeta) -> Result<()> {
        let mut dbs = self.databases.write();
        if let Some(v) = dbs.get(&meta.name) {
            anyhow::bail!("db exists")
        } else {
            dbs.insert(meta.name.clone(), DbTables::new(meta));
            Ok(())
        }
    }

    async fn commit_table(&self, tbl_snapshot: &TableSnapshot) -> Result<()> {
        let mut dbs = self.databases.write();
        if let Some(v) = dbs.get_mut(&tbl_snapshot.db_name) {
            if let Some(tbl) = v.tables.get_mut(&tbl_snapshot.table_name) {
                if tbl_snapshot.sequence == tbl.sequence + 1 {
                    *tbl = tbl_snapshot.clone();
                    Ok(())
                } else {
                    anyhow::bail!("not consecutive sequence number");
                }
            } else {
                v.tables
                    .insert(tbl_snapshot.table_name.clone(), tbl_snapshot.clone());
                Ok(())
            }
        } else {
            anyhow::bail!("db exists")
        }
    }
}
