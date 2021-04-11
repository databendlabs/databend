// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use async_trait::async_trait;

use crate::meta::table_meta::TableSnapshot;
use crate::meta::DatabaseMeta;

//TODO refine result type; we might need `this-error` at this layer
#[async_trait]
pub trait Catalog: Send + Sync {
    async fn list_databases(&self) -> Result<Vec<DatabaseMeta>>;
    async fn list_tables(&self, db_name: &str) -> Result<Vec<TableSnapshot>>;
    async fn get_table(&self, db_name: &str, tbl_name: &str) -> Result<Option<TableSnapshot>>;
    async fn get_db(&self, db_name: &str) -> Result<Option<DatabaseMeta>>;

    async fn commit_db_meta(&self, db_name: DatabaseMeta) -> Result<()>;

    async fn commit_table(&self, tbl_snapshot: &TableSnapshot) -> Result<()>;
}
