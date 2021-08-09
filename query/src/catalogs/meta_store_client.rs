// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_datavalues::prelude::Arc;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::catalogs::utils::TableMeta;
use crate::datasources::Database;

// Client of database meta store (the data dictionary)
//
// Mixing async and sync functions are NOT a good idea, we keep
// the interface as it was for minimizing the impacts in this iteration.
// It should be unified in coming refactorings.
#[async_trait::async_trait]
pub trait DBMetaStoreClient: Send + Sync {
    fn get_database(&self, db_name: &str) -> common_exception::Result<Arc<dyn Database>>;
    fn get_databases(&self) -> common_exception::Result<Vec<String>>;
    fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<TableMeta>>;
    fn get_all_tables(&self) -> common_exception::Result<Vec<(String, Arc<TableMeta>)>>;
    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> common_exception::Result<Arc<TableMeta>>;

    fn get_db_tables(&self, db_name: &str) -> common_exception::Result<Vec<Arc<TableMeta>>>;
    async fn create_table(&self, plan: CreateTablePlan) -> common_exception::Result<()>;
    async fn drop_table(&self, plan: DropTablePlan) -> common_exception::Result<()>;
}
