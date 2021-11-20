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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateTableReq;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use dyn_clone::DynClone;

use crate::catalogs::Database;
use crate::catalogs::Table;
use crate::catalogs::TableFunction;
use crate::datasources::table_func_engine::TableArgs;

/// Catalog is the global view of all the databases of the user.
/// The global view has many engine type: Local-Database(engine=Local), Remote-Database(engine=Remote)
/// or others(like MySQL-Database, engine=MySQL)
/// When we create a new database, we first to get the engine from the registered engines,
/// and use the engine to create them.
#[async_trait::async_trait]
pub trait Catalog: DynClone + Send + Sync {
    // Get all the databases.
    async fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>>;

    // Get the database by name.
    async fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>>;

    // Get one table by db and table name.
    async fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>>;

    async fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<dyn Table>>>;

    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)>;

    async fn create_table(&self, req: CreateTableReq) -> Result<()>;

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply>;

    // Check a db.table is exists or not.
    async fn exists_table(&self, db_name: &str, table_name: &str) -> Result<bool> {
        match self.get_table(db_name, table_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UnknownTableCode() {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Build a `Arc<dyn Table>` from `TableInfo`.
    fn build_table(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>>;

    // Get function by name.
    fn get_table_function(
        &self,
        _func_name: &str,
        _tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        unimplemented!()
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> common_exception::Result<UpsertTableOptionReply>;

    // Operation with database.
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply>;

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<()>;

    async fn exists_database(&self, db_name: &str) -> Result<bool> {
        match self.get_database(db_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UnknownDatabaseCode() {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }
}
