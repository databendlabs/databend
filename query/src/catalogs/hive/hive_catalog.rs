// Copyright 2022 Datafuse Labs.
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
use common_hive_meta_store::TThriftHiveMetastoreSyncClient;
use common_hive_meta_store::ThriftHiveMetastoreSyncClient;
use common_meta_app::schema::CountTablesReply;
use common_meta_app::schema::CountTablesReq;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::DropTableReq;
use common_meta_app::schema::RenameDatabaseReply;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::UndropDatabaseReply;
use common_meta_app::schema::UndropDatabaseReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_types::*;
use thrift::protocol::*;
use thrift::transport::*;

use super::hive_database::HiveDatabase;
use crate::catalogs::hive::HiveTable;
use crate::catalogs::Catalog;
use crate::databases::Database;
use crate::storages::StorageDescription;
use crate::storages::Table;
use crate::table_functions::TableArgs;
use crate::table_functions::TableFunction;

#[derive(Clone)]
pub struct HiveCatalog {
    /// address of hive meta store service
    client_address: String,
}

impl HiveCatalog {
    pub fn try_create(hms_address: impl Into<String>) -> Result<HiveCatalog> {
        Ok(HiveCatalog {
            client_address: hms_address.into(),
        })
    }

    pub fn get_client(&self) -> Result<impl TThriftHiveMetastoreSyncClient> {
        let mut c = TTcpChannel::new();
        c.open(self.client_address.as_str())
            .map_err(from_thrift_error)?;
        let (i_chan, o_chan) = c.split().map_err(from_thrift_error)?;
        let i_tran = TBufferedReadTransport::new(i_chan);
        let o_tran = TBufferedWriteTransport::new(o_chan);
        let i_prot = TBinaryInputProtocol::new(i_tran, true);
        let o_prot = TBinaryOutputProtocol::new(o_tran, true);
        Ok(ThriftHiveMetastoreSyncClient::new(i_prot, o_prot))
    }
}

fn from_thrift_error(error: thrift::Error) -> ErrorCode {
    ErrorCode::from_std_error(error)
}

#[async_trait::async_trait]
impl Catalog for HiveCatalog {
    async fn get_database(&self, _tenant: &str, db_name: &str) -> Result<Arc<dyn Database>> {
        let thrift_db_meta = self
            .get_client()?
            .get_database(db_name.to_owned())
            .map_err(from_thrift_error)?;
        let hive_database: HiveDatabase = thrift_db_meta.into();
        let res: Arc<dyn Database> = Arc::new(hive_database);
        Ok(res)
    }

    // Get all the databases.
    async fn list_databases(&self, _tenant: &str) -> Result<Vec<Arc<dyn Database>>> {
        todo!()
    }

    // Operation with database.
    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        Err(ErrorCode::UnImplement(
            "Cannot create database in HIVE catalog",
        ))
    }

    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<()> {
        Err(ErrorCode::UnImplement(
            "Cannot drop database in HIVE catalog",
        ))
    }

    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        Err(ErrorCode::UnImplement(
            "Cannot undrop database in HIVE catalog",
        ))
    }

    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        Err(ErrorCode::UnImplement(
            "Cannot rename database in HIVE catalog",
        ))
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let res: Arc<dyn Table> = Arc::new(HiveTable::try_create(table_info.clone())?);
        Ok(res)
    }

    async fn get_table_meta_by_id(
        &self,
        _table_id: MetaId,
    ) -> Result<(TableIdent, Arc<TableMeta>)> {
        Err(ErrorCode::UnImplement(
            "Cannot get table by id in HIVE catalog",
        ))
    }

    // Get one table by db and table name.
    async fn get_table(
        &self,
        _tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let mut client = self.get_client()?;
        let table_meta = client
            .get_table(db_name.to_owned(), table_name.to_owned())
            .map_err(from_thrift_error)?;
        let fields = client
            .get_schema(db_name.to_owned(), table_name.to_owned())
            .map_err(from_thrift_error)?;
        let table_info: TableInfo = super::converters::try_into_table_info(table_meta, fields)?;
        let res: Arc<dyn Table> = Arc::new(HiveTable::try_create(table_info)?);
        Ok(res)
    }

    async fn list_tables(&self, _tenant: &str, _db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        todo!()
    }

    async fn list_tables_history(
        &self,
        _tenant: &str,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        Err(ErrorCode::UnImplement(
            "Cannot list table history in HIVE catalog",
        ))
    }

    async fn create_table(&self, _req: CreateTableReq) -> Result<()> {
        Err(ErrorCode::UnImplement(
            "Cannot create table in HIVE catalog",
        ))
    }

    async fn drop_table(&self, _req: DropTableReq) -> Result<DropTableReply> {
        Err(ErrorCode::UnImplement("Cannot drop table in HIVE catalog"))
    }

    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        Err(ErrorCode::UnImplement(
            "Cannot undrop table in HIVE catalog",
        ))
    }

    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(ErrorCode::UnImplement(
            "Cannot rename table in HIVE catalog",
        ))
    }

    // Check a db.table is exists or not.
    async fn exists_table(&self, tenant: &str, db_name: &str, table_name: &str) -> Result<bool> {
        // TODO refine this
        match self.get_table(tenant, db_name, table_name).await {
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

    async fn upsert_table_option(
        &self,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::UnImplement(
            "Cannot upsert table option in HIVE catalog",
        ))
    }

    async fn update_table_meta(&self, _req: UpdateTableMetaReq) -> Result<UpdateTableMetaReply> {
        Err(ErrorCode::UnImplement(
            "Cannot update table meta in HIVE catalog",
        ))
    }

    async fn count_tables(&self, _req: CountTablesReq) -> Result<CountTablesReply> {
        unimplemented!()
    }

    /// Table function

    // Get function by name.
    fn get_table_function(
        &self,
        _func_name: &str,
        _tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        unimplemented!()
    }

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        unimplemented!()
    }
}
