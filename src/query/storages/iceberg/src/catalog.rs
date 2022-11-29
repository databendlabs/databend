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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::catalog::Catalog;
use common_catalog::catalog::StorageDescription;
use common_catalog::database::Database;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_function::TableFunction;
use common_exception::Result;
use common_meta_app::schema::CountTablesReply;
use common_meta_app::schema::CountTablesReq;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::DropTableReq;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::RenameDatabaseReply;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TruncateTableReply;
use common_meta_app::schema::TruncateTableReq;
use common_meta_app::schema::UndropDatabaseReply;
use common_meta_app::schema::UndropDatabaseReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableCopiedFileReply;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_types::MetaId;
use common_storage::DataOperator;
use futures::TryStreamExt;

use crate::database::IcebergDatabase;
use crate::table::IcebergTable;

pub const ICEBERG_CATALOG: &str = "iceberg";

#[derive(Clone)]
/// `Catalog` for a external iceberg storage
/// - Metadata of databases are saved in meta store
/// - Instances of `Database` are created from reading subdirectories of
///    Iceberg table
/// - Table metadata are saved in external Iceberg storage
pub struct IcebergCatalog {
    /// name of this iceberg table
    name: String,
    /// underlying storage access operator
    operator: Arc<DataOperator>,
}

impl IcebergCatalog {
    /// create a new iceberg catalog from the endpoint_address
    ///
    /// # NOTE:
    /// endpoint_url should be set as in `Stage`s.
    /// For example, to create a iceberg catalog on S3, the endpoint_url should be:
    ///
    /// `s3://bucket_name/path/to/iceberg_catalog`
    pub fn try_create(name: &str, operator: DataOperator) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            operator: Arc::new(operator),
        })
    }

    /// list read databases
    pub async fn list_database_from_read(&self, tenant: &str) -> Result<Vec<Arc<dyn Database>>> {
        let root = self.operator.object("/");
        let mut dbs = vec![];
        let mut ls = root.list().await?;
        while let Some(dir) = ls.try_next().await? {
            let db: Arc<dyn Database> = Arc::new(IcebergDatabase::create_database_from_read(
                dir.name(),
                tenant,
            ));
            dbs.push(db);
        }
        Ok(dbs)
    }

    /// get table from iceberg storage
    pub async fn get_table(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        IcebergTable::try_create_table_from_read(
            &self.name,
            tenant,
            db_name,
            table_name,
            self.operator.clone(),
        )
        .await
        .map(|tbl| {
            let tbl: Arc<dyn Table> = Arc::new(tbl);
            tbl
        })
    }
}

#[async_trait]
impl Catalog for IcebergCatalog {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_database(&self, tenant: &str, db_name: &str) -> Result<Arc<dyn Database>> {
        Ok(Arc::new(IcebergDatabase::create_database_from_read(
            db_name, tenant,
        )))
    }

    async fn list_databases(&self, tenant: &str) -> Result<Vec<Arc<dyn Database>>> {
        self.list_database_from_read(tenant).await
    }

    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        unimplemented!()
    }

    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<()> {
        unimplemented!()
    }

    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        unimplemented!()
    }

    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        unimplemented!()
    }

    fn get_table_by_info(&self, _table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        unimplemented!()
    }

    async fn get_table_meta_by_id(
        &self,
        _table_id: MetaId,
    ) -> Result<(TableIdent, Arc<TableMeta>)> {
        unimplemented!()
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn get_table(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let tbl: Arc<dyn Table> = Arc::new(
            IcebergTable::try_create_table_from_read(
                &self.name,
                tenant,
                db_name,
                table_name,
                self.operator.clone(),
            )
            .await?,
        );
        Ok(tbl)
    }

    async fn list_tables(&self, tenant: &str, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let db_op = self.operator.object(db_name);
        let mut ls = db_op.list().await?;
        let mut tbls = vec![];
        while let Some(tbl) = ls.try_next().await? {
            let tbl_name = tbl.name();
            let tbl = IcebergTable::try_create_table_from_read(
                &self.name,
                tenant,
                db_name,
                tbl_name,
                self.operator.clone(),
            )
            .await?;

            let tbl: Arc<dyn Table> = Arc::new(tbl);
            tbls.push(tbl);
        }
        Ok(tbls)
    }

    async fn list_tables_history(
        &self,
        _tenant: &str,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        unimplemented!()
    }

    async fn create_table(&self, _req: CreateTableReq) -> Result<()> {
        unimplemented!()
    }

    async fn drop_table(&self, _req: DropTableReq) -> Result<DropTableReply> {
        unimplemented!()
    }

    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        unimplemented!()
    }

    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        unimplemented!()
    }

    async fn exists_table(&self, tenant: &str, db_name: &str, table_name: &str) -> Result<bool> {
        let tbl = IcebergTable::try_create_table_from_read(
            &self.name,
            tenant,
            db_name,
            table_name,
            self.operator.clone(),
        )
        .await;

        match tbl {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn upsert_table_option(
        &self,
        _tenant: &str,
        _db_name: &str,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        unimplemented!()
    }

    async fn update_table_meta(
        &self,
        _tenant: &str,
        _db_name: &str,
        _req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply> {
        unimplemented!()
    }

    async fn count_tables(&self, _req: CountTablesReq) -> Result<CountTablesReply> {
        unimplemented!()
    }

    async fn get_table_copied_file_info(
        &self,
        _tenant: &str,
        _db_name: &str,
        _req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        unimplemented!()
    }

    async fn upsert_table_copied_file_info(
        &self,
        _tenant: &str,
        _db_name: &str,
        _req: UpsertTableCopiedFileReq,
    ) -> Result<UpsertTableCopiedFileReply> {
        unimplemented!()
    }

    async fn truncate_table(
        &self,
        _tenant: &str,
        _db_name: &str,
        _req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        unimplemented!()
    }
}
