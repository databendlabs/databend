// Copyright 2021 Datafuse Labs
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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_meta_api::DatabaseApi;
use databend_common_meta_api::SecurityApi;
use databend_common_meta_api::TableApi;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::ListTableCopiedFileReply;
use databend_common_meta_app::schema::ListTableReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::SwapTableReply;
use databend_common_meta_app::schema::SwapTableReq;
use databend_common_meta_app::schema::TableIdHistoryIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateDatabaseOptionsReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;

use crate::databases::Database;
use crate::databases::DatabaseContext;
use crate::meta_service_error;

#[derive(Clone)]
pub struct DefaultDatabase {
    ctx: DatabaseContext,

    db_info: DatabaseInfo,
}

impl DefaultDatabase {
    pub const NAME: &'static str = "DEFAULT";

    pub fn try_create(ctx: DatabaseContext, db_info: DatabaseInfo) -> Result<Box<dyn Database>> {
        Ok(Box::new(Self { ctx, db_info }))
    }

    fn load_tables(&self, table_infos: Vec<Arc<TableInfo>>) -> Result<Vec<Arc<dyn Table>>> {
        table_infos.iter().try_fold(vec![], |mut acc, item| {
            let tbl = self.get_table_by_info(item.as_ref())?;
            acc.push(tbl);
            Ok(acc)
        })
    }

    async fn list_table_infos(&self) -> Result<Vec<Arc<TableInfo>>> {
        let db_id = self.db_info.database_id;

        let name_id_metas = self
            .ctx
            .meta
            .list_tables(ListTableReq::new(self.get_tenant(), db_id))
            .await?;

        let table_infos = name_id_metas
            .iter()
            .map(|(name, id, meta)| {
                Arc::new(TableInfo {
                    ident: TableIdent {
                        table_id: id.table_id,
                        seq: meta.seq,
                    },
                    desc: format!("'{}'.'{}'", self.get_db_name(), name),
                    name: name.to_string(),
                    meta: meta.data.clone(),
                    db_type: DatabaseType::NormalDB,
                    catalog_info: Default::default(),
                })
            })
            .collect::<Vec<_>>();

        Ok(table_infos)
    }
}
#[async_trait::async_trait]
impl Database for DefaultDatabase {
    fn name(&self) -> &str {
        self.db_info.name_ident.database_name()
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let storage = &self.ctx.storage_factory;
        storage.get_table(table_info, self.ctx.disable_table_info_refresh)
    }

    // Get one table by db and table name.
    #[async_backtrace::framed]
    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let name_ident = DBIdTableName::new(self.get_db_info().database_id.db_id, table_name);
        let table_niv = self
            .ctx
            .meta
            .get_table_in_db(&name_ident)
            .await
            .map_err(meta_service_error)?;

        let Some(table_niv) = table_niv else {
            return Err(AppError::from(UnknownTable::new(
                table_name,
                format!("get_table: '{}'.'{}'", self.get_db_name(), table_name),
            ))
            .into());
        };

        let (_name, id, seq_meta) = table_niv.unpack();

        let table_info = TableInfo {
            ident: TableIdent {
                table_id: id.table_id,
                seq: seq_meta.seq,
            },
            desc: format!("'{}'.'{}'", self.get_db_name(), table_name),
            name: table_name.to_string(),
            meta: seq_meta.data,
            db_type: DatabaseType::NormalDB,
            catalog_info: Default::default(),
        };

        let table_info = Arc::new(table_info);

        self.get_table_by_info(table_info.as_ref())
    }

    #[async_backtrace::framed]
    async fn get_table_history(&self, table_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let metas = self
            .ctx
            .meta
            .get_retainable_tables(&TableIdHistoryIdent {
                database_id: self.db_info.database_id.db_id,
                table_name: table_name.to_string(),
            })
            .await
            .map_err(meta_service_error)?;

        let table_infos: Vec<Arc<TableInfo>> = metas
            .into_iter()
            .map(|(table_id, seqv)| {
                Arc::new(TableInfo {
                    ident: TableIdent {
                        table_id: table_id.table_id,
                        seq: seqv.seq,
                    },
                    desc: format!(
                        "'{}'.'{}'",
                        self.db_info.name_ident.database_name(),
                        table_name
                    ),
                    name: table_name.to_string(),
                    meta: seqv.data,
                    db_type: DatabaseType::NormalDB,
                    catalog_info: Default::default(),
                })
            })
            .collect();

        // disable refresh in history table
        self.load_tables(table_infos)
    }

    #[async_backtrace::framed]
    async fn list_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        let table_infos = self.list_table_infos().await?;
        self.load_tables(table_infos)
    }

    #[async_backtrace::framed]
    async fn mget_tables(&self, table_names: &[String]) -> Result<Vec<Arc<dyn Table>>> {
        if table_names.is_empty() {
            return Ok(vec![]);
        }

        let db_id = self.db_info.database_id.db_id;
        let db_name = self.get_db_name();

        // Batch get table infos from meta
        let table_infos = self
            .ctx
            .meta
            .mget_tables(db_id, db_name, table_names)
            .await?;

        self.load_tables(table_infos)
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        include_non_retainable: bool,
    ) -> Result<Vec<Arc<dyn Table>>> {
        // `get_table_history` will not fetch the tables that created before the
        // "metasrv time travel functions" is added.
        // thus, only the table-infos of dropped tables are used.
        //
        // For dropped tables, we do not bother refreshing the table info.
        let mut dropped = self
            .ctx
            .meta
            .list_history_tables(
                include_non_retainable,
                ListTableReq::new(self.get_tenant(), self.db_info.database_id),
            )
            .await?
            .into_iter()
            .filter(|i| i.value().drop_on.is_some())
            .map(|niv| {
                Arc::new(TableInfo::new_full(
                    self.get_db_name(),
                    &niv.name().table_name,
                    TableIdent::new(niv.id().table_id, niv.value().seq),
                    niv.value().data.clone(),
                    Arc::new(CatalogInfo::default()),
                    DatabaseType::NormalDB,
                ))
            })
            .collect::<Vec<_>>();

        let mut table_infos = self.list_table_infos().await?;

        table_infos.append(&mut dropped);

        self.load_tables(table_infos)
    }

    #[async_backtrace::framed]
    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply> {
        let res = self.ctx.meta.create_table(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply> {
        let res = self.ctx.meta.drop_table_by_id(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, req: UndropTableReq) -> Result<()> {
        let res = self.ctx.meta.undrop_table(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn commit_table_meta(&self, req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        let res = self.ctx.meta.commit_table_meta(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply> {
        let res = self.ctx.meta.rename_table(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn swap_table(&self, req: SwapTableReq) -> Result<SwapTableReply> {
        let res = self.ctx.meta.swap_table(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn update_options(
        &self,
        expected_meta_seq: u64,
        options: BTreeMap<String, String>,
    ) -> Result<()> {
        let req = UpdateDatabaseOptionsReq {
            db_id: self.db_info.database_id.db_id,
            expected_meta_seq,
            options,
        };
        self.ctx.meta.update_database_options(req).await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        let res = self.ctx.meta.upsert_table_option(req).await?;
        Ok(res)
    }

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        let res = self.ctx.meta.set_table_column_mask_policy(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        let res = self.ctx.meta.get_table_copied_file_info(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn list_table_copied_file_info(&self, table_id: u64) -> Result<ListTableCopiedFileReply> {
        let res = self
            .ctx
            .meta
            .list_table_copied_file_info(table_id)
            .await
            .map_err(meta_service_error)?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn truncate_table(&self, req: TruncateTableReq) -> Result<TruncateTableReply> {
        let res = self.ctx.meta.truncate_table(req).await?;
        Ok(res)
    }
}
