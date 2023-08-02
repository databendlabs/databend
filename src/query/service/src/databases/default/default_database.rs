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

use std::sync::Arc;

use common_catalog::table::Table;
use common_exception::Result;
use common_meta_api::SchemaApi;
use common_meta_app::schema::CreateTableReply;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DropTableByIdReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::GetTableReq;
use common_meta_app::schema::ListTableReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::SetTableColumnMaskPolicyReply;
use common_meta_app::schema::SetTableColumnMaskPolicyReq;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TruncateTableReply;
use common_meta_app::schema::TruncateTableReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;

use crate::databases::Database;
use crate::databases::DatabaseContext;

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
}

#[async_trait::async_trait]
impl Database for DefaultDatabase {
    fn name(&self) -> &str {
        &self.db_info.name_ident.db_name
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let storage = self.ctx.storage_factory.clone();
        storage.get_table(table_info)
    }

    // Get one table by db and table name.
    #[async_backtrace::framed]
    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let table_info = self
            .ctx
            .meta
            .get_table(GetTableReq::new(
                self.get_tenant(),
                self.get_db_name(),
                table_name,
            ))
            .await?;
        self.get_table_by_info(table_info.as_ref())
    }

    #[async_backtrace::framed]
    async fn list_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        let table_infos = self
            .ctx
            .meta
            .list_tables(ListTableReq::new(self.get_tenant(), self.get_db_name()))
            .await?;

        self.load_tables(table_infos)
    }

    #[async_backtrace::framed]
    async fn list_tables_history(&self) -> Result<Vec<Arc<dyn Table>>> {
        // `get_table_history` will not fetch the tables that created before the
        // "metasrv time travel functions" is added.
        // thus, only the table-infos of dropped tables are used.
        let mut dropped = self
            .ctx
            .meta
            .get_table_history(ListTableReq::new(self.get_tenant(), self.get_db_name()))
            .await?
            .into_iter()
            .filter(|i| i.meta.drop_on.is_some())
            .collect::<Vec<_>>();

        let mut table_infos = self
            .ctx
            .meta
            .list_tables(ListTableReq::new(self.get_tenant(), self.get_db_name()))
            .await?;

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
    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply> {
        let res = self.ctx.meta.undrop_table(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply> {
        let res = self.ctx.meta.rename_table(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        let res = self.ctx.meta.upsert_table_option(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn update_table_meta(&self, req: UpdateTableMetaReq) -> Result<UpdateTableMetaReply> {
        let res = self.ctx.meta.update_table_meta(req).await?;
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
    async fn truncate_table(&self, req: TruncateTableReq) -> Result<TruncateTableReply> {
        let res = self.ctx.meta.truncate_table(req).await?;
        Ok(res)
    }
}
