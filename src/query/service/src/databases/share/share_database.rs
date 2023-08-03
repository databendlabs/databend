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

use std::str;
use std::sync::Arc;

use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::SchemaApi;
use common_meta_app::schema::CreateTableReply;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DropTableByIdReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
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
use common_sharing::ShareEndpointManager;

use crate::databases::Database;
use crate::databases::DatabaseContext;

// Share Database implementation for `Database` trait.
#[derive(Clone)]
pub struct ShareDatabase {
    ctx: DatabaseContext,

    db_info: DatabaseInfo,
}

impl ShareDatabase {
    pub const NAME: &'static str = "SHARE";
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

    #[async_backtrace::framed]
    async fn get_table_info(&self, table_name: &str) -> Result<Arc<TableInfo>> {
        let table_info_map = ShareEndpointManager::instance()
            .get_table_info_map(&self.ctx.tenant, &self.db_info, vec![
                table_name.to_string(),
            ])
            .await?;
        match table_info_map.get(table_name) {
            None => Err(ErrorCode::UnknownTable(format!(
                "share table {} is unknown",
                table_name
            ))),
            Some(table_info) => Ok(Arc::new(table_info.clone())),
        }
    }

    #[async_backtrace::framed]
    async fn list_tables(&self) -> Result<Vec<Arc<TableInfo>>> {
        let table_info_map = ShareEndpointManager::instance()
            .get_table_info_map(&self.ctx.tenant, &self.db_info, vec![])
            .await?;
        let table_infos: Vec<Arc<TableInfo>> = table_info_map
            .values()
            .map(|table_info| Arc::new(table_info.to_owned()))
            .collect();
        Ok(table_infos)
    }
}

#[async_trait::async_trait]
impl Database for ShareDatabase {
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
        let table_info = self.get_table_info(table_name).await?;
        self.get_table_by_info(table_info.as_ref())
    }

    #[async_backtrace::framed]
    async fn list_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        let table_infos = self.list_tables().await?;

        self.load_tables(table_infos)
    }

    #[async_backtrace::framed]
    async fn list_tables_history(&self) -> Result<Vec<Arc<dyn Table>>> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot list table history from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot create table from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot drop table from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot undrop table from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot rename table from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot upsert table option from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn update_table_meta(&self, _req: UpdateTableMetaReq) -> Result<UpdateTableMetaReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot upsert table meta from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot set_table_column_mask_policy from a shared database"
                .to_string(),
        ))
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
    async fn truncate_table(&self, _req: TruncateTableReq) -> Result<TruncateTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot truncate table from a shared database".to_string(),
        ))
    }
}
