// Copyright 2021 Datafuse Labs.
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

use bytes::Bytes;
use common_auth::RefreshableToken;
use common_catalog::table::Table;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::SchemaApi;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DropTableByIdReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TruncateTableReply;
use common_meta_app::schema::TruncateTableReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableCopiedFileReply;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_app::share::TableInfoMap;
use common_storage::ShareTableConfig;
use http::header::AUTHORIZATION;
use http::header::CONTENT_LENGTH;
use http::Method;
use http::Request;
use opendal::raw::AsyncBody;
use opendal::raw::HttpClient;

use crate::databases::Database;
use crate::databases::DatabaseContext;

const TENANT_HEADER: &str = "X-DATABEND-TENANT";

// Share Database implementation for `Database` trait.
#[derive(Clone)]
pub struct ShareDatabase {
    ctx: DatabaseContext,

    db_info: DatabaseInfo,

    client: HttpClient,

    token: RefreshableToken,

    endpoint: String,
}

impl ShareDatabase {
    pub const NAME: &'static str = "SHARE";
    pub fn try_create(ctx: DatabaseContext, db_info: DatabaseInfo) -> Result<Box<dyn Database>> {
        let share_endpoint_address = match ShareTableConfig::share_endpoint_address() {
            Some(share_endpoint_address) => share_endpoint_address,
            None => {
                return Err(ErrorCode::EmptyShareEndpointConfig(
                    "EmptyShareEndpointConfig, cannot query share databases".to_string(),
                ));
            }
        };
        let share_name = db_info.meta.from_share.clone().unwrap();
        let endpoint = format!(
            "http://{}/tenant/{}/{}/meta",
            share_endpoint_address, share_name.tenant, share_name.share_name,
        );
        Ok(Box::new(Self {
            ctx,
            db_info,
            client: HttpClient::new()?,
            token: ShareTableConfig::share_endpoint_token(),
            endpoint,
        }))
    }

    fn load_tables(&self, table_infos: Vec<Arc<TableInfo>>) -> Result<Vec<Arc<dyn Table>>> {
        table_infos.iter().try_fold(vec![], |mut acc, item| {
            let tbl = self.get_table_by_info(item.as_ref())?;
            acc.push(tbl);
            Ok(acc)
        })
    }

    // Read table info map from operator
    async fn get_table_info_map(&self, req: Vec<String>) -> Result<TableInfoMap> {
        let bs = Bytes::from(serde_json::to_vec(&req)?);
        let auth = self.token.to_header().await?;
        let requester = GlobalConfig::instance().as_ref().query.tenant_id.clone();
        let req = Request::builder()
            .method(Method::POST)
            .uri(&self.endpoint)
            .header(AUTHORIZATION, auth)
            .header(CONTENT_LENGTH, bs.len())
            .header(TENANT_HEADER, requester)
            .body(AsyncBody::Bytes(bs))?;
        let resp = self.client.send_async(req).await?;
        let bs = resp.into_body().bytes().await?;
        let table_info_map: TableInfoMap = serde_json::from_slice(&bs)?;

        Ok(table_info_map)
    }

    async fn get_table_info(&self, table_name: &str) -> Result<Arc<TableInfo>> {
        let table_info_map = self
            .get_table_info_map(vec![table_name.to_string()])
            .await?;
        match table_info_map.get(table_name) {
            None => Err(ErrorCode::UnknownTable(format!(
                "share table {} is unknown",
                table_name
            ))),
            Some(table_info) => Ok(Arc::new(table_info.clone())),
        }
    }

    async fn list_tables(&self) -> Result<Vec<Arc<TableInfo>>> {
        let table_info_map = self.get_table_info_map(vec![]).await?;
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
    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let table_info = self.get_table_info(table_name).await?;
        self.get_table_by_info(table_info.as_ref())
    }

    async fn list_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        let table_infos = self.list_tables().await?;

        self.load_tables(table_infos)
    }

    async fn list_tables_history(&self) -> Result<Vec<Arc<dyn Table>>> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot list table history from a shared database".to_string(),
        ))
    }

    async fn create_table(&self, _req: CreateTableReq) -> Result<()> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot create table from a shared database".to_string(),
        ))
    }

    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot drop table from a shared database".to_string(),
        ))
    }

    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot undrop table from a shared database".to_string(),
        ))
    }

    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot rename table from a shared database".to_string(),
        ))
    }

    async fn upsert_table_option(
        &self,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot upsert table option from a shared database".to_string(),
        ))
    }

    async fn update_table_meta(&self, _req: UpdateTableMetaReq) -> Result<UpdateTableMetaReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot upsert table meta from a shared database".to_string(),
        ))
    }

    async fn get_table_copied_file_info(
        &self,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        let res = self.ctx.meta.get_table_copied_file_info(req).await?;
        Ok(res)
    }

    async fn upsert_table_copied_file_info(
        &self,
        req: UpsertTableCopiedFileReq,
    ) -> Result<UpsertTableCopiedFileReply> {
        let res = self.ctx.meta.upsert_table_copied_file_info(req).await?;
        Ok(res)
    }

    async fn truncate_table(&self, _req: TruncateTableReq) -> Result<TruncateTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot truncate table from a shared database".to_string(),
        ))
    }
}
