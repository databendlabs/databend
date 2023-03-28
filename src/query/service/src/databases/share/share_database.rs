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
use std::sync::Mutex;

use bytes::Bytes;
use common_auth::RefreshableToken;
use common_catalog::table::Table;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::SchemaApi;
use common_meta_api::ShareApi;
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
use common_meta_app::share::GetShareEndpointReq;
use common_meta_app::share::TableInfoMap;
use common_users::UserApiProvider;
use http::header::AUTHORIZATION;
use http::header::CONTENT_LENGTH;
use http::Method;
use http::Request;
use opendal::raw::AsyncBody;
use opendal::raw::HttpClient;
use tracing::error;
use tracing::info;

use crate::databases::Database;
use crate::databases::DatabaseContext;

const TENANT_HEADER: &str = "X-DATABEND-TENANT";

#[derive(Clone, Debug)]
struct EndpointConfig {
    pub url: String,

    pub token: RefreshableToken,
}

// Share Database implementation for `Database` trait.
#[derive(Clone)]
pub struct ShareDatabase {
    ctx: DatabaseContext,

    db_info: DatabaseInfo,

    client: HttpClient,

    endpoint_config: Arc<Mutex<Option<EndpointConfig>>>,
}

impl ShareDatabase {
    pub const NAME: &'static str = "SHARE";
    pub fn try_create(ctx: DatabaseContext, db_info: DatabaseInfo) -> Result<Box<dyn Database>> {
        Ok(Box::new(Self {
            ctx,
            db_info,
            client: HttpClient::new()?,
            endpoint_config: Arc::new(Mutex::new(None)),
        }))
    }

    async fn get_share_endpoint(&self) -> Result<EndpointConfig> {
        let endpoint_config = {
            let endpoint_config = self.endpoint_config.lock().unwrap();
            endpoint_config.clone()
        };

        match endpoint_config {
            Some(ref endpoint_config) => Ok(endpoint_config.clone()),
            None => {
                let share_name = self.db_info.meta.from_share.clone().unwrap();
                let req = GetShareEndpointReq {
                    tenant: self.db_info.name_ident.tenant.clone(),
                    endpoint: None,
                    to_tenant: Some(share_name.tenant.clone()),
                };
                let meta_api = UserApiProvider::instance().get_meta_store_client();
                let resp = meta_api.get_share_endpoint(req).await?;
                if let Some((_, endpoint_meta)) = resp.share_endpoint_meta_vec.into_iter().next() {
                    let endpoint = format!(
                        "{}tenant/{}/{}/meta",
                        endpoint_meta.url, share_name.tenant, share_name.share_name,
                    );
                    let config = EndpointConfig {
                        url: endpoint,
                        token: RefreshableToken::Direct(self.db_info.name_ident.tenant.clone()),
                    };
                    let mut endpoint_config = self.endpoint_config.lock().unwrap();
                    *endpoint_config = Some(config.clone());
                    return Ok(config);
                }

                Err(ErrorCode::EmptyShareEndpointConfig(
                    "EmptyShareEndpointConfig, cannot query share databases".to_string(),
                ))
            }
        }
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
        // only when endpoint_config is Some can try again
        let mut try_again = {
            let endpoint_config = self.endpoint_config.lock().unwrap();
            !endpoint_config.is_some()
        };

        loop {
            let endpoint_config = self.get_share_endpoint().await?;
            let bs = Bytes::from(serde_json::to_vec(&req)?);
            let auth = endpoint_config.token.to_header().await?;
            let requester = GlobalConfig::instance().as_ref().query.tenant_id.clone();
            let req = Request::builder()
                .method(Method::POST)
                .uri(&endpoint_config.url)
                .header(AUTHORIZATION, auth)
                .header(CONTENT_LENGTH, bs.len())
                .header(TENANT_HEADER, requester)
                .body(AsyncBody::Bytes(bs))?;
            let resp = self.client.send_async(req).await;
            match resp {
                Ok(resp) => {
                    let bs = resp.into_body().bytes().await?;
                    let table_info_map: TableInfoMap = serde_json::from_slice(&bs)?;

                    return Ok(table_info_map);
                }
                Err(err) => {
                    if try_again {
                        error!("get_table_info_map error: {:?}", err);
                        return Err(err.into());
                    } else {
                        // endpoint may be changed, so cleanup endpoint and try again
                        try_again = true;
                        let mut endpoint_config = self.endpoint_config.lock().unwrap();
                        *endpoint_config = None;
                        info!("get_table_info_map error: {:?}, try again", err);
                    }
                }
            }
        }
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
