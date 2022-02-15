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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use common_meta_api::MetaApi;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateTableReply;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseInfo;
use common_meta_types::DropDatabaseReply;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetDatabaseReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListDatabaseReq;
use common_meta_types::ListTableReq;
use common_meta_types::MetaError;
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;

use crate::common::MetaClientProvider;

/// A `MetaApi` impl with MetaApi RPC.
#[allow(dead_code)]
#[derive(Clone)]
pub struct MetaBackend {
    rpc_time_out: Option<Duration>,
    meta_api_provider: Arc<MetaClientProvider>,
}

impl MetaBackend {
    pub fn create(apis_provider: Arc<MetaClientProvider>) -> MetaBackend {
        Self::with_timeout_setting(apis_provider, Some(Duration::from_secs(5)))
    }

    pub fn with_timeout_setting(
        apis_provider: Arc<MetaClientProvider>,
        timeout: Option<Duration>,
    ) -> MetaBackend {
        MetaBackend {
            rpc_time_out: timeout,
            meta_api_provider: apis_provider,
        }
    }

    async fn query_backend<F, T, ResFut>(&self, f: F) -> std::result::Result<T, MetaError>
    where
        ResFut: Future<Output = std::result::Result<T, MetaError>> + Send + 'static,
        F: FnOnce(Arc<dyn MetaApi>) -> ResFut,
        F: Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        let res: Result<_, std::convert::Infallible> =
            self.meta_api_provider.try_get_meta_client().await;
        let cli = res.unwrap();

        f(cli).await
    }
}

#[async_trait::async_trait]
impl MetaApi for MetaBackend {
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> std::result::Result<CreateDatabaseReply, MetaError> {
        self.query_backend(move |cli| async move { cli.create_database(req).await })
            .await
    }

    async fn drop_database(
        &self,
        req: DropDatabaseReq,
    ) -> std::result::Result<DropDatabaseReply, MetaError> {
        self.query_backend(move |cli| async move { cli.drop_database(req).await })
            .await
    }

    async fn get_database(
        &self,
        req: GetDatabaseReq,
    ) -> std::result::Result<Arc<DatabaseInfo>, MetaError> {
        self.query_backend(move |cli| async move { cli.get_database(req).await })
            .await
    }

    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> std::result::Result<Vec<Arc<DatabaseInfo>>, MetaError> {
        self.query_backend(move |cli| async move { cli.list_databases(req).await })
            .await
    }

    async fn create_table(
        &self,
        req: CreateTableReq,
    ) -> std::result::Result<CreateTableReply, MetaError> {
        // TODO validate plan by table engine first
        self.query_backend(move |cli| async move { cli.create_table(req).await })
            .await
    }

    async fn drop_table(
        &self,
        req: DropTableReq,
    ) -> std::result::Result<DropTableReply, MetaError> {
        self.query_backend(move |cli| async move { cli.drop_table(req).await })
            .await
    }

    async fn get_table(&self, req: GetTableReq) -> std::result::Result<Arc<TableInfo>, MetaError> {
        self.query_backend(move |cli| async move { cli.get_table(req).await })
            .await
    }

    async fn list_tables(
        &self,
        req: ListTableReq,
    ) -> std::result::Result<Vec<Arc<TableInfo>>, MetaError> {
        self.query_backend(move |cli| async move { cli.list_tables(req).await })
            .await
    }

    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> std::result::Result<(TableIdent, Arc<TableMeta>), MetaError> {
        self.query_backend(move |cli| async move { cli.get_table_by_id(table_id).await })
            .await
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> std::result::Result<UpsertTableOptionReply, MetaError> {
        self.query_backend(move |cli| async move { cli.upsert_table_option(req).await })
            .await
    }

    fn name(&self) -> String {
        "meta-remote".to_owned()
    }
}
