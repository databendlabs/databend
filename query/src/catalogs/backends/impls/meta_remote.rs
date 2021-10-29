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

use common_exception::Result;
use common_meta_api::MetaApi;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateTableReply;
use common_meta_types::DatabaseInfo;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

use crate::common::MetaClientProvider;

/// A `MetaApi` impl with MetaApi RPC.
#[derive(Clone)]
pub struct MetaRemote {
    #[allow(dead_code)]
    rpc_time_out: Option<Duration>,
    meta_api_provider: Arc<MetaClientProvider>,
}

impl MetaRemote {
    pub fn create(apis_provider: Arc<MetaClientProvider>) -> MetaRemote {
        Self::with_timeout_setting(apis_provider, Some(Duration::from_secs(5)))
    }

    pub fn with_timeout_setting(
        apis_provider: Arc<MetaClientProvider>,
        timeout: Option<Duration>,
    ) -> MetaRemote {
        MetaRemote {
            rpc_time_out: timeout,
            meta_api_provider: apis_provider,
        }
    }

    async fn query_backend<F, T, ResFut>(&self, f: F) -> Result<T>
    where
        ResFut: Future<Output = Result<T>> + Send + 'static,
        F: FnOnce(Arc<dyn MetaApi>) -> ResFut,
        F: Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        let cli = self.meta_api_provider.try_get_meta_client().await?;
        f(cli).await
    }
}

#[async_trait::async_trait]
impl MetaApi for MetaRemote {
    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<CreateDatabaseReply> {
        self.query_backend(move |cli| async move { cli.create_database(plan).await })
            .await
    }

    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        self.query_backend(move |cli| async move { cli.drop_database(plan).await })
            .await
    }

    async fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseInfo>> {
        let db_name = db_name.to_owned();
        self.query_backend(move |cli| async move { cli.get_database(&db_name).await })
            .await
    }

    async fn get_databases(&self) -> Result<Vec<Arc<DatabaseInfo>>> {
        self.query_backend(move |cli| async move { cli.get_databases().await })
            .await
    }

    async fn create_table(&self, plan: CreateTablePlan) -> Result<CreateTableReply> {
        // TODO validate plan by table engine first
        self.query_backend(move |cli| async move { cli.create_table(plan).await })
            .await
    }

    async fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        self.query_backend(move |cli| async move { cli.drop_table(plan).await })
            .await
    }

    async fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableInfo>> {
        let table_name = table_name.to_string();
        let db_name = db_name.to_string();
        self.query_backend(move |cli| async move { cli.get_table(&db_name, &table_name).await })
            .await
    }

    async fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableInfo>>> {
        let db_name = db_name.to_owned();
        self.query_backend(move |cli| async move { cli.get_tables(&db_name).await })
            .await
    }

    async fn get_table_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)> {
        self.query_backend(move |cli| async move { cli.get_table_by_id(table_id).await })
            .await
    }

    async fn upsert_table_option(
        &self,
        table_id: MetaId,
        table_version: MetaVersion,
        option_key: String,
        option_value: String,
    ) -> Result<UpsertTableOptionReply> {
        self.query_backend(move |cli| async move {
            cli.upsert_table_option(table_id, table_version, option_key, option_value)
                .await
        })
        .await
    }

    fn name(&self) -> String {
        "meta-remote".to_owned()
    }
}
