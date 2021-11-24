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

use std::sync::Arc;

use async_trait::async_trait;
use common_exception::Result;
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
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;

use crate::MetaEmbedded;

#[async_trait]
impl MetaApi for MetaEmbedded {
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        let sm = self.inner.lock().await;
        sm.create_database(req).await
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        let sm = self.inner.lock().await;
        sm.drop_database(req).await
    }

    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>> {
        let sm = self.inner.lock().await;
        sm.get_database(req).await
    }

    async fn list_databases(&self, req: ListDatabaseReq) -> Result<Vec<Arc<DatabaseInfo>>> {
        let sm = self.inner.lock().await;
        sm.list_databases(req).await
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply> {
        let sm = self.inner.lock().await;
        sm.create_table(req).await
    }

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply> {
        let sm = self.inner.lock().await;
        sm.drop_table(req).await
    }

    async fn get_table(&self, req: GetTableReq) -> Result<Arc<TableInfo>> {
        let sm = self.inner.lock().await;
        sm.get_table(req).await
    }

    async fn list_tables(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>> {
        let sm = self.inner.lock().await;
        sm.list_tables(req).await
    }

    async fn get_table_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)> {
        let sm = self.inner.lock().await;
        sm.get_table_by_id(table_id).await
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        let sm = self.inner.lock().await;
        sm.upsert_table_option(req).await
    }

    fn name(&self) -> String {
        "meta-embedded".to_string()
    }
}
