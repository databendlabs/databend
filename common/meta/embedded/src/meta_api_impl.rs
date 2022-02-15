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

use crate::MetaEmbedded;

#[async_trait]
impl MetaApi for MetaEmbedded {
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, MetaError> {
        let sm = self.inner.lock().await;
        let reply = sm.create_database(req).await?;
        Ok(reply)
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, MetaError> {
        let sm = self.inner.lock().await;
        let reply = sm.drop_database(req).await?;
        Ok(reply)
    }

    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, MetaError> {
        let sm = self.inner.lock().await;
        let reply = sm.get_database(req).await?;
        Ok(reply)
    }

    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, MetaError> {
        let sm = self.inner.lock().await;
        let reply = sm.list_databases(req).await?;
        Ok(reply)
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply, MetaError> {
        let sm = self.inner.lock().await;
        let reply = sm.create_table(req).await?;
        Ok(reply)
    }

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply, MetaError> {
        let sm = self.inner.lock().await;
        let reply = sm.drop_table(req).await?;
        Ok(reply)
    }

    async fn get_table(&self, req: GetTableReq) -> Result<Arc<TableInfo>, MetaError> {
        let sm = self.inner.lock().await;
        let reply = sm.get_table(req).await?;
        Ok(reply)
    }

    async fn list_tables(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, MetaError> {
        let sm = self.inner.lock().await;
        let reply = sm.list_tables(req).await?;
        Ok(reply)
    }

    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> Result<(TableIdent, Arc<TableMeta>), MetaError> {
        let sm = self.inner.lock().await;
        let reply = sm.get_table_by_id(table_id).await?;
        Ok(reply)
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, MetaError> {
        let sm = self.inner.lock().await;
        let reply = sm.upsert_table_option(req).await?;
        Ok(reply)
    }

    fn name(&self) -> String {
        "meta-embedded".to_string()
    }
}
