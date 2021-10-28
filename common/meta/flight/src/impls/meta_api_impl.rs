// Copyright 2020 Datafuse Labs.
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
//

use std::sync::Arc;

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

use crate::CreateDatabaseAction;
use crate::CreateTableAction;
use crate::DropDatabaseAction;
use crate::DropTableAction;
use crate::GetDatabaseAction;
use crate::GetDatabasesAction;
use crate::GetTableAction;
use crate::GetTableExtReq;
use crate::GetTablesAction;
use crate::MetaFlightClient;
use crate::UpsertTableOptionReq;

#[async_trait::async_trait]
impl MetaApi for MetaFlightClient {
    /// Create database call.
    async fn create_database(
        &self,
        plan: CreateDatabasePlan,
    ) -> common_exception::Result<CreateDatabaseReply> {
        self.do_action(CreateDatabaseAction { plan }).await
    }

    /// Drop database call.
    async fn drop_database(&self, plan: DropDatabasePlan) -> common_exception::Result<()> {
        self.do_action(DropDatabaseAction { plan }).await
    }

    async fn get_database(&self, db: &str) -> common_exception::Result<Arc<DatabaseInfo>> {
        let x = self
            .do_action(GetDatabaseAction { db: db.to_string() })
            .await?;

        Ok(Arc::new(x))
    }

    async fn get_databases(&self) -> common_exception::Result<Vec<Arc<DatabaseInfo>>> {
        self.do_action(GetDatabasesAction {}).await
    }

    /// Create table call.
    async fn create_table(
        &self,
        plan: CreateTablePlan,
    ) -> common_exception::Result<CreateTableReply> {
        self.do_action(CreateTableAction { plan }).await
    }

    /// Drop table call.
    async fn drop_table(&self, plan: DropTablePlan) -> common_exception::Result<()> {
        self.do_action(DropTableAction { plan }).await
    }

    /// Get table.
    async fn get_table(&self, db: &str, table: &str) -> common_exception::Result<Arc<TableInfo>> {
        let x = self
            .do_action(GetTableAction {
                db: db.to_string(),
                table: table.to_string(),
            })
            .await?;
        Ok(Arc::new(x))
    }

    /// Get tables.
    async fn get_tables(&self, db: &str) -> common_exception::Result<Vec<Arc<TableInfo>>> {
        self.do_action(GetTablesAction { db: db.to_string() }).await
    }

    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> common_exception::Result<(TableIdent, Arc<TableMeta>)> {
        let x = self.do_action(GetTableExtReq { tbl_id: table_id }).await?;
        Ok((x.ident, Arc::new(x.meta)))
    }

    async fn upsert_table_option(
        &self,
        table_id: MetaId,
        table_version: MetaVersion,
        option_key: String,
        option_value: String,
    ) -> common_exception::Result<UpsertTableOptionReply> {
        self.do_action(UpsertTableOptionReq {
            table_id,
            table_version,
            option_key,
            option_value,
        })
        .await
    }

    fn name(&self) -> String {
        "MetaFlightClient".to_string()
    }
}
