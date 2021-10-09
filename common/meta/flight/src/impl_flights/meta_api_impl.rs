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

use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::TableInfo;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

use crate::action_declare;
use crate::meta_api::MetaApi;
use crate::meta_flight_action::MetaFlightAction;
use crate::meta_flight_reply::CreateDatabaseReply;
use crate::meta_flight_reply::CreateTableReply;
use crate::meta_flight_reply::DatabaseInfo;
use crate::meta_flight_reply::GetDatabasesReply;
use crate::meta_flight_reply::GetTablesReply;
use crate::MetaFlightClient;
use crate::RequestFor;

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

    async fn get_database(&self, db: &str) -> common_exception::Result<DatabaseInfo> {
        self.do_action(GetDatabaseAction { db: db.to_string() })
            .await
    }

    async fn get_databases(&self) -> common_exception::Result<GetDatabasesReply> {
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
    async fn get_table(&self, db: &str, table: &str) -> common_exception::Result<TableInfo> {
        self.do_action(GetTableAction {
            db: db.to_string(),
            table: table.to_string(),
        })
        .await
    }

    /// Get tables.
    async fn get_tables(&self, db: &str) -> common_exception::Result<GetTablesReply> {
        self.do_action(GetTablesAction { db: db.to_string() }).await
    }

    async fn get_table_by_id(
        &self,
        tbl_id: MetaId,
        tbl_ver: Option<MetaVersion>,
    ) -> common_exception::Result<TableInfo> {
        self.do_action(GetTableExtReq { tbl_id, tbl_ver }).await
    }
}

// == database actions ==
// - create database
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateDatabaseAction {
    pub plan: CreateDatabasePlan,
}
action_declare!(
    CreateDatabaseAction,
    CreateDatabaseReply,
    MetaFlightAction::CreateDatabase
);

// - get database
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetDatabaseAction {
    pub db: String,
}
action_declare!(
    GetDatabaseAction,
    DatabaseInfo,
    MetaFlightAction::GetDatabase
);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropDatabaseAction {
    pub plan: DropDatabasePlan,
}
action_declare!(DropDatabaseAction, (), MetaFlightAction::DropDatabase);

// == table actions ==
// - create table
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateTableAction {
    pub plan: CreateTablePlan,
}
action_declare!(
    CreateTableAction,
    CreateTableReply,
    MetaFlightAction::CreateTable
);

// - drop table
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropTableAction {
    pub plan: DropTablePlan,
}
action_declare!(DropTableAction, (), MetaFlightAction::DropTable);

// - get table
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableAction {
    pub db: String,
    pub table: String,
}

action_declare!(GetTableAction, TableInfo, MetaFlightAction::GetTable);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableExtReq {
    pub tbl_id: MetaId,
    pub tbl_ver: Option<MetaVersion>,
}
action_declare!(GetTableExtReq, TableInfo, MetaFlightAction::GetTableExt);

// - get tables
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTablesAction {
    pub db: String,
}

action_declare!(GetTablesAction, GetTablesReply, MetaFlightAction::GetTables);

// -get databases

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct GetDatabasesAction;

action_declare!(
    GetDatabasesAction,
    GetDatabasesReply,
    MetaFlightAction::GetDatabases
);
