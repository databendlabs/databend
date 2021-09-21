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

use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
pub use common_store_api::CreateDatabaseActionResult;
pub use common_store_api::CreateTableActionResult;
pub use common_store_api::DatabaseMetaReply;
pub use common_store_api::DatabaseMetaSnapshot;
pub use common_store_api::DropDatabaseActionResult;
pub use common_store_api::DropTableActionResult;
pub use common_store_api::GetDatabaseActionResult;
pub use common_store_api::GetTableActionResult;
use common_store_api::MetaApi;

use crate::action_declare;
use crate::store_do_action::StoreDoAction;
use crate::RequestFor;
use crate::StoreClient;

#[async_trait::async_trait]
impl MetaApi for StoreClient {
    /// Create database call.
    async fn create_database(
        &self,
        plan: CreateDatabasePlan,
    ) -> common_exception::Result<CreateDatabaseActionResult> {
        self.do_action(CreateDatabaseAction { plan }).await
    }

    async fn get_database(&self, db: &str) -> common_exception::Result<GetDatabaseActionResult> {
        self.do_action(GetDatabaseAction { db: db.to_string() })
            .await
    }

    /// Drop database call.
    async fn drop_database(
        &self,
        plan: DropDatabasePlan,
    ) -> common_exception::Result<DropDatabaseActionResult> {
        self.do_action(DropDatabaseAction { plan }).await
    }

    /// Create table call.
    async fn create_table(
        &self,
        plan: CreateTablePlan,
    ) -> common_exception::Result<CreateTableActionResult> {
        self.do_action(CreateTableAction { plan }).await
    }

    /// Drop table call.
    async fn drop_table(
        &self,
        plan: DropTablePlan,
    ) -> common_exception::Result<DropTableActionResult> {
        self.do_action(DropTableAction { plan }).await
    }

    /// Get table.
    async fn get_table(
        &self,
        db: String,
        table: String,
    ) -> common_exception::Result<GetTableActionResult> {
        self.do_action(GetTableAction { db, table }).await
    }

    async fn get_table_ext(
        &self,
        tbl_id: MetaId,
        tbl_ver: Option<MetaVersion>,
    ) -> common_exception::Result<GetTableActionResult> {
        self.do_action(GetTableExtReq { tbl_id, tbl_ver }).await
    }

    async fn get_database_meta(
        &self,
        ver_lower_bound: Option<u64>,
    ) -> common_exception::Result<DatabaseMetaReply> {
        self.do_action(GetDatabaseMetaAction { ver_lower_bound })
            .await
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
    CreateDatabaseActionResult,
    StoreDoAction::CreateDatabase
);

// - get database
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetDatabaseAction {
    pub db: String,
}
action_declare!(
    GetDatabaseAction,
    GetDatabaseActionResult,
    StoreDoAction::GetDatabase
);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropDatabaseAction {
    pub plan: DropDatabasePlan,
}
action_declare!(
    DropDatabaseAction,
    DropDatabaseActionResult,
    StoreDoAction::DropDatabase
);

// == table actions ==
// - create table
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateTableAction {
    pub plan: CreateTablePlan,
}
action_declare!(
    CreateTableAction,
    CreateTableActionResult,
    StoreDoAction::CreateTable
);

// - drop table
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropTableAction {
    pub plan: DropTablePlan,
}
action_declare!(
    DropTableAction,
    DropTableActionResult,
    StoreDoAction::DropTable
);

// - get table
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableAction {
    pub db: String,
    pub table: String,
}
action_declare!(
    GetTableAction,
    GetTableActionResult,
    StoreDoAction::GetTable
);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableExtReq {
    pub tbl_id: MetaId,
    pub tbl_ver: Option<MetaVersion>,
}
action_declare!(
    GetTableExtReq,
    GetTableActionResult,
    StoreDoAction::GetTableExt
);

// - get database meta

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetDatabaseMetaAction {
    pub ver_lower_bound: Option<u64>,
}

action_declare!(
    GetDatabaseMetaAction,
    DatabaseMetaReply,
    StoreDoAction::GetDatabaseMeta
);
