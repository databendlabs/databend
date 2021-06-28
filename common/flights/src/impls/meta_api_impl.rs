// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_store_api::CreateDatabaseActionResult;
use common_store_api::CreateTableActionResult;
use common_store_api::DropDatabaseActionResult;
use common_store_api::DropTableActionResult;
use common_store_api::GetDatabaseActionResult;
use common_store_api::GetTableActionResult;
use common_store_api::MetaApi;

use crate::action_declare;
use crate::store_do_action::StoreDoAction;
use crate::RequestFor;
use crate::StoreClient;

#[async_trait::async_trait]
impl MetaApi for StoreClient {
    /// Create database call.
    async fn create_database(
        &mut self,
        plan: CreateDatabasePlan,
    ) -> common_exception::Result<CreateDatabaseActionResult> {
        self.do_action(CreateDatabaseAction { plan }).await
    }

    async fn get_database(
        &mut self,
        db: &str,
    ) -> common_exception::Result<GetDatabaseActionResult> {
        self.do_action(GetDatabaseAction { db: db.to_string() })
            .await
    }

    /// Drop database call.
    async fn drop_database(
        &mut self,
        plan: DropDatabasePlan,
    ) -> common_exception::Result<DropDatabaseActionResult> {
        self.do_action(DropDatabaseAction { plan }).await
    }

    /// Create table call.
    async fn create_table(
        &mut self,
        plan: CreateTablePlan,
    ) -> common_exception::Result<CreateTableActionResult> {
        self.do_action(CreateTableAction { plan }).await
    }

    /// Drop table call.
    async fn drop_table(
        &mut self,
        plan: DropTablePlan,
    ) -> common_exception::Result<DropTableActionResult> {
        self.do_action(DropTableAction { plan }).await
    }

    /// Get table.
    async fn get_table(
        &mut self,
        db: String,
        table: String,
    ) -> common_exception::Result<GetTableActionResult> {
        self.do_action(GetTableAction { db, table }).await
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateDatabaseAction {
    pub plan: CreateDatabasePlan,
}

// === database: get ===
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetDatabaseAction {
    pub db: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropDatabaseAction {
    pub plan: DropDatabasePlan,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateTableAction {
    pub plan: CreateTablePlan,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropTableAction {
    pub plan: DropTablePlan,
}

impl RequestFor for DropTableAction {
    type Reply = DropTableActionResult;
}

impl From<DropTableAction> for StoreDoAction {
    fn from(act: DropTableAction) -> Self {
        StoreDoAction::DropTable(act)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableAction {
    pub db: String,
    pub table: String,
}

action_declare!(
    CreateTableAction,
    CreateTableActionResult,
    StoreDoAction::CreateTable
);

action_declare!(
    GetTableAction,
    GetTableActionResult,
    StoreDoAction::GetTable
);

action_declare!(
    CreateDatabaseAction,
    CreateDatabaseActionResult,
    StoreDoAction::CreateDatabase
);

action_declare!(
    DropDatabaseAction,
    DropDatabaseActionResult,
    StoreDoAction::DropDatabase
);

action_declare!(
    GetDatabaseAction,
    GetDatabaseActionResult,
    StoreDoAction::GetDatabase
);

// === database: create ===

// === table: drop ===

// === table: get ===
