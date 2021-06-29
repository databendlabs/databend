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

use crate::CreateDatabaseAction;
use crate::CreateTableAction;
use crate::DropDatabaseAction;
use crate::DropTableAction;
use crate::GetDatabaseAction;
use crate::GetTableAction;
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
