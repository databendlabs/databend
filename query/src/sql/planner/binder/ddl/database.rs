// Copyright 2022 Datafuse Labs.
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

use std::collections::BTreeMap;

use common_ast::ast::AlterDatabaseAction;
use common_ast::ast::AlterDatabaseStmt;
use common_ast::ast::CreateDatabaseStmt;
use common_ast::ast::DatabaseEngine;
use common_ast::ast::DropDatabaseStmt;
use common_exception::Result;
use common_meta_app::schema::DatabaseMeta;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;
use common_planners::RenameDatabaseEntity;
use common_planners::RenameDatabasePlan;

use crate::sql::binder::Binder;
use crate::sql::plans::Plan;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_alter_database(
        &self,
        stmt: &AlterDatabaseStmt<'a>,
    ) -> Result<Plan> {
        let catalog = stmt
            .catalog
            .as_ref()
            .map(|catalog| catalog.name.clone())
            .unwrap_or_else(|| self.ctx.get_current_catalog());

        let tenant = self.ctx.get_tenant();
        let database = stmt.database.name.clone();
        let if_exists = stmt.if_exists;

        match &stmt.action {
            AlterDatabaseAction::RenameDatabase { new_db } => {
                let new_database = new_db.name.clone();
                let entry = RenameDatabaseEntity {
                    if_exists,
                    catalog_name: catalog,
                    database,
                    new_database,
                };

                Ok(Plan::RenameDatabase(Box::new(RenameDatabasePlan {
                    tenant,
                    entities: vec![entry],
                })))
            }
        }
    }

    pub(in crate::sql::planner::binder) async fn bind_drop_database(
        &self,
        stmt: &DropDatabaseStmt<'a>,
    ) -> Result<Plan> {
        let catalog = stmt
            .catalog
            .as_ref()
            .map(|catalog| catalog.name.clone())
            .unwrap_or_else(|| self.ctx.get_current_catalog());

        let tenant = self.ctx.get_tenant();
        let database = stmt.database.name.clone();
        let if_exists = stmt.if_exists;

        Ok(Plan::DropDatabase(Box::new(DropDatabasePlan {
            tenant,
            catalog,
            database,
            if_exists,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_create_database(
        &self,
        stmt: &CreateDatabaseStmt<'a>,
    ) -> Result<Plan> {
        let catalog = stmt
            .catalog
            .as_ref()
            .map(|catalog| catalog.name.clone())
            .unwrap_or_else(|| self.ctx.get_current_catalog());

        let tenant = self.ctx.get_tenant();
        let if_not_exists = stmt.if_not_exists;
        let database = stmt.database.name.clone();
        let meta = self.database_meta(stmt)?;

        Ok(Plan::CreateDatabase(Box::new(CreateDatabasePlan {
            tenant,
            if_not_exists,
            catalog,
            database,
            meta,
        })))
    }

    fn database_meta(&self, stmt: &CreateDatabaseStmt<'a>) -> Result<DatabaseMeta> {
        let options = stmt
            .options
            .iter()
            .map(|property| (property.name.clone(), property.value.clone()))
            .collect::<BTreeMap<String, String>>();

        let database_engine = stmt.engine.as_ref().unwrap_or(&DatabaseEngine::Default);
        let (engine, engine_options) = match database_engine {
            DatabaseEngine::Github(token) => {
                let engine_options =
                    BTreeMap::from_iter(vec![("token".to_string(), token.clone())]);
                ("github", engine_options)
            }
            DatabaseEngine::Default => ("default", BTreeMap::default()),
        };

        Ok(DatabaseMeta {
            engine: engine.to_string(),
            engine_options,
            options,
            ..Default::default()
        })
    }
}
