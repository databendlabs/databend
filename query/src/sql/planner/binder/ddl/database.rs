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
use common_ast::ast::SQLProperty;
use common_ast::ast::ShowCreateDatabaseStmt;
use common_ast::ast::ShowDatabasesStmt;
use common_ast::ast::ShowLimit;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::ToDataType;
use common_datavalues::Vu8;
use common_exception::Result;
use common_meta_app::schema::DatabaseMeta;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;
use common_planners::PlanShowKind;
use common_planners::RenameDatabaseEntity;
use common_planners::RenameDatabasePlan;
use common_planners::ShowCreateDatabasePlan;
use common_planners::ShowDatabasesPlan;

use crate::sql::binder::Binder;
use crate::sql::plans::Plan;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_show_databases(
        &self,
        stmt: &ShowDatabasesStmt<'a>,
    ) -> Result<Plan> {
        let ShowDatabasesStmt { limit } = stmt;

        let kind = match limit {
            Some(ShowLimit::Like { pattern }) => PlanShowKind::Like(pattern.clone()),
            Some(ShowLimit::Where { selection }) => PlanShowKind::Like(selection.to_string()),
            None => PlanShowKind::All,
        };

        Ok(Plan::ShowDatabases(Box::new(ShowDatabasesPlan { kind })))
    }

    pub(in crate::sql::planner::binder) async fn bind_show_create_database(
        &self,
        stmt: &ShowCreateDatabaseStmt<'a>,
    ) -> Result<Plan> {
        let ShowCreateDatabaseStmt { catalog, database } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database.name.to_lowercase();
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Database", Vu8::to_data_type()),
            DataField::new("Create Database", Vu8::to_data_type()),
        ]);

        Ok(Plan::ShowCreateDatabase(Box::new(ShowCreateDatabasePlan {
            catalog,
            database,
            schema,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_alter_database(
        &self,
        stmt: &AlterDatabaseStmt<'a>,
    ) -> Result<Plan> {
        let AlterDatabaseStmt {
            if_exists,
            catalog,
            database,
            action,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database.name.to_lowercase();

        match &action {
            AlterDatabaseAction::RenameDatabase { new_db } => {
                let new_database = new_db.name.clone();
                let entry = RenameDatabaseEntity {
                    if_exists: *if_exists,
                    catalog,
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
        let DropDatabaseStmt {
            if_exists,
            catalog,
            database,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database.name.to_lowercase();

        Ok(Plan::DropDatabase(Box::new(DropDatabasePlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_create_database(
        &self,
        stmt: &CreateDatabaseStmt<'a>,
    ) -> Result<Plan> {
        let CreateDatabaseStmt {
            if_not_exists,
            catalog,
            database,
            engine,
            options,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database.name.to_lowercase();
        let meta = self.database_meta(engine, options)?;

        Ok(Plan::CreateDatabase(Box::new(CreateDatabasePlan {
            if_not_exists: *if_not_exists,
            tenant,
            catalog,
            database,
            meta,
        })))
    }

    fn database_meta(
        &self,
        engine: &Option<DatabaseEngine>,
        options: &[SQLProperty],
    ) -> Result<DatabaseMeta> {
        let options = options
            .iter()
            .map(|property| (property.name.clone(), property.value.clone()))
            .collect::<BTreeMap<String, String>>();

        let database_engine = engine.as_ref().unwrap_or(&DatabaseEngine::Default);
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
