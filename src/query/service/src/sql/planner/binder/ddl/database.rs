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

use core::fmt::Write;
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
use common_ast::ast::UndropDatabaseStmt;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::ToDataType;
use common_datavalues::Vu8;
use common_exception::Result;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::share::ShareNameIdent;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;
use common_planners::RenameDatabaseEntity;
use common_planners::RenameDatabasePlan;
use common_planners::ShowCreateDatabasePlan;
use common_planners::UndropDatabasePlan;

use crate::sessions::TableContext;
use crate::sql::binder::Binder;
use crate::sql::planner::semantic::normalize_identifier;
use crate::sql::plans::Plan;
use crate::sql::plans::RewriteKind;
use crate::sql::BindContext;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_show_databases(
        &mut self,
        bind_context: &BindContext,
        stmt: &ShowDatabasesStmt<'a>,
    ) -> Result<Plan> {
        let ShowDatabasesStmt { limit } = stmt;
        let mut query = String::new();
        write!(query, "SELECT name AS Database FROM system.databases").unwrap();
        match limit {
            Some(ShowLimit::Like { pattern }) => {
                write!(query, " WHERE name LIKE '{pattern}'").unwrap();
            }
            Some(ShowLimit::Where { selection }) => {
                write!(query, " WHERE {selection}").unwrap();
            }
            None => (),
        }
        write!(query, " ORDER BY name").unwrap();

        self.bind_rewrite_to_query(bind_context, query.as_str(), RewriteKind::ShowDatabases)
            .await
    }

    pub(in crate::sql::planner::binder) async fn bind_show_create_database(
        &self,
        stmt: &ShowCreateDatabaseStmt<'a>,
    ) -> Result<Plan> {
        let ShowCreateDatabaseStmt { catalog, database } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;
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
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;

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
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;

        Ok(Plan::DropDatabase(Box::new(DropDatabasePlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_undrop_database(
        &self,
        stmt: &UndropDatabaseStmt<'a>,
    ) -> Result<Plan> {
        let UndropDatabaseStmt { catalog, database } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;

        Ok(Plan::UndropDatabase(Box::new(UndropDatabasePlan {
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
            from_share,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;
        let meta = self.database_meta(engine, options, from_share)?;

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
        from_share: &Option<ShareNameIdent>,
    ) -> Result<DatabaseMeta> {
        let options = options
            .iter()
            .map(|property| (property.name.clone(), property.value.clone()))
            .collect::<BTreeMap<String, String>>();

        let database_engine = engine.as_ref().unwrap_or(&DatabaseEngine::Default);
        let (engine, engine_options) = match database_engine {
            DatabaseEngine::Default => ("default", BTreeMap::default()),
        };

        Ok(DatabaseMeta {
            engine: engine.to_string(),
            engine_options,
            options,
            from_share: from_share.clone(),
            ..Default::default()
        })
    }
}
