// Copyright 2021 Datafuse Labs
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

use databend_common_ast::ast::AlterDatabaseAction;
use databend_common_ast::ast::AlterDatabaseStmt;
use databend_common_ast::ast::CreateDatabaseStmt;
use databend_common_ast::ast::DatabaseEngine;
use databend_common_ast::ast::DatabaseRef;
use databend_common_ast::ast::DropDatabaseStmt;
use databend_common_ast::ast::SQLProperty;
use databend_common_ast::ast::ShowCreateDatabaseStmt;
use databend_common_ast::ast::ShowDatabasesStmt;
use databend_common_ast::ast::ShowDropDatabasesStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::UndropDatabaseStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_meta_app::schema::DatabaseMeta;
use log::debug;

use crate::BindContext;
use crate::SelectBuilder;
use crate::binder::Binder;
use crate::planner::semantic::normalize_identifier;
use crate::plans::AlterDatabasePlan;
use crate::plans::CreateDatabasePlan;
use crate::plans::DropDatabasePlan;
use crate::plans::Plan;
use crate::plans::RefreshDatabaseCachePlan;
use crate::plans::RenameDatabaseEntity;
use crate::plans::RenameDatabasePlan;
use crate::plans::RewriteKind;
use crate::plans::ShowCreateDatabasePlan;
use crate::plans::UndropDatabasePlan;

pub const DEFAULT_STORAGE_CONNECTION: &str = "DEFAULT_STORAGE_CONNECTION";
pub const DEFAULT_STORAGE_PATH: &str = "DEFAULT_STORAGE_PATH";

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_databases(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowDatabasesStmt,
    ) -> Result<Plan> {
        let ShowDatabasesStmt {
            catalog,
            full,
            limit,
        } = stmt;

        let ctl = if let Some(ctl) = catalog {
            if let Err(err) = self.ctx.get_catalog(ctl.to_string().as_str()).await {
                return Err(ErrorCode::UnknownCatalog(format!(
                    "Get catalog {} with error: {}",
                    ctl, err
                )));
            }
            normalize_identifier(ctl, &self.name_resolution_ctx).name
        } else {
            self.ctx.get_current_catalog().to_string()
        };

        let mut select_builder =
            SelectBuilder::from(&format!("{}.system.databases", ctl.to_lowercase()));

        select_builder.with_filter(format!("catalog = '{ctl}'"));

        if *full {
            select_builder.with_column("catalog AS Catalog");
            select_builder.with_column("owner");
        }
        select_builder.with_column(format!("name AS `databases_in_{ctl}`"));
        select_builder.with_order_by("catalog");
        select_builder.with_order_by("name");

        match limit {
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE '{pattern}'"));
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
            }
            None => (),
        }

        let query = select_builder.build();
        debug!("show databases rewrite to: {:?}", query);

        self.bind_rewrite_to_query(bind_context, query.as_str(), RewriteKind::ShowDatabases)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_drop_databases(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowDropDatabasesStmt,
    ) -> Result<Plan> {
        let ShowDropDatabasesStmt { catalog, limit } = stmt;
        let default_catalog = self.ctx.get_default_catalog()?.name();
        let mut select_builder = SelectBuilder::from(&format!(
            "{}.system.databases_with_history",
            default_catalog
        ));

        let ctl = if let Some(ctl) = catalog {
            normalize_identifier(ctl, &self.name_resolution_ctx).name
        } else {
            self.ctx.get_current_catalog().to_string()
        };

        select_builder.with_filter(format!("catalog = '{ctl}'"));

        select_builder.with_column("catalog");
        select_builder.with_column("name");
        select_builder.with_column("database_id");
        select_builder.with_column("dropped_on");

        select_builder.with_order_by("catalog");
        select_builder.with_order_by("name");

        match limit {
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE '{pattern}'"));
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
            }
            None => (),
        }
        let query = select_builder.build();
        debug!("show drop databases rewrite to: {:?}", query);

        self.bind_rewrite_to_query(bind_context, query.as_str(), RewriteKind::ShowDropDatabases)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_create_database(
        &self,
        stmt: &ShowCreateDatabaseStmt,
    ) -> Result<Plan> {
        let ShowCreateDatabaseStmt { catalog, database } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Database", DataType::String),
            DataField::new("Create Database", DataType::String),
        ]);

        Ok(Plan::ShowCreateDatabase(Box::new(ShowCreateDatabasePlan {
            catalog,
            database,
            schema,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_database(
        &self,
        stmt: &AlterDatabaseStmt,
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

            AlterDatabaseAction::RefreshDatabaseCache => Ok(Plan::RefreshDatabaseCache(Box::new(
                RefreshDatabaseCachePlan {
                    tenant,
                    catalog,
                    database,
                },
            ))),

            AlterDatabaseAction::SetOptions { options } => {
                let catalog_arc = self.ctx.get_catalog(&catalog).await?;
                let db_exists = catalog_arc.exists_database(&tenant, &database).await?;
                if !db_exists && !*if_exists {
                    return Err(ErrorCode::UnknownDatabase(format!(
                        "Unknown database '{}'",
                        database
                    )));
                }

                // Validate database options only when the database exists.
                let db_options = if db_exists {
                    // For ALTER DATABASE, allow modifying single option (the other already exists)
                    self.validate_database_options(options, false).await?;

                    options
                        .iter()
                        .map(|property| (property.name.clone(), property.value.clone()))
                        .collect::<BTreeMap<String, String>>()
                } else {
                    BTreeMap::new()
                };

                Ok(Plan::AlterDatabase(Box::new(AlterDatabasePlan {
                    tenant,
                    catalog,
                    database,
                    if_exists: *if_exists,
                    options: db_options,
                })))
            }
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_database(
        &self,
        stmt: &DropDatabaseStmt,
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

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_undrop_database(
        &self,
        stmt: &UndropDatabaseStmt,
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

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_database(
        &self,
        stmt: &CreateDatabaseStmt,
    ) -> Result<Plan> {
        let CreateDatabaseStmt {
            create_option,
            database: DatabaseRef { catalog, database },
            engine,
            options,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;

        // Validate database options (connection, URI, storage access)
        // For CREATE DATABASE, require both options to be specified together
        self.validate_database_options(options, true).await?;

        let meta = self.database_meta(engine, options)?;

        Ok(Plan::CreateDatabase(Box::new(CreateDatabasePlan {
            create_option: create_option.clone().into(),
            tenant,
            catalog,
            database,
            meta,
        })))
    }

    /// Validate database options including connection existence, URI location, and storage access
    ///
    /// # Arguments
    /// * `options` - The database options to validate
    /// * `require_both` - If true (CREATE), both options must be present together.
    ///                    If false (ALTER), allows modifying single option.
    #[async_backtrace::framed]
    async fn validate_database_options(
        &self,
        options: &[SQLProperty],
        require_both: bool,
    ) -> Result<()> {
        // Validate database options - only allow specific connection-related options
        const VALID_DATABASE_OPTIONS: &[&str] = &[DEFAULT_STORAGE_CONNECTION, DEFAULT_STORAGE_PATH];

        // Check for duplicate options
        let mut seen_options = std::collections::HashSet::new();
        for option in options {
            if !seen_options.insert(&option.name) {
                return Err(ErrorCode::InvalidArgument(format!(
                    "Duplicate database option '{}' is not allowed",
                    option.name
                )));
            }

            if !VALID_DATABASE_OPTIONS.contains(&option.name.to_uppercase().as_str()) {
                return Err(ErrorCode::InvalidArgument(format!(
                    "Invalid database option '{}'. Valid options are: {}",
                    option.name,
                    VALID_DATABASE_OPTIONS.join(", ")
                )));
            }
        }

        // Validate pairing requirement based on operation type
        let has_connection = options.iter().any(|p| p.name == DEFAULT_STORAGE_CONNECTION);
        let has_path = options.iter().any(|p| p.name == DEFAULT_STORAGE_PATH);

        if require_both {
            // For CREATE DATABASE: both options must be specified together
            if has_connection && !has_path {
                return Err(ErrorCode::BadArguments(format!(
                    "{} requires {} to be specified",
                    DEFAULT_STORAGE_CONNECTION, DEFAULT_STORAGE_PATH
                )));
            }

            if has_path && !has_connection {
                return Err(ErrorCode::BadArguments(format!(
                    "{} requires {} to be specified",
                    DEFAULT_STORAGE_PATH, DEFAULT_STORAGE_CONNECTION
                )));
            }
        }
        // For ALTER DATABASE: allow modifying single option (the other one already exists in database)

        // Validate that the specified connection exists
        if let Some(connection_property) = options
            .iter()
            .find(|p| p.name == DEFAULT_STORAGE_CONNECTION)
        {
            let connection_name = &connection_property.value;

            // Check if the connection exists by trying to get it through the context
            match self.ctx.get_connection(connection_name).await {
                Ok(_) => {
                    // Connection exists, continue
                }
                Err(_) => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Connection '{}' does not exist. Please create the connection first using CREATE CONNECTION",
                        connection_name
                    )));
                }
            }
        }

        // Validate storage path accessibility when both connection and path are specified
        if let (Some(connection_prop), Some(path_prop)) = (
            options
                .iter()
                .find(|p| p.name == DEFAULT_STORAGE_CONNECTION),
            options.iter().find(|p| p.name == DEFAULT_STORAGE_PATH),
        ) {
            // Validate the storage path is accessible and matches the connection protocol
            let connection = self.ctx.get_connection(&connection_prop.value).await?;

            let uri_for_scheme = databend_common_ast::ast::UriLocation::from_uri(
                path_prop.value.clone(),
                BTreeMap::new(),
            )
            .map_err(|e| {
                ErrorCode::BadArguments(format!(
                    "Invalid storage path '{}': {}",
                    path_prop.value, e
                ))
            })?;

            let path_protocol = uri_for_scheme.protocol.to_ascii_lowercase();
            let connection_protocol = connection.storage_type.to_ascii_lowercase();

            if path_protocol != connection_protocol {
                return Err(ErrorCode::BadArguments(format!(
                    "{} protocol '{}' does not match connection '{}' protocol '{}'",
                    DEFAULT_STORAGE_PATH,
                    uri_for_scheme.protocol,
                    connection_prop.value,
                    connection.storage_type
                )));
            }

            let mut uri_location = databend_common_ast::ast::UriLocation::from_uri(
                path_prop.value.clone(),
                connection.storage_params.clone(),
            )
            .map_err(|e| {
                ErrorCode::BadArguments(format!(
                    "Invalid storage path '{}': {}",
                    path_prop.value, e
                ))
            })?;

            // Parse and validate the URI location using parse_storage_params_from_uri
            // This enforces that the path must end with '/' (directory requirement)
            let storage_params = crate::binder::parse_storage_params_from_uri(
                &mut uri_location,
                Some(&*self.ctx),
                "when setting database DEFAULT_STORAGE_PATH",
            )
            .await
            .map_err(|e| {
                ErrorCode::BadArguments(format!(
                    "Invalid storage path '{}': {}",
                    path_prop.value, e
                ))
            })?;

            // Check if storage is secure when required
            if !storage_params.is_secure()
                && !databend_common_config::GlobalConfig::instance()
                    .storage
                    .allow_insecure
            {
                return Err(ErrorCode::StorageInsecure(
                    "Database default storage path points to insecure storage, which is not allowed",
                ));
            }

            // Verify essential privileges for the external storage location
            // Similar to table creation, we test basic storage operations
            let operator =
                databend_common_storage::init_operator(&storage_params).map_err(|e| {
                    ErrorCode::BadArguments(format!(
                        "Failed to access storage location '{}': {}",
                        path_prop.value, e
                    ))
                })?;

            // Test storage accessibility with basic operations
            // Reuse the existing verify_external_location_privileges function from table.rs
            crate::binder::verify_external_location_privileges(operator)
                .await
                .map_err(|e| {
                    ErrorCode::BadArguments(format!(
                    "Failed to verify permissions for database default storage location '{}': {}",
                    path_prop.value, e
                ))
                })?;
        }

        Ok(())
    }

    fn database_meta(
        &self,
        engine: &Option<DatabaseEngine>,
        options: &[SQLProperty],
    ) -> Result<DatabaseMeta> {
        // Note: Options validation is done in validate_database_options()
        // This function only creates the DatabaseMeta structure

        let options = options
            .iter()
            .map(|property| (property.name.clone(), property.value.clone()))
            .collect::<BTreeMap<String, String>>();

        let database_engine = engine.as_ref().unwrap_or(&DatabaseEngine::Default);
        let (engine, engine_options) = match database_engine {
            DatabaseEngine::Default => ("default", BTreeMap::default()),
            DatabaseEngine::Share => ("share", BTreeMap::default()),
        };

        Ok(DatabaseMeta {
            engine: engine.to_string(),
            engine_options,
            options,
            ..Default::default()
        })
    }
}
