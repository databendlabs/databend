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
use std::collections::HashSet;
use std::iter;
use std::sync::Arc;

use databend_common_ast::ast::AddColumnOption as AstAddColumnOption;
use databend_common_ast::ast::AlterTableAction;
use databend_common_ast::ast::AlterTableStmt;
use databend_common_ast::ast::AnalyzeTableStmt;
use databend_common_ast::ast::AttachTableStmt;
use databend_common_ast::ast::ClusterOption;
use databend_common_ast::ast::ClusterType;
use databend_common_ast::ast::ClusterType as AstClusterType;
use databend_common_ast::ast::ColumnDefinition;
use databend_common_ast::ast::ColumnExpr;
use databend_common_ast::ast::CompactTarget;
use databend_common_ast::ast::ConstraintDefinition;
use databend_common_ast::ast::ConstraintType as AstConstraintType;
use databend_common_ast::ast::CreateTableSource;
use databend_common_ast::ast::CreateTableStmt;
use databend_common_ast::ast::DescribeTableStmt;
use databend_common_ast::ast::DropTableStmt;
use databend_common_ast::ast::Engine;
use databend_common_ast::ast::ExistsTableStmt;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::ModifyColumnAction;
use databend_common_ast::ast::OptimizeTableAction as AstOptimizeTableAction;
use databend_common_ast::ast::OptimizeTableStmt;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::RenameTableStmt;
use databend_common_ast::ast::ShowCreateTableStmt;
use databend_common_ast::ast::ShowDropTablesStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowStatisticsStmt;
use databend_common_ast::ast::ShowStatsTarget;
use databend_common_ast::ast::ShowTablesStatusStmt;
use databend_common_ast::ast::ShowTablesStmt;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableIndexDefinition;
use databend_common_ast::ast::TableIndexType as AstTableIndexType;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::TableType;
use databend_common_ast::ast::TruncateTableStmt;
use databend_common_ast::ast::TypeName;
use databend_common_ast::ast::UndropTableStmt;
use databend_common_ast::ast::UriLocation;
use databend_common_ast::ast::VacuumDropTableStmt;
use databend_common_ast::ast::VacuumTableStmt;
use databend_common_ast::ast::VacuumTemporaryFiles;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::CompactionLimits;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AutoIncrementExpr;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_schema_type;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::Constraint;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::storage::StorageParams;
use databend_common_pipeline::core::SharedLockGuard;
use databend_common_storage::check_operator;
use databend_common_storage::init_operator;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_ENGINE_META;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_PREFIX;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_DATA_URI;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use databend_storages_common_table_meta::table::TableCompression;
use databend_storages_common_table_meta::table::is_reserved_opt_key;
use derive_visitor::DriveMut;
use log::debug;
use opendal::Operator;
use uuid::Uuid;

use crate::BindContext;
use crate::ClusterKeyNormalizer;
use crate::DefaultExprBinder;
use crate::Planner;
use crate::SelectBuilder;
use crate::binder::Binder;
use crate::binder::ColumnBindingBuilder;
use crate::binder::ConstraintExprBinder;
use crate::binder::Visibility;
use crate::binder::get_storage_params_from_options;
use crate::binder::parse_storage_params_from_uri;
use crate::binder::scalar::ScalarBinder;
use crate::optimizer::ir::SExpr;
use crate::parse_computed_expr_to_string;
use crate::planner::binder::ddl::database::DEFAULT_STORAGE_CONNECTION;
use crate::planner::binder::ddl::database::DEFAULT_STORAGE_PATH;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::resolve_type_name;
use crate::plans::AddColumnOption;
use crate::plans::AddTableColumnPlan;
use crate::plans::AddTableConstraintPlan;
use crate::plans::AddTableRowAccessPolicyPlan;
use crate::plans::AlterTableClusterKeyPlan;
use crate::plans::AnalyzeTablePlan;
use crate::plans::CreateTablePlan;
use crate::plans::CreateTableRefPlan;
use crate::plans::DescribeTablePlan;
use crate::plans::DropAllTableRowAccessPoliciesPlan;
use crate::plans::DropTableClusterKeyPlan;
use crate::plans::DropTableColumnPlan;
use crate::plans::DropTableConstraintPlan;
use crate::plans::DropTablePlan;
use crate::plans::DropTableRefPlan;
use crate::plans::DropTableRowAccessPolicyPlan;
use crate::plans::ExistsTablePlan;
use crate::plans::ModifyColumnAction as ModifyColumnActionInPlan;
use crate::plans::ModifyTableColumnPlan;
use crate::plans::ModifyTableCommentPlan;
use crate::plans::ModifyTableConnectionPlan;
use crate::plans::OptimizeCompactBlock;
use crate::plans::OptimizeCompactSegmentPlan;
use crate::plans::OptimizePurgePlan;
use crate::plans::Plan;
use crate::plans::ReclusterPlan;
use crate::plans::RefreshTableCachePlan;
use crate::plans::RelOperator;
use crate::plans::RenameTableColumnPlan;
use crate::plans::RenameTablePlan;
use crate::plans::RevertTablePlan;
use crate::plans::RewriteKind;
use crate::plans::SetOptionsPlan;
use crate::plans::ShowCreateTablePlan;
use crate::plans::SwapTablePlan;
use crate::plans::TruncateTablePlan;
use crate::plans::UndropTablePlan;
use crate::plans::UnsetOptionsPlan;
use crate::plans::VacuumDropTableOption;
use crate::plans::VacuumDropTablePlan;
use crate::plans::VacuumTableOption;
use crate::plans::VacuumTablePlan;
use crate::plans::VacuumTemporaryFilesPlan;

pub(in crate::planner::binder) struct AnalyzeCreateTableResult {
    pub(in crate::planner::binder) schema: TableSchemaRef,
    pub(in crate::planner::binder) field_comments: Vec<String>,
    pub(in crate::planner::binder) table_indexes: Option<BTreeMap<String, TableIndex>>,
    pub(in crate::planner::binder) table_constraints: Option<BTreeMap<String, Constraint>>,
}

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_tables(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowTablesStmt,
    ) -> Result<Plan> {
        let ShowTablesStmt {
            catalog,
            database,
            full,
            limit,
            with_history,
        } = stmt;

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => {
                // check in check_database_exist
                normalize_identifier(ident, &self.name_resolution_ctx).name
            }
        };
        let database = self.check_database_exist(catalog, database).await?;

        let mut select_builder = if stmt.with_history {
            SelectBuilder::from(&format!(
                "{}.system.tables_with_history",
                catalog_name.to_lowercase()
            ))
        } else {
            SelectBuilder::from(&format!("{}.system.tables", catalog_name.to_lowercase()))
        };

        if *full {
            select_builder
                .with_column("name AS Tables")
                .with_column("'BASE TABLE' AS Table_type")
                .with_column("database AS Database")
                .with_column("catalog AS Catalog")
                .with_column("owner")
                .with_column("engine")
                .with_column("cluster_by AS Cluster_by")
                .with_column("created_on AS create_time");
            if *with_history {
                select_builder.with_column("dropped_on AS drop_time");
            }

            select_builder
                .with_column("num_rows")
                .with_column("data_size")
                .with_column("data_compressed_size")
                .with_column("index_size");
        } else {
            select_builder.with_column(format!("name AS `Tables_in_{database}`"));
            if *with_history {
                select_builder.with_column("dropped_on AS drop_time");
            };
        }

        select_builder
            .with_order_by("catalog")
            .with_order_by("database")
            .with_order_by("name");

        select_builder.with_filter(format!("database = '{database}'"));
        select_builder.with_filter("table_type = 'BASE TABLE'".to_string());

        select_builder.with_filter(format!("catalog = '{catalog_name}'"));
        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show tables rewrite to: {:?}", query);
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowTables(catalog_name, database),
        )
        .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_create_table(
        &mut self,
        stmt: &ShowCreateTableStmt,
    ) -> Result<Plan> {
        let ShowCreateTableStmt {
            catalog,
            database,
            table,
            with_quoted_ident,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Table", DataType::String),
            DataField::new("Create Table", DataType::String),
        ]);
        Ok(Plan::ShowCreateTable(Box::new(ShowCreateTablePlan {
            catalog,
            database,
            table,
            schema,
            with_quoted_ident: *with_quoted_ident,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_describe_table(
        &mut self,
        stmt: &DescribeTableStmt,
    ) -> Result<Plan> {
        let DescribeTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Field", DataType::String),
            DataField::new("Type", DataType::String),
            DataField::new("Null", DataType::String),
            DataField::new("Default", DataType::String),
            DataField::new("Extra", DataType::String),
        ]);

        Ok(Plan::DescribeTable(Box::new(DescribeTablePlan {
            catalog,
            database,
            table,
            schema,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_statistics(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowStatisticsStmt,
    ) -> Result<Plan> {
        let ShowStatisticsStmt {
            catalog,
            database,
            target,
        } = stmt;

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => {
                let catalog = normalize_identifier(ident, &self.name_resolution_ctx).name;
                self.ctx.get_catalog(&catalog).await?;
                catalog
            }
        };
        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        let mut select_builder = SelectBuilder::from(&format!("{catalog_name}.system.statistics"));

        let database = match database {
            None => self.ctx.get_current_database(),
            Some(ident) => {
                let database = normalize_identifier(ident, &self.name_resolution_ctx).name;
                catalog
                    .get_database(&self.ctx.get_tenant(), &database)
                    .await?;
                database
            }
        };
        select_builder.with_filter(format!("database = '{database}'"));

        if let ShowStatsTarget::Table(table) = target {
            let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;
            catalog
                .get_table(&self.ctx.get_tenant(), database.as_str(), &table_name)
                .await?;
            select_builder.with_filter(format!("table = '{table_name}'"));
        }

        select_builder
            .with_column("database")
            .with_column("table")
            .with_column("column_name")
            .with_column("stats_row_count")
            .with_column("actual_row_count")
            .with_column("distinct_count")
            .with_column("null_count")
            .with_column("min")
            .with_column("max")
            .with_column("avg_size")
            .with_column("histogram");

        select_builder
            .with_order_by("database")
            .with_order_by("table")
            .with_order_by("column_name");

        let query = select_builder.build();

        debug!("show statistics rewrite to: {:?}", query);
        self.bind_rewrite_to_query(bind_context, query.as_str(), RewriteKind::ShowStatistics)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_tables_status(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowTablesStatusStmt,
    ) -> Result<Plan> {
        let ShowTablesStatusStmt { database, limit } = stmt;

        let default_catalog = self.ctx.get_current_catalog();
        let database = self.check_database_exist(&None, database).await?;

        let select_cols = "name AS Name, engine AS Engine, 0 AS Version, \
        NULL AS Row_format, num_rows AS Rows, NULL AS Avg_row_length, data_size AS Data_length, \
        NULL AS Max_data_length, index_size AS Index_length, NULL AS Data_free, NULL AS Auto_increment, \
        created_on AS Create_time, NULL AS Update_time, NULL AS Check_time, NULL AS Collation, \
        NULL AS Checksum, '' AS Comment, cluster_by as Cluster_by"
            .to_string();

        // Use `system.tables` AS the "base" table to construct the result-set of `SHOW TABLE STATUS ..`
        //
        // To constraint the schema of the final result-set,
        //  `(select ${select_cols} from default.system.tables where ..)`
        // is used AS a derived table.
        // (unlike mysql, alias of derived table is not required in databend).
        let query = match limit {
            None => format!(
                "SELECT {} FROM {}.system.tables WHERE database = '{}' ORDER BY Name",
                select_cols, default_catalog, database
            ),
            Some(ShowLimit::Like { pattern }) => format!(
                "SELECT * from (SELECT {} FROM {}.system.tables WHERE database = '{}') \
            WHERE Name LIKE '{}' ORDER BY Name",
                select_cols, default_catalog, database, pattern
            ),
            Some(ShowLimit::Where { selection }) => format!(
                "SELECT * from (SELECT {} FROM {}.system.tables WHERE database = '{}') \
            WHERE ({}) ORDER BY Name",
                select_cols, default_catalog, database, selection
            ),
        };

        let tokens = tokenize_sql(query.as_str())?;
        let (stmt, _) = parse_sql(&tokens, self.dialect)?;
        self.bind_statement(bind_context, &stmt).await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_drop_tables(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowDropTablesStmt,
    ) -> Result<Plan> {
        let ShowDropTablesStmt { database, limit } = stmt;

        let default_catalog = self.ctx.get_default_catalog()?.name();
        let database = self.check_database_exist(&None, database).await?;

        let mut select_builder =
            SelectBuilder::from(&format!("{}.system.tables_with_history", default_catalog));

        select_builder
            .with_column("name AS Tables")
            .with_column("'BASE TABLE' AS Table_type")
            .with_column("database AS Database")
            .with_column("catalog AS Catalog")
            .with_column("engine")
            .with_column("created_on AS create_time");
        select_builder.with_column("dropped_on AS drop_time");

        select_builder
            .with_column("num_rows")
            .with_column("data_size")
            .with_column("data_compressed_size")
            .with_column("index_size");

        select_builder
            .with_order_by("catalog")
            .with_order_by("database")
            .with_order_by("name");

        select_builder.with_filter(format!("database = '{database}'"));
        select_builder.with_filter("dropped_on IS NOT NULL".to_string());

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };

        debug!("show drop tables rewrite to: {:?}", query);
        let catalog = self.ctx.get_current_catalog();
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowTables(catalog, database),
        )
        .await
    }

    #[async_backtrace::framed]
    pub async fn check_database_exist(
        &mut self,
        catalog: &Option<Identifier>,
        database: &Option<Identifier>,
    ) -> Result<String> {
        let ctl_name = match catalog {
            Some(ctl) => ctl.to_string(),
            None => self.ctx.get_current_catalog(),
        };
        match database {
            None => Ok(self.ctx.get_current_database()),
            Some(ident) => {
                let database = normalize_identifier(ident, &self.name_resolution_ctx).name;
                self.ctx
                    .get_catalog(&ctl_name)
                    .await?
                    .get_database(&self.ctx.get_tenant(), &database)
                    .await?;
                Ok(database)
            }
        }
    }

    async fn as_query_plan(&mut self, query: &Query) -> Result<Plan> {
        let stmt = Statement::Query(Box::new(query.clone()));
        let mut bind_context = BindContext::new();
        self.bind_statement(&mut bind_context, &stmt).await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_table(
        &mut self,
        stmt: &CreateTableStmt,
    ) -> Result<Plan> {
        let CreateTableStmt {
            create_option,
            catalog,
            database,
            table,
            source,
            table_options,
            cluster_by,
            as_query,
            table_type,
            engine,
            uri_location,
            iceberg_table_partition,
            table_properties,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let catalog = self.ctx.get_catalog(&catalog).await?;

        let mut options: BTreeMap<String, String> = BTreeMap::new();

        // FUSE tables can inherit database connection defaults for external storage
        let engine = engine.unwrap_or(catalog.default_table_engine());

        // Construct a UriLocation from database defaults if table doesn't have explicit location
        let uri_location_to_use: Option<UriLocation> = if uri_location.is_none()
            && matches!(engine, Engine::Fuse)
        {
            if let Ok(database_info) = catalog
                .get_database(&self.ctx.get_tenant(), &database)
                .await
            {
                // Extract database-level default connection options
                let default_connection_name =
                    database_info.options().get(DEFAULT_STORAGE_CONNECTION);
                let default_path = database_info.options().get(DEFAULT_STORAGE_PATH);

                // If both database defaults exist, construct UriLocation
                if let (Some(connection_name), Some(path)) = (default_connection_name, default_path)
                {
                    // Get the connection object to access its storage_params
                    match self.ctx.get_connection(connection_name).await {
                        Ok(connection) => {
                            // Construct UriLocation using the database defaults
                            match UriLocation::from_uri(path.clone(), connection.storage_params) {
                                Ok(uri) => Some(uri),
                                Err(e) => {
                                    return Err(ErrorCode::BadArguments(format!(
                                        "Failed to parse database default storage path '{}': {}",
                                        path, e
                                    )));
                                }
                            }
                        }
                        Err(e) => {
                            return Err(ErrorCode::BadArguments(format!(
                                "Database default connection '{}' does not exist: {}",
                                connection_name, e
                            )));
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            // Use the provided uri_location by cloning it
            uri_location.clone()
        };

        if catalog.support_partition() != (engine == Engine::Iceberg) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "Catalog '{}' engine type is {:?} but table {} engine type is {}",
                catalog.name(),
                catalog.info().catalog_type(),
                table,
                engine
            )));
        }

        let mut engine_options: BTreeMap<String, String> = BTreeMap::new();
        // Table-specific options override database defaults
        for table_option in table_options.iter() {
            self.insert_table_option_with_validation(
                &mut options,
                table_option.0.to_lowercase(),
                table_option.1.to_string(),
            )?;
        }

        let table_properties = match &table_properties {
            Some(props) => {
                let mut iceberg_table_options = BTreeMap::new();
                props.iter().try_for_each(|(k, v)| {
                    self.insert_table_option_with_validation(
                        &mut iceberg_table_options,
                        k.to_lowercase(),
                        v.to_string(),
                    )
                })?;
                Some(iceberg_table_options)
            }
            None => None,
        };

        let table_partition = iceberg_table_partition.as_ref().map(|partitions| {
            partitions
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<String>>()
        });

        let mut storage_params = match (uri_location_to_use.as_ref(), engine) {
            (Some(uri), Engine::Fuse) => {
                let mut uri = UriLocation {
                    protocol: uri.protocol.clone(),
                    name: uri.name.clone(),
                    path: uri.path.clone(),
                    connection: uri.connection.clone(),
                };
                let sp = parse_storage_params_from_uri(
                    &mut uri,
                    Some(self.ctx.as_ref()),
                    "when create TABLE with external location",
                )
                .await?;

                // create a temporary op to check if params is correct
                let op = init_operator(&sp)?;
                check_operator(&op, &sp).await?;

                // Verify essential privileges.
                // The permission check might fail for reasons other than the permissions themselves,
                // such as network communication issues.
                verify_external_location_privileges(op).await?;
                Some(sp)
            }
            (Some(uri), _) => Err(ErrorCode::BadArguments(format!(
                "Incorrect CREATE query: CREATE TABLE with external location is only supported for FUSE engine, but got {:?} for {:?}",
                engine, uri
            )))?,
            _ => None,
        };

        match table_type {
            TableType::Normal => {}
            TableType::Transient => {
                let _ = options.insert("TRANSIENT".to_owned(), "T".to_owned());
            }
            TableType::Temporary => {
                if engine != Engine::Fuse && engine != Engine::Memory {
                    return Err(ErrorCode::BadArguments(
                        "Temporary table is only supported for FUSE and MEMORY engine",
                    ));
                }
                let _ = options.insert(
                    OPT_KEY_TEMP_PREFIX.to_string(),
                    self.ctx.get_temp_table_prefix()?,
                );
            }
        };

        // todo(geometry): remove this when geometry stable.
        if let Some(CreateTableSource::Columns { columns, .. }) = &source {
            if columns
                .iter()
                .any(|col| matches!(col.data_type, TypeName::Geometry | TypeName::Geography))
                && !self.ctx.get_settings().get_enable_geo_create_table()?
            {
                return Err(ErrorCode::GeometryError(
                    "Create table using the geometry/geography type is an experimental feature. \
                    You can `set enable_geo_create_table=1` to use this feature. \
                    We do not guarantee its compatibility until we doc this feature.",
                ));
            }
        }

        // Build table schema
        let (
            AnalyzeCreateTableResult {
                schema,
                field_comments,
                table_indexes,
                table_constraints,
            },
            as_query_plan,
        ) = match (&source, &as_query) {
            (Some(source), None) => {
                // `CREATE TABLE` without `AS SELECT ...`
                let result = self.analyze_create_table_schema(&table, source).await?;
                (result, None)
            }
            (None, Some(query)) => {
                // `CREATE TABLE AS SELECT ...` without column definitions
                let as_query_plan = self.as_query_plan(query).await?;
                let bind_context = as_query_plan.bind_context().unwrap();
                let mut schema = bind_context.output_table_schema(self.metadata.clone())?;
                let mut fields = schema.fields().clone();
                for field in fields.iter_mut() {
                    if field.data_type == TableDataType::Null {
                        field.data_type = TableDataType::String.wrap_nullable();
                    } else if !field.data_type().is_nullable_or_null() && !self.is_column_not_null()
                    {
                        field.data_type = field.data_type().clone().wrap_nullable();
                    }
                }
                schema = TableSchemaRefExt::create(fields);

                Self::validate_create_table_schema(&schema)?;
                (
                    AnalyzeCreateTableResult {
                        schema,
                        field_comments: vec![],
                        table_indexes: None,
                        table_constraints: None,
                    },
                    Some(Box::new(as_query_plan)),
                )
            }
            (Some(source), Some(query)) => {
                // e.g. `CREATE TABLE t (i INT) AS SELECT * from old_t` with columns specified
                let result = self.analyze_create_table_schema(&table, source).await?;
                let as_query_plan = self.as_query_plan(query).await?;
                let bind_context = as_query_plan.bind_context().unwrap();
                let query_fields: Vec<TableField> = bind_context
                    .columns
                    .iter()
                    .map(|column_binding| {
                        Ok(TableField::new(
                            &column_binding.column_name,
                            infer_schema_type(&column_binding.data_type)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if result.schema.fields().len() != query_fields.len() {
                    return Err(ErrorCode::BadArguments("Number of columns does not match"));
                }
                Self::validate_create_table_schema(&result.schema)?;
                (result, Some(Box::new(as_query_plan)))
            }
            _ => {
                let as_query_plan = if let Some(query) = as_query {
                    let as_query_plan = self.as_query_plan(query).await?;
                    Some(Box::new(as_query_plan))
                } else {
                    None
                };
                match engine {
                    Engine::Iceberg => {
                        let sp =
                            get_storage_params_from_options(self.ctx.as_ref(), &options).await?;
                        let (table_schema, _) =
                            self.ctx.load_datalake_schema("iceberg", &sp).await?;
                        // the first version of current iceberg table do not need to persist the storage_params,
                        // since we get it from table options location and connection when load table each time.
                        // we do this in case we change this idea.
                        storage_params = Some(sp);
                        (
                            AnalyzeCreateTableResult {
                                schema: Arc::new(table_schema),
                                field_comments: vec![],
                                table_indexes: None,
                                table_constraints: None,
                            },
                            as_query_plan,
                        )
                    }
                    Engine::Delta => {
                        let sp =
                            get_storage_params_from_options(self.ctx.as_ref(), &options).await?;
                        let (table_schema, meta) =
                            self.ctx.load_datalake_schema("delta", &sp).await?;
                        // the first version of current iceberg table do not need to persist the storage_params,
                        // since we get it from table options location and connection when load table each time.
                        // we do this in case we change this idea.
                        storage_params = Some(sp);
                        engine_options.insert(OPT_KEY_ENGINE_META.to_lowercase().to_string(), meta);
                        (
                            AnalyzeCreateTableResult {
                                schema: Arc::new(table_schema),
                                field_comments: vec![],
                                table_indexes: None,
                                table_constraints: None,
                            },
                            as_query_plan,
                        )
                    }
                    _ => Err(ErrorCode::BadArguments(
                        "Incorrect CREATE query: required list of column descriptions or AS section or SELECT or ICEBERG/DELTA table engine",
                    ))?,
                }
            }
        };

        if engine == Engine::Memory {
            let db = catalog
                .get_database(&self.ctx.get_tenant(), &database)
                .await?;
            let db_id = db.get_db_info().database_id.db_id;
            options.insert(OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());
        }

        if engine == Engine::Fuse {
            // Currently, [Table] can not accesses its database id yet, thus
            // here we keep the db id AS an entry of `table_meta.options`.
            //
            // To make the unit/stateless test cases (`show create ..`) easier,
            // here we care about the FUSE engine only.
            //
            // Later, when database id is kept, let say in `TableInfo`, we can
            // safely eliminate this "FUSE" constant and the table meta option entry.
            let db = catalog
                .get_database(&self.ctx.get_tenant(), &database)
                .await?;
            let db_id = db.get_db_info().database_id.db_id;
            options.insert(OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());

            let config = GlobalConfig::instance();
            let is_blocking_fs = matches!(
                storage_params.as_ref().unwrap_or(&config.storage.params),
                StorageParams::Fs(_)
            );

            // we should persist the storage format and compression type instead of using the default value in fuse table
            if !options.contains_key(OPT_KEY_STORAGE_FORMAT) {
                let default_storage_format =
                    match config.query.common.default_storage_format.as_str() {
                        "" | "auto" => {
                            if is_blocking_fs {
                                "native"
                            } else {
                                "parquet"
                            }
                        }
                        _ => config.query.common.default_storage_format.as_str(),
                    };
                options.insert(
                    OPT_KEY_STORAGE_FORMAT.to_owned(),
                    default_storage_format.to_owned(),
                );
            }

            if !options.contains_key(OPT_KEY_TABLE_COMPRESSION) {
                let default_compression = match config.query.common.default_compression.as_str() {
                    "" | "auto" => {
                        if is_blocking_fs {
                            "lz4"
                        } else {
                            "zstd"
                        }
                    }
                    _ => config.query.common.default_compression.as_str(),
                };
                options.insert(
                    OPT_KEY_TABLE_COMPRESSION.to_owned(),
                    default_compression.to_owned(),
                );
            } else {
                // validate the compression type
                let _: TableCompression = options
                    .get(OPT_KEY_TABLE_COMPRESSION)
                    .ok_or_else(|| {
                        ErrorCode::BadArguments("Table compression type is not specified")
                    })?
                    .as_str()
                    .try_into()?;
            }
        } else if table_indexes.is_some() {
            return Err(ErrorCode::UnsupportedIndex(format!(
                "Table engine {} does not support create index",
                engine
            )));
        } else if table_constraints.is_some() {
            return Err(ErrorCode::UnsupportedIndex(format!(
                "Table engine {} does not support create constraints",
                engine
            )));
        }

        let mut cluster_key = None;
        if let Some(cluster_opt) = cluster_by {
            let keys = self
                .analyze_cluster_keys(cluster_opt, schema.clone())
                .await?;
            if !keys.is_empty() {
                options.insert(
                    OPT_KEY_CLUSTER_TYPE.to_owned(),
                    cluster_opt.cluster_type.to_string().to_lowercase(),
                );
                cluster_key = Some(format!("({})", keys.join(", ")));
            }
        }

        let plan = CreateTablePlan {
            create_option: create_option.clone().into(),
            tenant: self.ctx.get_tenant(),
            catalog: catalog.name().clone(),
            database: database.clone(),
            table,
            schema: schema.clone(),
            engine,
            engine_options,
            storage_params,
            options,
            table_properties,
            table_partition,
            field_comments,
            cluster_key,
            as_select: as_query_plan,
            table_indexes,
            table_constraints,
            attached_columns: None,
        };
        Ok(Plan::CreateTable(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_attach_table(
        &mut self,
        stmt: &AttachTableStmt,
    ) -> Result<Plan> {
        let (catalog, database, table) =
            self.normalize_object_identifier_triple(&stmt.catalog, &stmt.database, &stmt.table);

        let mut path = stmt.uri_location.path.clone();
        // First, to make it easy for users to use, path = "/testbucket/data/1/2" and path = "/testbucket/data/1/2/" are both legal
        // So we need to remove the last "/"
        if path.ends_with('/') {
            path.pop();
        }
        // Then, split the path into two parts, the first part is the root, and the second part is the storage_prefix
        // For example, path = "/testbucket/data/1/2", then root = "/testbucket/data/", storage_prefix = "1/2"
        // root is used by OpenDAL operator, storage_prefix is used to specify the storage location of the table
        // Note that the root must end with "/", and the storage_prefix must not start or end with "/"
        let mut parts = path.split('/').collect::<Vec<_>>();
        if parts.len() < 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid path: {}",
                stmt.uri_location
            )));
        }
        let storage_prefix = parts.split_off(parts.len() - 2).join("/");
        let root = format!("{}/", parts.join("/"));
        let mut options = BTreeMap::new();
        options.insert(OPT_KEY_STORAGE_PREFIX.to_string(), storage_prefix);

        // keep a copy of table data uri_location, will be used in "show create table"
        options.insert(
            OPT_KEY_TABLE_ATTACHED_DATA_URI.to_string(),
            format!("{}", stmt.uri_location.mask()),
        );

        let mut uri = stmt.uri_location.clone();
        uri.path = root;
        let sp =
            parse_storage_params_from_uri(&mut uri, Some(self.ctx.as_ref()), "when ATTACH TABLE")
                .await?;

        // create a temporary op to check if params is correct
        let op = init_operator(&sp)?;
        check_operator(&op, &sp).await?;

        Ok(Plan::CreateTable(Box::new(CreateTablePlan {
            create_option: CreateOption::Create,
            tenant: self.ctx.get_tenant(),
            catalog,
            database,
            table,
            schema: Arc::new(TableSchema::default()),
            engine: Engine::Fuse,
            engine_options: BTreeMap::new(),
            storage_params: Some(sp),
            options,
            table_properties: None,
            table_partition: None,
            field_comments: vec![],
            cluster_key: None,
            as_select: None,
            table_indexes: None,
            table_constraints: None,
            attached_columns: stmt.columns_opt.clone(),
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_table(
        &mut self,
        stmt: &DropTableStmt,
    ) -> Result<Plan> {
        let DropTableStmt {
            if_exists,
            catalog,
            database,
            table,
            all,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::DropTable(Box::new(DropTablePlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
            table,
            all: *all,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_undrop_table(
        &mut self,
        stmt: &UndropTableStmt,
    ) -> Result<Plan> {
        let UndropTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::UndropTable(Box::new(UndropTablePlan {
            tenant,
            catalog,
            database,
            table,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_table(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &AlterTableStmt,
    ) -> Result<Plan> {
        let AlterTableStmt {
            if_exists,
            table_reference,
            action,
        } = stmt;

        let tenant = self.ctx.get_tenant();

        let (catalog, database, table, branch) =
            if let TableReference::Table { table, .. } = table_reference {
                let branch = table
                    .branch
                    .as_ref()
                    .map(|b| normalize_identifier(b, &self.name_resolution_ctx).name);
                let (catalog, database, table) = self.normalize_object_identifier_triple(
                    &table.catalog,
                    &table.database,
                    &table.table,
                );
                (catalog, database, table, branch)
            } else {
                return Err(ErrorCode::Internal(
                    "should not happen, parser should have report error already",
                ));
            };

        if branch.is_some() {
            if !self
                .ctx
                .get_settings()
                .get_enable_experimental_table_ref()
                .unwrap_or_default()
            {
                return Err(ErrorCode::Unimplemented(
                    "Table ref is an experimental feature, `set enable_experimental_table_ref=1` to use this feature",
                ));
            }

            if !matches!(
                action,
                AlterTableAction::AddColumn { .. }
                    | AlterTableAction::ModifyColumn { .. }
                    | AlterTableAction::DropColumn { .. }
                    | AlterTableAction::RenameColumn { .. }
                    | AlterTableAction::AlterTableClusterKey { .. }
                    | AlterTableAction::DropTableClusterKey
            ) {
                return Err(ErrorCode::SemanticError(
                    "ALTER TABLE <table>/<branch> only supports ADD/MODIFY/DROP/RENAME COLUMN, ALTER/DROP CLUSTER KEY",
                ));
            }
        }

        match action {
            AlterTableAction::RenameTable { new_table } => {
                Ok(Plan::RenameTable(Box::new(RenameTablePlan {
                    tenant,
                    if_exists: *if_exists,
                    new_database: database.clone(),
                    new_table: normalize_identifier(new_table, &self.name_resolution_ctx).name,
                    catalog,
                    database,
                    table,
                })))
            }
            AlterTableAction::SwapWith { target_table } => {
                Ok(Plan::SwapTable(Box::new(SwapTablePlan {
                    tenant,
                    if_exists: *if_exists,
                    catalog: catalog.clone(),
                    database: database.clone(),
                    table: table.clone(),
                    target_table: normalize_identifier(target_table, &self.name_resolution_ctx)
                        .name,
                })))
            }
            AlterTableAction::ModifyTableComment { new_comment } => {
                Ok(Plan::ModifyTableComment(Box::new(ModifyTableCommentPlan {
                    if_exists: *if_exists,
                    new_comment: new_comment.to_string(),
                    catalog,
                    database,
                    table,
                })))
            }
            AlterTableAction::ModifyConnection { new_connection } => Ok(
                Plan::ModifyTableConnection(Box::new(ModifyTableConnectionPlan {
                    new_connection: new_connection.clone(),
                    catalog,
                    database,
                    table,
                })),
            ),
            AlterTableAction::RenameColumn {
                old_column,
                new_column,
            } => {
                let schema = self
                    .ctx
                    .get_table_with_batch(&catalog, &database, &table, branch.as_deref(), None)
                    .await?
                    .schema();
                let (new_schema, old_column, new_column) = self
                    .analyze_rename_column(old_column, new_column, schema)
                    .await?;
                Ok(Plan::RenameTableColumn(Box::new(RenameTableColumnPlan {
                    tenant: self.ctx.get_tenant(),
                    catalog,
                    database,
                    table,
                    branch,
                    schema: new_schema,
                    old_column,
                    new_column,
                })))
            }
            AlterTableAction::AddColumn {
                column,
                option: ast_option,
            } => {
                let schema = self
                    .ctx
                    .get_table_with_batch(&catalog, &database, &table, branch.as_deref(), None)
                    .await?
                    .schema();
                let (field, comment, is_deterministic, is_nextval, is_autoincrement) =
                    self.analyze_add_column(column, schema).await?;
                let option = match ast_option {
                    AstAddColumnOption::First => AddColumnOption::First,
                    AstAddColumnOption::After(ident) => AddColumnOption::After(
                        normalize_identifier(ident, &self.name_resolution_ctx).name,
                    ),
                    AstAddColumnOption::End => AddColumnOption::End,
                };
                Ok(Plan::AddTableColumn(Box::new(AddTableColumnPlan {
                    tenant: self.ctx.get_tenant(),
                    catalog,
                    database,
                    table,
                    branch,
                    field,
                    comment,
                    option,
                    is_deterministic,
                    is_nextval,
                    is_autoincrement,
                })))
            }
            AlterTableAction::AddConstraint { constraint } => {
                let schema = self
                    .ctx
                    .get_table(&catalog, &database, &table)
                    .await?
                    .schema();
                let (constraint_name, constraint) = self
                    .analyze_constraints(&table, schema, iter::once(constraint))
                    .await?
                    .pop_first()
                    .unwrap();

                Ok(Plan::AddTableConstraint(Box::new(AddTableConstraintPlan {
                    tenant,
                    catalog,
                    database,
                    table,
                    constraint_name,
                    constraint,
                })))
            }
            AlterTableAction::DropConstraint { constraint_name } => {
                let constraint_name = self.normalize_object_identifier(constraint_name);

                Ok(Plan::DropTableConstraint(Box::new(
                    DropTableConstraintPlan {
                        tenant,
                        catalog,
                        database,
                        table,
                        constraint_name,
                    },
                )))
            }
            AlterTableAction::ModifyColumn { action } => {
                let mut lock_guard = None;
                let action_in_plan = match action {
                    ModifyColumnAction::SetMaskingPolicy(column, name, using_columns) => {
                        let column = self.normalize_object_identifier(column);
                        if let Some(columns) = using_columns {
                            if columns.len() < 2 {
                                return Err(ErrorCode::InvalidArgument(format!(
                                    "Invalid number of arguments for attaching policy '{}' to '{}': \
                                     expected at least 2 arguments (masked column + condition columns), \
                                     got {} argument(s)",
                                    name,
                                    table,
                                    columns.len()
                                )));
                            }

                            let first_column = self.normalize_object_identifier(&columns[0]);
                            if first_column != column {
                                return Err(ErrorCode::InvalidArgument(format!(
                                    "First column argument to masking policy does not match the masked column '{}'. \
                                     The first column in USING clause must be the column being masked.",
                                    column
                                )));
                            }

                            let cols = columns
                                .iter()
                                .map(|col| self.normalize_object_identifier(col))
                                .collect();
                            ModifyColumnActionInPlan::SetMaskingPolicy(name.to_string(), cols)
                        } else {
                            ModifyColumnActionInPlan::SetMaskingPolicy(name.to_string(), vec![
                                column,
                            ])
                        }
                    }
                    ModifyColumnAction::UnsetMaskingPolicy(column) => {
                        let column = self.normalize_object_identifier(column);
                        ModifyColumnActionInPlan::UnsetMaskingPolicy(column)
                    }
                    ModifyColumnAction::ConvertStoredComputedColumn(column) => {
                        let column = self.normalize_object_identifier(column);
                        ModifyColumnActionInPlan::ConvertStoredComputedColumn(column)
                    }
                    ModifyColumnAction::SetDataType(column_def_vec) => {
                        let mut field_and_comment = Vec::with_capacity(column_def_vec.len());
                        // try add lock table.
                        lock_guard = self
                            .ctx
                            .clone()
                            .acquire_table_lock(
                                &catalog,
                                &database,
                                &table,
                                &LockTableOption::LockWithRetry,
                            )
                            .await?
                            .map(SharedLockGuard::new);
                        let schema = self
                            .ctx
                            .get_table_with_batch(
                                &catalog,
                                &database,
                                &table,
                                branch.as_deref(),
                                None,
                            )
                            .await?
                            .schema();
                        for column in column_def_vec {
                            let (field, comment, _, _, _) =
                                self.analyze_add_column(column, schema.clone()).await?;
                            field_and_comment.push((field, comment));
                        }
                        ModifyColumnActionInPlan::SetDataType(field_and_comment)
                    }
                    ModifyColumnAction::Comment(column_comments) => {
                        let mut field_and_comment = Vec::with_capacity(column_comments.len());
                        let schema = self
                            .ctx
                            .get_table_with_batch(
                                &catalog,
                                &database,
                                &table,
                                branch.as_deref(),
                                None,
                            )
                            .await?
                            .schema();

                        for column_comment in column_comments {
                            let column = self.normalize_object_identifier(&column_comment.name);
                            let field = schema.field_with_name(&column)?.clone();
                            let comment = column_comment.comment.to_string();
                            field_and_comment.push((field, comment));
                        }
                        ModifyColumnActionInPlan::Comment(field_and_comment)
                    }
                };
                Ok(Plan::ModifyTableColumn(Box::new(ModifyTableColumnPlan {
                    catalog,
                    database,
                    table,
                    branch,
                    action: action_in_plan,
                    lock_guard,
                })))
            }
            AlterTableAction::DropColumn { column } => {
                let column = self.normalize_object_identifier(column);
                Ok(Plan::DropTableColumn(Box::new(DropTableColumnPlan {
                    catalog,
                    database,
                    table,
                    branch,
                    column,
                })))
            }
            AlterTableAction::AlterTableClusterKey { cluster_by } => {
                let tbl = self
                    .ctx
                    .get_table_with_batch(&catalog, &database, &table, branch.as_deref(), None)
                    .await?;
                // Branch tables currently only support LINEAR cluster key.
                if tbl.get_branch_info().is_some()
                    && !matches!(cluster_by.cluster_type, ClusterType::Linear)
                {
                    return Err(ErrorCode::AlterTableError(format!(
                        "Cluster key type '{}' is not supported for table branch. Only LINEAR is supported",
                        cluster_by.cluster_type,
                    )));
                }

                let cluster_keys = self.analyze_cluster_keys(cluster_by, tbl.schema()).await?;

                Ok(Plan::AlterTableClusterKey(Box::new(
                    AlterTableClusterKeyPlan {
                        tenant,
                        catalog,
                        database,
                        table,
                        branch,
                        cluster_keys,
                        cluster_type: cluster_by.cluster_type.to_string().to_lowercase(),
                    },
                )))
            }
            AlterTableAction::DropTableClusterKey => Ok(Plan::DropTableClusterKey(Box::new(
                DropTableClusterKeyPlan {
                    tenant,
                    catalog,
                    database,
                    table,
                    branch,
                },
            ))),
            AlterTableAction::ReclusterTable {
                is_final,
                selection,
                limit,
            } => Ok(Plan::ReclusterTable(Box::new(ReclusterPlan {
                catalog,
                database,
                table,
                limit: limit.map(|v| v as usize),
                selection: selection.clone(),
                is_final: *is_final,
            }))),
            AlterTableAction::FlashbackTo { point } => {
                let point = self.resolve_data_travel_point(bind_context, point)?;
                Ok(Plan::RevertTable(Box::new(RevertTablePlan {
                    tenant,
                    catalog,
                    database,
                    table,
                    point,
                })))
            }
            AlterTableAction::SetOptions { set_options } => {
                Ok(Plan::SetOptions(Box::new(SetOptionsPlan {
                    set_options: set_options.clone(),
                    catalog,
                    database,
                    table,
                })))
            }
            AlterTableAction::UnsetOptions { targets } => {
                Ok(Plan::UnsetOptions(Box::new(UnsetOptionsPlan {
                    options: targets.iter().map(|i| i.name.to_lowercase()).collect(),
                    catalog,
                    database,
                    table,
                })))
            }
            AlterTableAction::RefreshTableCache => {
                Ok(Plan::RefreshTableCache(Box::new(RefreshTableCachePlan {
                    tenant,
                    catalog,
                    database,
                    table,
                })))
            }
            AlterTableAction::AddRowAccessPolicy { columns, policy } => {
                if !self
                    .ctx
                    .get_settings()
                    .get_enable_experimental_row_access_policy()?
                {
                    return Err(ErrorCode::Unimplemented(
                        "Experimental Row Access Policy is unstable and may have compatibility issues. To use it, set enable_experimental_row_access_policy=1",
                    ));
                }
                let columns = columns
                    .iter()
                    .map(|c| self.normalize_identifier(c).name)
                    .collect();
                let policy = self.normalize_identifier(policy).name;
                Ok(Plan::AddTableRowAccessPolicy(Box::new(
                    AddTableRowAccessPolicyPlan {
                        tenant,
                        catalog,
                        database,
                        table,
                        columns,
                        policy,
                    },
                )))
            }
            AlterTableAction::DropRowAccessPolicy { policy } => {
                if !self
                    .ctx
                    .get_settings()
                    .get_enable_experimental_row_access_policy()?
                {
                    return Err(ErrorCode::Unimplemented(
                        "Experimental Row Access Policy is unstable and may have compatibility issues. To use it, set enable_experimental_row_access_policy=1",
                    ));
                }
                let policy = self.normalize_identifier(policy).name;
                Ok(Plan::DropTableRowAccessPolicy(Box::new(
                    DropTableRowAccessPolicyPlan {
                        tenant,
                        catalog,
                        database,
                        table,
                        policy,
                    },
                )))
            }
            AlterTableAction::DropAllRowAccessPolicies => {
                if !self
                    .ctx
                    .get_settings()
                    .get_enable_experimental_row_access_policy()?
                {
                    return Err(ErrorCode::Unimplemented(
                        "Experimental Row Access Policy is unstable and may have compatibility issues. To use it, set enable_experimental_row_access_policy=1",
                    ));
                }
                Ok(Plan::DropAllTableRowAccessPolicies(Box::new(
                    DropAllTableRowAccessPoliciesPlan {
                        tenant,
                        catalog,
                        database,
                        table,
                    },
                )))
            }
            AlterTableAction::CreateTableRef {
                ref_type,
                ref_name,
                travel_point,
                retain,
            } => {
                let navigation = if let Some(point) = travel_point {
                    Some(self.resolve_data_travel_point(bind_context, point)?)
                } else {
                    None
                };
                let ref_name = self.normalize_identifier(ref_name).name;
                Ok(Plan::CreateTableRef(Box::new(CreateTableRefPlan {
                    tenant,
                    catalog,
                    database,
                    table,
                    ref_type: ref_type.into(),
                    ref_name,
                    navigation,
                    retain: *retain,
                })))
            }
            AlterTableAction::DropTableRef { ref_type, ref_name } => {
                let ref_name = self.normalize_identifier(ref_name).name;
                Ok(Plan::DropTableRef(Box::new(DropTableRefPlan {
                    tenant,
                    catalog,
                    database,
                    table,
                    ref_type: ref_type.into(),
                    ref_name,
                })))
            }
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_rename_table(
        &mut self,
        stmt: &RenameTableStmt,
    ) -> Result<Plan> {
        let RenameTableStmt {
            if_exists,
            catalog,
            database,
            table,
            new_catalog,
            new_database,
            new_table,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, db_name, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let (new_catalog, new_database, new_table) =
            self.normalize_object_identifier_triple(new_catalog, new_database, new_table);

        if new_catalog != catalog || new_database != db_name {
            return Err(ErrorCode::BadArguments(
                "Rename table not allow modify catalog or database",
            )
            .set_span(database.as_ref().and_then(|ident| ident.span)));
        }

        Ok(Plan::RenameTable(Box::new(RenameTablePlan {
            tenant,
            if_exists: *if_exists,
            catalog,
            database: db_name,
            table,
            new_database,
            new_table,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_truncate_table(
        &mut self,
        stmt: &TruncateTableStmt,
    ) -> Result<Plan> {
        let TruncateTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::TruncateTable(Box::new(TruncateTablePlan {
            catalog,
            database,
            table,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_optimize_table(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &OptimizeTableStmt,
    ) -> Result<Plan> {
        let OptimizeTableStmt {
            catalog,
            database,
            table,
            action: ast_action,
            limit,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);
        let limit = limit.map(|v| v as usize);
        let plan = match ast_action {
            AstOptimizeTableAction::All => {
                let compact_block = RelOperator::CompactBlock(OptimizeCompactBlock {
                    catalog,
                    database,
                    table,
                    limit: CompactionLimits {
                        segment_limit: limit,
                        block_limit: None,
                    },
                });
                let s_expr = SExpr::create_leaf(Arc::new(compact_block));
                Plan::OptimizeCompactBlock {
                    s_expr: Box::new(s_expr),
                    need_purge: true,
                }
            }
            AstOptimizeTableAction::Purge { before } => {
                let instant = if let Some(point) = before {
                    let point = self.resolve_data_travel_point(bind_context, point)?;
                    Some(point)
                } else {
                    None
                };
                Plan::OptimizePurge(Box::new(OptimizePurgePlan {
                    catalog,
                    database,
                    table,
                    instant,
                    num_snapshot_limit: limit,
                }))
            }
            AstOptimizeTableAction::Compact { target } => match target {
                CompactTarget::Block => {
                    let compact_block = RelOperator::CompactBlock(OptimizeCompactBlock {
                        catalog,
                        database,
                        table,
                        limit: CompactionLimits {
                            segment_limit: limit,
                            block_limit: None,
                        },
                    });
                    let s_expr = SExpr::create_leaf(Arc::new(compact_block));
                    Plan::OptimizeCompactBlock {
                        s_expr: Box::new(s_expr),
                        need_purge: false,
                    }
                }
                CompactTarget::Segment => {
                    Plan::OptimizeCompactSegment(Box::new(OptimizeCompactSegmentPlan {
                        catalog,
                        database,
                        table,
                        num_segment_limit: limit,
                    }))
                }
            },
        };

        Ok(plan)
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_vacuum_table(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &VacuumTableStmt,
    ) -> Result<Plan> {
        let VacuumTableStmt {
            catalog,
            database,
            table,
            option,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let option = VacuumTableOption {
            dry_run: option.dry_run,
        };
        Ok(Plan::VacuumTable(Box::new(VacuumTablePlan {
            catalog,
            database,
            table,
            option,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_vacuum_drop_table(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &VacuumDropTableStmt,
    ) -> Result<Plan> {
        let VacuumDropTableStmt {
            catalog,
            database,
            option,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| "".to_string());

        let option = {
            VacuumDropTableOption {
                dry_run: option.dry_run,
                limit: option.limit,
            }
        };
        Ok(Plan::VacuumDropTable(Box::new(VacuumDropTablePlan {
            catalog,
            database,
            option,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_vacuum_temporary_files(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &VacuumTemporaryFiles,
    ) -> Result<Plan> {
        Ok(Plan::VacuumTemporaryFiles(Box::new(
            VacuumTemporaryFilesPlan {
                limit: stmt.limit,
                retain: stmt.retain,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_analyze_table(
        &mut self,
        stmt: &AnalyzeTableStmt,
    ) -> Result<Plan> {
        let AnalyzeTableStmt {
            catalog,
            database,
            table,
            no_scan,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::AnalyzeTable(Box::new(AnalyzeTablePlan {
            catalog,
            database,
            table,
            no_scan: *no_scan,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_exists_table(
        &mut self,
        stmt: &ExistsTableStmt,
    ) -> Result<Plan> {
        let ExistsTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::ExistsTable(Box::new(ExistsTablePlan {
            catalog,
            database,
            table,
        })))
    }

    #[async_backtrace::framed]
    async fn analyze_rename_column(
        &self,
        old_column: &Identifier,
        new_column: &Identifier,
        table_schema: TableSchemaRef,
    ) -> Result<(TableSchema, String, String)> {
        let old_name = normalize_identifier(old_column, &self.name_resolution_ctx).name;
        let new_name = normalize_identifier(new_column, &self.name_resolution_ctx).name;

        if old_name == new_name {
            return Err(ErrorCode::SemanticError(
                "new column name is the same as old column name".to_string(),
            ));
        }
        let mut new_schema = table_schema.as_ref().clone();
        let mut old_column_existed = false;
        for (i, field) in table_schema.fields().iter().enumerate() {
            if field.name() == &new_name {
                return Err(ErrorCode::SemanticError(
                    "new column name existed".to_string(),
                ));
            }
            if field.name() == &old_name {
                new_schema.rename_field(i, &new_name);
                old_column_existed = true;
            }
        }
        if !old_column_existed {
            return Err(ErrorCode::SemanticError(
                "rename column not existed".to_string(),
            ));
        }
        Ok((new_schema, old_name, new_name))
    }

    #[async_backtrace::framed]
    async fn analyze_add_column(
        &self,
        column: &ColumnDefinition,
        table_schema: TableSchemaRef,
    ) -> Result<(TableField, String, bool, bool, bool)> {
        let name = normalize_identifier(&column.name, &self.name_resolution_ctx).name;
        let not_null = self.is_column_not_null();
        let data_type = resolve_type_name(&column.data_type, not_null)?;
        let mut is_deterministic = true;
        let mut is_nextval = false;
        let mut is_autoincrement = false;
        let mut field = TableField::new(&name, data_type);

        if let Some(expr) = &column.expr {
            match expr {
                ColumnExpr::Default(default_expr) => {
                    let mut default_expr_binder = DefaultExprBinder::try_new(self.ctx.clone())?;
                    let (expr, expr_is_deterministic, expr_is_nextval) =
                        default_expr_binder.parse_default_expr_to_string(&field, default_expr)?;
                    field = field.with_default_expr(Some(expr));
                    is_deterministic = expr_is_deterministic;
                    is_nextval = expr_is_nextval;
                }
                ColumnExpr::Virtual(virtual_expr) => {
                    let expr = parse_computed_expr_to_string(
                        self.ctx.clone(),
                        table_schema.clone(),
                        &field,
                        virtual_expr,
                    )?;
                    field = field.with_computed_expr(Some(ComputedExpr::Virtual(expr)));
                }
                ColumnExpr::Stored(stored_expr) => {
                    let expr = parse_computed_expr_to_string(
                        self.ctx.clone(),
                        table_schema.clone(),
                        &field,
                        stored_expr,
                    )?;
                    field = field.with_computed_expr(Some(ComputedExpr::Stored(expr)));
                    is_deterministic = false;
                }
                ColumnExpr::AutoIncrement {
                    start,
                    step,
                    is_ordered,
                } => {
                    Self::check_autoincrement_expr(&field, step, is_ordered)?;
                    field.auto_increment_expr = Some(AutoIncrementExpr {
                        column_id: table_schema.next_column_id(),
                        start: *start,
                        step: *step,
                        is_ordered: *is_ordered,
                    });
                    is_autoincrement = true;
                }
            }
        }
        let comment = column.comment.clone().unwrap_or_default();
        Ok((
            field,
            comment,
            is_deterministic,
            is_nextval,
            is_autoincrement,
        ))
    }

    fn check_autoincrement_expr(field: &TableField, step: &i64, is_ordered: &bool) -> Result<()> {
        if !matches!(
            field.data_type().remove_nullable(),
            TableDataType::Number(_) | TableDataType::Decimal(_)
        ) {
            return Err(ErrorCode::SemanticError(
                "AUTOINCREMENT only supports Decimal or Numeric (e.g. INT32) types",
            ));
        }
        if !is_ordered {
            return Err(ErrorCode::SemanticError(
                "AUTOINCREMENT only support ORDER now",
            ));
        }
        if step == &0 {
            return Err(ErrorCode::SemanticError("INCREMENT cannot be 0"));
        }
        if step < &0 {
            return Err(ErrorCode::SemanticError(
                "INCREMENT does not currently support negative numbers",
            ));
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn analyze_create_table_schema_by_columns(
        &self,
        columns: &[ColumnDefinition],
    ) -> Result<(TableSchemaRef, Vec<String>)> {
        let mut has_computed = false;
        let mut has_autoincrement = false;
        let mut fields = Vec::with_capacity(columns.len());
        let mut fields_comments = Vec::with_capacity(columns.len());
        let not_null = self.is_column_not_null();
        let mut default_expr_binder = DefaultExprBinder::try_new(self.ctx.clone())?;
        for column in columns.iter() {
            let name = normalize_identifier(&column.name, &self.name_resolution_ctx).name;
            let schema_data_type = resolve_type_name(&column.data_type, not_null)?;
            fields_comments.push(column.comment.clone().unwrap_or_default());
            let mut field = TableField::new(&name, schema_data_type.clone());
            if let Some(expr) = &column.expr {
                match expr {
                    ColumnExpr::Default(default_expr) => {
                        let (expr, _, _) = default_expr_binder
                            .parse_default_expr_to_string(&field, default_expr)?;
                        field = field.with_default_expr(Some(expr));
                    }
                    ColumnExpr::AutoIncrement {
                        start,
                        step,
                        is_ordered,
                    } => {
                        Self::check_autoincrement_expr(&field, step, is_ordered)?;
                        has_autoincrement = true;
                        field.auto_increment_expr = Some(AutoIncrementExpr {
                            column_id: 0,
                            start: *start,
                            step: *step,
                            is_ordered: *is_ordered,
                        });
                    }
                    _ => has_computed = true,
                }
            }
            fields.push(field);
        }

        let mut fields = if has_computed {
            let mut source_fields = Vec::with_capacity(fields.len());
            for (column, field) in columns.iter().zip(fields.iter()) {
                match &column.expr {
                    Some(ColumnExpr::Virtual(_)) | Some(ColumnExpr::Stored(_)) => {
                        continue;
                    }
                    _ => {}
                }
                source_fields.push(field.clone());
            }
            let source_schema = TableSchemaRefExt::create(source_fields);
            let mut new_fields = Vec::with_capacity(fields.len());
            for (column, field) in columns.iter().zip(fields.into_iter()) {
                match &column.expr {
                    Some(ColumnExpr::Virtual(virtual_expr)) => {
                        let expr = parse_computed_expr_to_string(
                            self.ctx.clone(),
                            source_schema.clone(),
                            &field,
                            virtual_expr,
                        )?;
                        new_fields
                            .push(field.with_computed_expr(Some(ComputedExpr::Virtual(expr))));
                    }
                    Some(ColumnExpr::Stored(stored_expr)) => {
                        let expr = parse_computed_expr_to_string(
                            self.ctx.clone(),
                            source_schema.clone(),
                            &field,
                            stored_expr,
                        )?;
                        new_fields.push(field.with_computed_expr(Some(ComputedExpr::Stored(expr))));
                    }
                    _ => {
                        new_fields.push(field);
                    }
                }
            }
            new_fields
        } else {
            fields
        };
        // update auto increment expr column id
        if has_autoincrement {
            let table_schema = TableSchema::new(fields.clone());

            for (i, table_field) in table_schema.fields().iter().enumerate() {
                let Some(auto_increment_expr) = fields[i].auto_increment_expr.as_mut() else {
                    continue;
                };

                auto_increment_expr.column_id = table_field.column_id;
            }
        }

        let schema = TableSchemaRefExt::create(fields);
        Self::validate_create_table_schema(&schema)?;
        Ok((schema, fields_comments))
    }

    #[async_backtrace::framed]
    async fn analyze_constraints(
        &self,
        table_name: &str,
        table_schema: TableSchemaRef,
        constraint_defs: impl Iterator<Item = &ConstraintDefinition> + Send,
    ) -> Result<BTreeMap<String, Constraint>> {
        let mut constraints = BTreeMap::new();
        let mut binder =
            ConstraintExprBinder::try_new(self.ctx.clone(), Arc::new(table_schema.clone().into()))?;

        for constraint_def in constraint_defs {
            let (constraint, used_columns) = match &constraint_def.constraint_type {
                AstConstraintType::Check(expr) => {
                    // test check expr bindable
                    let scalar_expr = binder.bind(expr)?;

                    (
                        Constraint::Check(expr.to_string()),
                        scalar_expr.used_columns(),
                    )
                }
            };
            let name = constraint_def
                .name
                .as_ref()
                .map(|ident| self.normalize_object_identifier(ident))
                .unwrap_or_else(|| {
                    let base_name = binder.default_name(table_name, &used_columns, &constraint);
                    let mut index = 0;

                    let mut constraint_name = base_name.clone();
                    while constraints.contains_key(&constraint_name) {
                        index += 1;
                        constraint_name = format!("{}{}", base_name, index);
                    }
                    constraint_name
                });
            constraints.insert(name, constraint);
        }

        Ok(constraints)
    }

    #[async_backtrace::framed]
    async fn analyze_table_indexes(
        &self,
        table_schema: TableSchemaRef,
        table_index_defs: &[TableIndexDefinition],
    ) -> Result<BTreeMap<String, TableIndex>> {
        let mut table_indexes = BTreeMap::new();
        for table_index_def in table_index_defs {
            let name = self.normalize_object_identifier(&table_index_def.index_name);
            if table_indexes.contains_key(&name) {
                return Err(ErrorCode::BadArguments(format!(
                    "Duplicated index name: {}",
                    name
                )));
            }
            let (index_type, column_ids, options) = match table_index_def.index_type {
                AstTableIndexType::Inverted => {
                    let column_ids = self.validate_inverted_index_columns(
                        table_schema.clone(),
                        &table_index_def.columns,
                    )?;
                    let options =
                        self.validate_inverted_index_options(&table_index_def.index_options)?;
                    (TableIndexType::Inverted, column_ids, options)
                }
                AstTableIndexType::Ngram => {
                    let column_ids = self.validate_ngram_index_columns(
                        table_schema.clone(),
                        &table_index_def.columns,
                    )?;
                    let options =
                        self.validate_ngram_index_options(&table_index_def.index_options)?;
                    (TableIndexType::Ngram, column_ids, options)
                }
                AstTableIndexType::Vector => {
                    let column_ids = self.validate_vector_index_columns(
                        table_schema.clone(),
                        &table_index_def.columns,
                    )?;
                    let options =
                        self.validate_vector_index_options(&table_index_def.index_options)?;
                    (TableIndexType::Vector, column_ids, options)
                }
                AstTableIndexType::Spatial => {
                    let column_ids = self.validate_spatial_index_columns(
                        table_schema.clone(),
                        &table_index_def.columns,
                    )?;
                    let options =
                        self.validate_spatial_index_options(&table_index_def.index_options)?;
                    (TableIndexType::Spatial, column_ids, options)
                }
                AstTableIndexType::Aggregating => unreachable!(),
            };

            let table_index = TableIndex {
                index_type,
                name: name.clone(),
                column_ids,
                sync_creation: table_index_def.sync_creation,
                version: Uuid::new_v4().simple().to_string(),
                options,
            };
            table_indexes.insert(name, table_index);
        }
        Ok(table_indexes)
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn analyze_create_table_schema(
        &self,
        table: &str,
        source: &CreateTableSource,
    ) -> Result<AnalyzeCreateTableResult> {
        match source {
            CreateTableSource::Columns {
                columns,
                opt_table_indexes,
                opt_table_constraints,
                opt_column_constraints,
            } => {
                let (schema, comments) =
                    self.analyze_create_table_schema_by_columns(columns).await?;
                let table_indexes = if let Some(table_index_defs) = opt_table_indexes {
                    let table_indexes = self
                        .analyze_table_indexes(schema.clone(), table_index_defs)
                        .await?;
                    Some(table_indexes)
                } else {
                    None
                };

                let constraints = match (opt_column_constraints, opt_table_constraints) {
                    (Some(column_constraints), Some(table_constraints)) => Some(Box::new(
                        column_constraints.iter().chain(table_constraints.iter()),
                    )
                        as Box<dyn Iterator<Item = &ConstraintDefinition> + Send>),
                    (Some(constraints), None) | (None, Some(constraints)) => {
                        Some(Box::new(constraints.iter()) as _)
                    }
                    (None, None) => None,
                };
                let table_constraints = if let Some(constraints) = constraints {
                    let constraints = self
                        .analyze_constraints(table, schema.clone(), constraints)
                        .await?;
                    Some(constraints)
                } else {
                    None
                };
                Ok(AnalyzeCreateTableResult {
                    schema,
                    field_comments: comments,
                    table_indexes,
                    table_constraints,
                })
            }
            CreateTableSource::Like {
                catalog,
                database,
                table,
            } => {
                let (catalog, database, table) =
                    self.normalize_object_identifier_triple(catalog, database, table);
                let table = self.ctx.get_table(&catalog, &database, &table).await?;

                if table.engine() == VIEW_ENGINE {
                    if let Some(query) = table.get_table_info().options().get(QUERY) {
                        let mut planner = Planner::new(self.ctx.clone());
                        let (plan, _) = planner.plan_sql(query).await?;
                        Ok(AnalyzeCreateTableResult {
                            schema: infer_table_schema(&plan.schema())?,
                            field_comments: vec![],
                            table_indexes: None,
                            table_constraints: None,
                        })
                    } else {
                        Err(ErrorCode::Internal(
                            "Logical error, View Table must have a SelectQuery inside.",
                        ))
                    }
                } else {
                    Ok(AnalyzeCreateTableResult {
                        schema: table.schema(),
                        field_comments: table.field_comments().clone(),
                        table_indexes: None,
                        table_constraints: None,
                    })
                }
            }
        }
    }

    /// Validate the schema of the table to be created.
    pub(in crate::planner::binder) fn validate_create_table_schema(
        schema: &TableSchemaRef,
    ) -> Result<()> {
        // Check if there are duplicated column names
        let mut name_set = HashSet::new();
        for field in schema.fields() {
            if !name_set.insert(field.name().clone()) {
                return Err(ErrorCode::BadArguments(format!(
                    "Duplicated column name: {}",
                    field.name()
                )));
            }
            if field.data_type == TableDataType::Null {
                return Err(ErrorCode::BadArguments(format!(
                    "Column `{}` have NULL type is not allowed",
                    field.name()
                )));
            }
        }

        Ok(())
    }

    pub(in crate::planner::binder) fn insert_table_option_with_validation(
        &self,
        options: &mut BTreeMap<String, String>,
        key: String,
        value: String,
    ) -> Result<()> {
        if is_reserved_opt_key(&key) {
            Err(ErrorCode::TableOptionInvalid(format!(
                "table option {key} reserved, please do not specify in the CREATE TABLE statement",
            )))
        } else if options.insert(key.clone(), value).is_some() {
            Err(ErrorCode::TableOptionInvalid(format!(
                "table option {key} duplicated"
            )))
        } else {
            Ok(())
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn analyze_cluster_keys(
        &mut self,
        cluster_opt: &ClusterOption,
        schema: TableSchemaRef,
    ) -> Result<Vec<String>> {
        let ClusterOption {
            cluster_type,
            cluster_exprs,
        } = cluster_opt;

        let expr_len = cluster_exprs.len();
        if matches!(cluster_type, AstClusterType::Hilbert) {
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.ctx.get_license_key(), Feature::HilbertClustering)?;

            if !(2..=5).contains(&expr_len) {
                return Err(ErrorCode::InvalidClusterKeys(
                    "Hilbert clustering requires the dimension to be between 2 and 5",
                ));
            }
        }

        // Build a temporary BindContext to resolve the expr
        let mut bind_context = BindContext::new();
        for (index, field) in schema.fields().iter().enumerate() {
            let column = ColumnBindingBuilder::new(
                field.name().clone(),
                index,
                Box::new(DataType::from(field.data_type())),
                Visibility::Visible,
            )
            .build();

            bind_context.add_column_binding(column);
        }
        let mut scalar_binder = ScalarBinder::new(
            &mut bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        // cluster keys cannot be a udf expression.
        scalar_binder.forbid_udf();

        let mut normalizer = ClusterKeyNormalizer {
            force_quoted_ident: false,
            unquoted_ident_case_sensitive: self.name_resolution_ctx.unquoted_ident_case_sensitive,
            quoted_ident_case_sensitive: self.name_resolution_ctx.quoted_ident_case_sensitive,
            sql_dialect: self.dialect,
        };
        let mut cluster_keys = Vec::with_capacity(expr_len);
        for cluster_expr in cluster_exprs.iter() {
            let (cluster_key, _) = scalar_binder.bind(cluster_expr)?;
            if cluster_key.used_columns().len() != 1 || !cluster_key.evaluable() {
                return Err(ErrorCode::InvalidClusterKeys(format!(
                    "Cluster by expression `{:#}` is invalid",
                    cluster_expr
                )));
            }

            let expr = cluster_key.as_expr()?;
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::InvalidClusterKeys(format!(
                    "Cluster by expression `{:#}` is not deterministic",
                    cluster_expr
                )));
            }

            let data_type = expr.data_type();
            if !Self::valid_cluster_key_type(data_type) {
                return Err(ErrorCode::InvalidClusterKeys(format!(
                    "Unsupported data type '{}' for cluster by expression `{:#}`",
                    data_type, cluster_expr
                )));
            }

            let mut cluster_expr = cluster_expr.clone();
            cluster_expr.drive_mut(&mut normalizer);
            cluster_keys.push(format!("{:#}", &cluster_expr));
        }

        Ok(cluster_keys)
    }

    pub(crate) fn valid_cluster_key_type(data_type: &DataType) -> bool {
        let inner_type = data_type.remove_nullable();
        matches!(
            inner_type,
            DataType::Number(_)
                | DataType::String
                | DataType::Timestamp
                | DataType::Date
                | DataType::Boolean
                | DataType::Decimal(_)
        )
    }

    fn is_column_not_null(&self) -> bool {
        !self
            .ctx
            .get_settings()
            .get_ddl_column_type_nullable()
            .unwrap_or(true)
    }
}

const VERIFICATION_KEY: &str = "_v_d77aa11285c22e0e1d4593a035c98c0d";
const VERIFICATION_KEY_DEL: &str = "_v_d77aa11285c22e0e1d4593a035c98c0d_del";

// verify that essential privileges has granted for accessing external location
//
// The permission check might fail for reasons other than the permissions themselves,
// such as network communication issues.
pub async fn verify_external_location_privileges(dal: Operator) -> Result<()> {
    let verification_task = async move {
        // verify privilege to put
        let mut errors = Vec::new();
        if let Err(e) = dal.write(VERIFICATION_KEY, "V").await {
            errors.push(format!("Permission check for [Write] failed: {}", e));
        }

        // verify privilege to get
        if let Err(e) = dal.read_with(VERIFICATION_KEY).range(0..1).await {
            errors.push(format!("Permission check for [Read] failed: {}", e));
        }

        // verify privilege to stat
        if let Err(e) = dal.stat(VERIFICATION_KEY).await {
            errors.push(format!("Permission check for [Stat] failed: {}", e));
        }

        // verify privilege to list
        // Append "/" to the verification key to ensure we are listing the contents of the directory/prefix
        // rather than attempting to list a single object.
        // Like aws s3 express one, the list requires a end delimiter.
        if let Err(e) = dal.list(&format!("{}{}", VERIFICATION_KEY, "/")).await {
            errors.push(format!("Permission check for [List] failed: {}", e));
        }

        // verify privilege to delete (del something not exist)
        if let Err(e) = dal.delete(VERIFICATION_KEY_DEL).await {
            errors.push(format!("Permission check for [Delete] failed: {}", e));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ErrorCode::StorageOther(
                "Checking essential permissions for the external location failed.",
            )
            .add_message(errors.join("\n")))
        }
    };

    GlobalIORuntime::instance()
        .spawn(verification_task)
        .await
        .expect("join must succeed")
}
