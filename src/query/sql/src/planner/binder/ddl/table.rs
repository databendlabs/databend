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
use std::collections::HashSet;
use std::sync::Arc;

use common_ast::ast::AlterTableAction;
use common_ast::ast::AlterTableStmt;
use common_ast::ast::AnalyzeTableStmt;
use common_ast::ast::ColumnDefinition;
use common_ast::ast::CompactTarget;
use common_ast::ast::CreateTableSource;
use common_ast::ast::CreateTableStmt;
use common_ast::ast::DescribeTableStmt;
use common_ast::ast::DropTableStmt;
use common_ast::ast::Engine;
use common_ast::ast::ExistsTableStmt;
use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_ast::ast::OptimizeTableAction as AstOptimizeTableAction;
use common_ast::ast::OptimizeTableStmt;
use common_ast::ast::RenameTableStmt;
use common_ast::ast::ShowCreateTableStmt;
use common_ast::ast::ShowLimit;
use common_ast::ast::ShowTablesStatusStmt;
use common_ast::ast::ShowTablesStmt;
use common_ast::ast::Statement;
use common_ast::ast::TableReference;
use common_ast::ast::TruncateTableStmt;
use common_ast::ast::UndropTableStmt;
use common_ast::ast::UriLocation;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::walk_expr_mut;
use common_ast::Dialect;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_schema_type;
use common_expression::infer_table_schema;
use common_expression::types::DataType;
use common_expression::ConstantFolder;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_expression::TableSchemaRefExt;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::storage::StorageParams;
use common_storage::DataOperator;
use common_storages_view::view_table::QUERY;
use common_storages_view::view_table::VIEW_ENGINE;
use storages_common_table_meta::table::is_reserved_opt_key;
use storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use tracing::debug;

use crate::binder::location::parse_uri_location;
use crate::binder::scalar::ScalarBinder;
use crate::binder::Binder;
use crate::binder::Visibility;
use crate::optimizer::optimize;
use crate::optimizer::OptimizerConfig;
use crate::optimizer::OptimizerContext;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::resolve_type_name;
use crate::planner::semantic::IdentifierNormalizer;
use crate::plans::AddTableColumnPlan;
use crate::plans::AlterTableClusterKeyPlan;
use crate::plans::AnalyzeTablePlan;
use crate::plans::CastExpr;
use crate::plans::CreateTablePlan;
use crate::plans::DescribeTablePlan;
use crate::plans::DropTableClusterKeyPlan;
use crate::plans::DropTableColumnPlan;
use crate::plans::DropTablePlan;
use crate::plans::ExistsTablePlan;
use crate::plans::OptimizeTableAction;
use crate::plans::OptimizeTablePlan;
use crate::plans::Plan;
use crate::plans::ReclusterTablePlan;
use crate::plans::RenameTablePlan;
use crate::plans::RevertTablePlan;
use crate::plans::RewriteKind;
use crate::plans::ShowCreateTablePlan;
use crate::plans::TruncateTablePlan;
use crate::plans::UndropTablePlan;
use crate::BindContext;
use crate::ColumnBinding;
use crate::Planner;
use crate::ScalarExpr;
use crate::SelectBuilder;

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

        let database = self.check_database_exist(catalog, database).await?;

        let mut select_builder = if stmt.with_history {
            SelectBuilder::from("system.tables_with_history")
        } else {
            SelectBuilder::from("system.tables")
        };

        if *full {
            select_builder
                .with_column("name AS Tables")
                .with_column("'BASE TABLE' AS Table_type")
                .with_column("database AS Database")
                .with_column("catalog AS Catalog")
                .with_column("engine")
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
            select_builder.with_column(format!("name AS Tables_in_{database}"));
            if *with_history {
                select_builder.with_column("dropped_on AS drop_time");
            };
        }

        select_builder
            .with_order_by("catalog")
            .with_order_by("database")
            .with_order_by("name");

        select_builder.with_filter(format!("database = '{database}'"));

        if let Some(catalog) = catalog {
            let catalog = normalize_identifier(catalog, &self.name_resolution_ctx).name;
            select_builder.with_filter(format!("catalog = '{catalog}'"));
        }

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
        self.bind_rewrite_to_query(bind_context, query.as_str(), RewriteKind::ShowTables)
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
    pub(in crate::planner::binder) async fn bind_show_tables_status(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowTablesStatusStmt,
    ) -> Result<Plan> {
        let ShowTablesStatusStmt { database, limit } = stmt;

        let database = self.check_database_exist(&None, database).await?;

        let select_cols = "name AS Name, engine AS Engine, 0 AS Version, \
        NULL AS Row_format, num_rows AS Rows, NULL AS Avg_row_length, data_size AS Data_length, \
        NULL AS Max_data_length, index_size AS Index_length, NULL AS Data_free, NULL AS Auto_increment, \
        created_on AS Create_time, NULL AS Update_time, NULL AS Check_time, NULL AS Collation, \
        NULL AS Checksum, '' AS Comment"
            .to_string();

        // Use `system.tables` AS the "base" table to construct the result-set of `SHOW TABLE STATUS ..`
        //
        // To constraint the schema of the final result-set,
        //  `(select ${select_cols} from system.tables where ..)`
        // is used AS a derived table.
        // (unlike mysql, alias of derived table is not required in databend).
        let query = match limit {
            None => format!(
                "SELECT * from (SELECT {} FROM system.tables WHERE database = '{}') \
                ORDER BY Name",
                select_cols, database
            ),
            Some(ShowLimit::Like { pattern }) => format!(
                "SELECT * from (SELECT {} FROM system.tables WHERE database = '{}') \
            WHERE Name LIKE '{}' ORDER BY Name",
                select_cols, database, pattern
            ),
            Some(ShowLimit::Where { selection }) => format!(
                "SELECT * from (SELECT {} FROM system.tables WHERE database = '{}') \
            WHERE ({}) ORDER BY Name",
                select_cols, database, selection
            ),
        };
        let tokens = tokenize_sql(query.as_str())?;
        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL)?;
        self.bind_statement(bind_context, &stmt).await
    }

    #[async_backtrace::framed]
    async fn check_database_exist(
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
                    .get_catalog(&ctl_name)?
                    .get_database(&self.ctx.get_tenant(), &database)
                    .await?;
                Ok(database)
            }
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_table(
        &mut self,
        stmt: &CreateTableStmt,
    ) -> Result<Plan> {
        let CreateTableStmt {
            if_not_exists,
            catalog,
            database,
            table,
            source,
            table_options,
            cluster_by,
            as_query,
            transient,
            engine,
            uri_location,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        // Take FUSE engine AS default engine
        let engine = engine.unwrap_or(Engine::Fuse);
        let mut options: BTreeMap<String, String> = BTreeMap::new();
        for table_option in table_options.iter() {
            self.insert_table_option_with_validation(
                &mut options,
                table_option.0.to_lowercase(),
                table_option.1.to_string(),
            )?;
        }

        let (storage_params, part_prefix) = match uri_location {
            Some(uri) => {
                let mut uri = UriLocation {
                    protocol: uri.protocol.clone(),
                    name: uri.name.clone(),
                    path: uri.path.clone(),
                    part_prefix: uri.part_prefix.clone(),
                    connection: uri.connection.clone(),
                };
                let (sp, _) = parse_uri_location(&mut uri)?;

                // create a temporary op to check if params is correct
                DataOperator::try_create(&sp).await?;

                // Path ends with "/" means it's a directory.
                let fp = if uri.path.ends_with('/') {
                    uri.part_prefix.clone()
                } else {
                    "".to_string()
                };

                (Some(sp), fp)
            }
            None => (None, "".to_string()),
        };

        // If table is TRANSIENT, set a flag in table option
        if *transient {
            options.insert("TRANSIENT".to_owned(), "T".to_owned());
        }

        // Build table schema
        let (schema, field_default_exprs, field_comments) = match (&source, &as_query) {
            (Some(source), None) => {
                // `CREATE TABLE` without `AS SELECT ...`
                self.analyze_create_table_schema(source).await?
            }
            (None, Some(query)) => {
                // `CREATE TABLE AS SELECT ...` without column definitions
                let mut init_bind_context = BindContext::new();
                let (_, bind_context) = self.bind_query(&mut init_bind_context, query).await?;
                let fields = bind_context
                    .columns
                    .iter()
                    .map(|column_binding| {
                        Ok(TableField::new(
                            &column_binding.column_name,
                            infer_schema_type(&column_binding.data_type)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let schema = TableSchemaRefExt::create(fields);
                Self::validate_create_table_schema(&schema)?;
                (schema, vec![], vec![])
            }
            (Some(source), Some(query)) => {
                // e.g. `CREATE TABLE t (i INT) AS SELECT * from old_t` with columns specified
                let (source_schema, source_default_exprs, source_comments) =
                    self.analyze_create_table_schema(source).await?;
                let mut init_bind_context = BindContext::new();
                let (_, bind_context) = self.bind_query(&mut init_bind_context, query).await?;
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
                if source_schema.fields().len() != query_fields.len() {
                    return Err(ErrorCode::BadArguments("Number of columns does not match"));
                }
                Self::validate_create_table_schema(&source_schema)?;
                (source_schema, source_default_exprs, source_comments)
            }
            _ => Err(ErrorCode::BadArguments(
                "Incorrect CREATE query: required list of column descriptions or AS section or SELECT..",
            ))?,
        };

        if engine == Engine::Fuse {
            // Currently, [Table] can not accesses its database id yet, thus
            // here we keep the db id AS an entry of `table_meta.options`.
            //
            // To make the unit/stateless test cases (`show create ..`) easier,
            // here we care about the FUSE engine only.
            //
            // Later, when database id is kept, let say in `TableInfo`, we can
            // safely eliminate this "FUSE" constant and the table meta option entry.
            let catalog = self.ctx.get_catalog(&catalog)?;
            let db = catalog
                .get_database(&self.ctx.get_tenant(), &database)
                .await?;
            let db_id = db.get_db_info().ident.db_id;
            options.insert(OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());

            let config = GlobalConfig::instance();
            let is_blocking_fs = matches!(
                storage_params.as_ref().unwrap_or(&config.storage.params),
                StorageParams::Fs(_)
            );

            // we should persist the storage format and compression type instead of using the default value in fuse table
            if !options.contains_key(OPT_KEY_STORAGE_FORMAT) {
                let default_storage_format = match config.query.default_storage_format.as_str() {
                    "" | "auto" => {
                        if is_blocking_fs {
                            "native"
                        } else {
                            "parquet"
                        }
                    }
                    _ => config.query.default_storage_format.as_str(),
                };
                options.insert(
                    OPT_KEY_STORAGE_FORMAT.to_owned(),
                    default_storage_format.to_owned(),
                );
            }

            if !options.contains_key(OPT_KEY_TABLE_COMPRESSION) {
                let default_compression = match config.query.default_compression.as_str() {
                    "" | "auto" => {
                        if is_blocking_fs {
                            "lz4"
                        } else {
                            "zstd"
                        }
                    }
                    _ => config.query.default_compression.as_str(),
                };
                options.insert(
                    OPT_KEY_TABLE_COMPRESSION.to_owned(),
                    default_compression.to_owned(),
                );
            }
        }

        let cluster_key = {
            let keys = self
                .analyze_cluster_keys(cluster_by, schema.clone())
                .await?;
            if keys.is_empty() {
                None
            } else {
                Some(format!("({})", keys.join(", ")))
            }
        };

        let plan = CreateTablePlan {
            if_not_exists: *if_not_exists,
            tenant: self.ctx.get_tenant(),
            catalog: catalog.clone(),
            database: database.clone(),
            table,
            schema: schema.clone(),
            engine,
            storage_params,
            part_prefix,
            options,
            field_default_exprs,
            field_comments,
            cluster_key,
            as_select: if let Some(query) = as_query {
                let mut bind_context = BindContext::new();
                let stmt = Statement::Query(Box::new(*query.clone()));
                let select_plan = self.bind_statement(&mut bind_context, &stmt).await?;
                // Don't enable distributed optimization for `CREATE TABLE ... AS SELECT ...` for now
                let opt_ctx = Arc::new(OptimizerContext::new(OptimizerConfig::default()));
                let optimized_plan = optimize(self.ctx.clone(), opt_ctx, select_plan)?;
                Some(Box::new(optimized_plan))
            } else {
                None
            },
        };
        Ok(Plan::CreateTable(Box::new(plan)))
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

        let (catalog, database, table) = if let TableReference::Table {
            catalog,
            database,
            table,
            ..
        } = table_reference
        {
            self.normalize_object_identifier_triple(catalog, database, table)
        } else {
            return Err(ErrorCode::Internal(
                "should not happen, parser should have report error already",
            ));
        };

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
            AlterTableAction::AddColumn { column } => {
                let (schema, field_default_exprs, field_comments) = self
                    .analyze_create_table_schema_by_columns(&[column.clone()])
                    .await?;
                Ok(Plan::AddTableColumn(Box::new(AddTableColumnPlan {
                    catalog,
                    database,
                    table,
                    schema,
                    field_default_exprs,
                    field_comments,
                })))
            }
            AlterTableAction::DropColumn { column } => {
                Ok(Plan::DropTableColumn(Box::new(DropTableColumnPlan {
                    catalog,
                    database,
                    table,
                    column: column.to_string(),
                })))
            }
            AlterTableAction::AlterTableClusterKey { cluster_by } => {
                let schema = self
                    .ctx
                    .get_table(&catalog, &database, &table)
                    .await?
                    .schema();
                let cluster_keys = self.analyze_cluster_keys(cluster_by, schema).await?;

                Ok(Plan::AlterTableClusterKey(Box::new(
                    AlterTableClusterKeyPlan {
                        tenant,
                        catalog,
                        database,
                        table,
                        cluster_keys,
                    },
                )))
            }
            AlterTableAction::DropTableClusterKey => Ok(Plan::DropTableClusterKey(Box::new(
                DropTableClusterKeyPlan {
                    tenant,
                    catalog,
                    database,
                    table,
                },
            ))),
            AlterTableAction::ReclusterTable {
                is_final,
                selection,
            } => {
                let (_, mut context) = self
                    .bind_table_reference(bind_context, table_reference)
                    .await?;

                let mut scalar_binder = ScalarBinder::new(
                    &mut context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                );

                let push_downs = if let Some(expr) = selection {
                    let (scalar, _) = scalar_binder.bind(expr).await?;
                    Some(scalar)
                } else {
                    None
                };

                Ok(Plan::ReclusterTable(Box::new(ReclusterTablePlan {
                    tenant,
                    catalog,
                    database,
                    table,
                    is_final: *is_final,
                    metadata: self.metadata.clone(),
                    push_downs,
                })))
            }
            AlterTableAction::RevertTo { point } => {
                let point = self.resolve_data_travel_point(bind_context, point).await?;
                Ok(Plan::RevertTable(Box::new(RevertTablePlan {
                    tenant,
                    catalog,
                    database,
                    table,
                    point,
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
        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let (new_catalog, new_database, new_table) =
            self.normalize_object_identifier_triple(new_catalog, new_database, new_table);

        if new_catalog != catalog {
            return Err(ErrorCode::BadArguments(
                "alter catalog not allowed while rename table",
            ));
        }

        Ok(Plan::RenameTable(Box::new(RenameTablePlan {
            tenant,
            if_exists: *if_exists,
            catalog,
            database,
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
            purge,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::TruncateTable(Box::new(TruncateTablePlan {
            catalog,
            database,
            table,
            purge: *purge,
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
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);
        let action = match ast_action {
            AstOptimizeTableAction::All => OptimizeTableAction::All,
            AstOptimizeTableAction::Purge { before } => {
                let p = if let Some(point) = before {
                    let point = self.resolve_data_travel_point(bind_context, point).await?;
                    Some(point)
                } else {
                    None
                };
                OptimizeTableAction::Purge(p)
            }
            AstOptimizeTableAction::Compact { target, limit } => {
                let limit_cnt = match limit {
                    Some(Expr::Literal {
                        lit: Literal::UInt64(uint),
                        ..
                    }) => Some(*uint as usize),
                    Some(_) => {
                        return Err(ErrorCode::IllegalDataType("Unsupported limit type"));
                    }
                    _ => None,
                };
                match target {
                    CompactTarget::Block => OptimizeTableAction::CompactBlocks(limit_cnt),
                    CompactTarget::Segment => OptimizeTableAction::CompactSegments(limit_cnt),
                }
            }
        };

        Ok(Plan::OptimizeTable(Box::new(OptimizeTablePlan {
            catalog,
            database,
            table,
            action,
        })))
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
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::AnalyzeTable(Box::new(AnalyzeTablePlan {
            catalog,
            database,
            table,
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
    async fn analyze_create_table_schema_by_columns(
        &self,
        columns: &[ColumnDefinition],
    ) -> Result<(TableSchemaRef, Vec<Option<String>>, Vec<String>)> {
        let mut bind_context = BindContext::new();
        let mut scalar_binder = ScalarBinder::new(
            &mut bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        let mut fields = Vec::with_capacity(columns.len());
        let mut fields_default_expr = Vec::with_capacity(columns.len());
        let mut fields_comments = Vec::with_capacity(columns.len());
        for column in columns.iter() {
            let name = normalize_identifier(&column.name, &self.name_resolution_ctx).name;
            let schema_data_type = resolve_type_name(&column.data_type)?;

            fields.push(TableField::new(&name, schema_data_type.clone()));
            fields_default_expr.push({
                if let Some(default_expr) = &column.default_expr {
                    let (expr, _) = scalar_binder.bind(default_expr).await?;
                    let is_try = schema_data_type.is_nullable();
                    let cast_expr_to_field_type = ScalarExpr::CastExpr(CastExpr {
                        span: expr.span(),
                        is_try,
                        target_type: Box::new(DataType::from(&schema_data_type)),
                        argument: Box::new(expr),
                    })
                    .as_expr_with_col_index()?;
                    let (fold_to_constant, _) = ConstantFolder::fold(
                        &cast_expr_to_field_type,
                        self.ctx.get_function_context()?,
                        &BUILTIN_FUNCTIONS,
                    );
                    if let common_expression::Expr::Constant { .. } = fold_to_constant {
                        Some(default_expr.to_string())
                    } else {
                        return Err(ErrorCode::SemanticError(format!(
                            "default expression {cast_expr_to_field_type} is not a valid constant",
                        )));
                    }
                } else {
                    None
                }
            });
            fields_comments.push(column.comment.clone().unwrap_or_default());
        }
        let schema = TableSchemaRefExt::create(fields);
        Self::validate_create_table_schema(&schema)?;
        Ok((schema, fields_default_expr, fields_comments))
    }

    #[async_backtrace::framed]
    async fn analyze_create_table_schema(
        &self,
        source: &CreateTableSource,
    ) -> Result<(TableSchemaRef, Vec<Option<String>>, Vec<String>)> {
        match source {
            CreateTableSource::Columns(columns) => {
                self.analyze_create_table_schema_by_columns(columns).await
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
                    let query = table.get_table_info().options().get(QUERY).unwrap();
                    let mut planner = Planner::new(self.ctx.clone());
                    let (plan, _) = planner.plan_sql(query).await?;
                    Ok((infer_table_schema(&plan.schema())?, vec![], vec![]))
                } else {
                    Ok((table.schema(), vec![], table.field_comments().clone()))
                }
            }
        }
    }

    /// Validate the schema of the table to be created.
    fn validate_create_table_schema(schema: &TableSchemaRef) -> Result<()> {
        // Check if there are duplicated column names
        let mut name_set = HashSet::new();
        for field in schema.fields() {
            if !name_set.insert(field.name().clone()) {
                return Err(ErrorCode::BadArguments(format!(
                    "Duplicated column name: {}",
                    field.name()
                )));
            }
        }

        Ok(())
    }

    fn insert_table_option_with_validation(
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
    async fn analyze_cluster_keys(
        &mut self,
        cluster_by: &[Expr],
        schema: TableSchemaRef,
    ) -> Result<Vec<String>> {
        // Build a temporary BindContext to resolve the expr
        let mut bind_context = BindContext::new();
        for (index, field) in schema.fields().iter().enumerate() {
            let column = ColumnBinding {
                database_name: None,
                table_name: None,
                table_index: None,
                column_name: field.name().clone(),
                index,
                data_type: Box::new(DataType::from(field.data_type())),
                visibility: Visibility::Visible,
            };
            bind_context.columns.push(column);
        }
        let mut scalar_binder = ScalarBinder::new(
            &mut bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );

        let mut cluster_keys = Vec::with_capacity(cluster_by.len());
        for cluster_by in cluster_by.iter() {
            let (cluster_key, _) = scalar_binder.bind(cluster_by).await?;
            let expr = cluster_key.as_expr_with_col_index()?;
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::InvalidClusterKeys(format!(
                    "Cluster by expression `{:#}` is not deterministic",
                    cluster_by
                )));
            }
            let mut cluster_by = cluster_by.clone();
            walk_expr_mut(
                &mut IdentifierNormalizer {
                    ctx: &self.name_resolution_ctx,
                },
                &mut cluster_by,
            );
            cluster_keys.push(format!("{:#}", &cluster_by));
        }

        Ok(cluster_keys)
    }
}
