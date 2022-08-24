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
use std::collections::HashSet;
use std::sync::Arc;

use common_ast::ast::OptimizeTableAction as AstOptimizeTableAction;
use common_ast::ast::*;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::walk_expr_mut;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_datavalues::type_coercion::compare_coercion;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::ToDataType;
use common_datavalues::TypeFactory;
use common_datavalues::Vu8;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableMeta;
use common_planners::OptimizeTableAction;
use common_planners::*;
use tracing::debug;

use crate::catalogs::DatabaseCatalog;
use crate::sessions::TableContext;
use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::Binder;
use crate::sql::executor::PhysicalScalarBuilder;
use crate::sql::is_reserved_opt_key;
use crate::sql::optimizer::optimize;
use crate::sql::optimizer::OptimizerConfig;
use crate::sql::optimizer::OptimizerContext;
use crate::sql::planner::semantic::normalize_identifier;
use crate::sql::planner::semantic::IdentifierNormalizer;
use crate::sql::plans::create_table_v2::CreateTablePlanV2;
use crate::sql::plans::CastExpr;
use crate::sql::plans::Plan;
use crate::sql::plans::RewriteKind;
use crate::sql::plans::Scalar;
use crate::sql::BindContext;
use crate::sql::ColumnBinding;
use crate::sql::ScalarExpr;
use crate::sql::OPT_KEY_DATABASE_ID;

struct SelectBuilder {
    from: String,
    columns: Vec<String>,
    filters: Vec<String>,
    order_bys: Vec<String>,
}

impl SelectBuilder {
    fn from(table_name: &str) -> SelectBuilder {
        SelectBuilder {
            from: table_name.to_owned(),
            columns: vec![],
            filters: vec![],
            order_bys: vec![],
        }
    }
    fn with_column(&mut self, col_name: impl Into<String>) -> &mut Self {
        self.columns.push(col_name.into());
        self
    }

    fn with_filter(&mut self, col_name: impl Into<String>) -> &mut Self {
        self.filters.push(col_name.into());
        self
    }

    fn with_order_by(&mut self, order_by: &str) -> &mut Self {
        self.order_bys.push(order_by.to_owned());
        self
    }

    fn build(self) -> String {
        let mut query = String::new();
        let mut columns = String::new();
        let s = self.columns.join(",");
        if s.is_empty() {
            write!(columns, "*").unwrap();
        } else {
            write!(columns, "{s}").unwrap();
        }

        let mut order_bys = String::new();
        let s = self.order_bys.join(",");
        if s.is_empty() {
            write!(order_bys, "{s}").unwrap();
        } else {
            write!(order_bys, "ORDER BY {s}").unwrap();
        }

        let mut filters = String::new();
        let s = self.filters.join(" and ");
        if !s.is_empty() {
            write!(filters, "where {s}").unwrap();
        } else {
            write!(filters, "").unwrap();
        }

        let from = self.from;
        write!(query, "SELECT {columns} FROM {from} {filters} {order_bys} ").unwrap();
        query
    }
}

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_show_tables(
        &mut self,
        bind_context: &BindContext,
        stmt: &ShowTablesStmt<'a>,
    ) -> Result<Plan> {
        let ShowTablesStmt {
            database,
            full,
            limit,
            with_history,
        } = stmt;

        let mut database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());

        if DatabaseCatalog::is_case_insensitive_db(database.as_str()) {
            database = database.to_uppercase();
        }

        let mut select_builder = if stmt.with_history {
            SelectBuilder::from("system.tables_with_history")
        } else {
            SelectBuilder::from("system.tables")
        };

        if *full {
            select_builder
                .with_column(format!("name AS Tables_in_{database}"))
                .with_column("'BASE TABLE' AS Table_type")
                .with_column("database AS table_catalog")
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
            .with_order_by("database")
            .with_order_by("name");

        select_builder.with_filter(format!("database = '{database}'"));

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

    pub(in crate::sql::planner::binder) async fn bind_show_create_table(
        &mut self,
        stmt: &ShowCreateTableStmt<'a>,
    ) -> Result<Plan> {
        let ShowCreateTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Table", Vu8::to_data_type()),
            DataField::new("Create Table", Vu8::to_data_type()),
        ]);
        Ok(Plan::ShowCreateTable(Box::new(ShowCreateTablePlan {
            catalog,
            database,
            table,
            schema,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_describe_table(
        &mut self,
        stmt: &DescribeTableStmt<'a>,
    ) -> Result<Plan> {
        let DescribeTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Field", Vu8::to_data_type()),
            DataField::new("Type", Vu8::to_data_type()),
            DataField::new("Null", Vu8::to_data_type()),
            DataField::new("Default", Vu8::to_data_type()),
            DataField::new("Extra", Vu8::to_data_type()),
        ]);

        Ok(Plan::DescribeTable(Box::new(DescribeTablePlan {
            catalog,
            database,
            table,
            schema,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_show_tables_status(
        &mut self,
        bind_context: &BindContext,
        stmt: &ShowTablesStatusStmt<'a>,
    ) -> Result<Plan> {
        let ShowTablesStatusStmt {
            database: db,
            limit,
        } = stmt;

        let database = db
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());

        let select_cols = "name AS Name, engine AS Engine, 0 AS Version, \
        NULL AS Row_format, num_rows AS Rows, NULL AS Avg_row_length, data_size AS Data_length, \
        NULL AS Max_data_length, NULL AS Index_length, NULL AS Data_free, NULL AS Auto_increment, \
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
        let backtrace = Backtrace::new();
        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL, &backtrace)?;
        self.bind_statement(bind_context, &stmt).await
    }

    pub(in crate::sql::planner::binder) async fn bind_create_table(
        &mut self,
        stmt: &CreateTableStmt<'a>,
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
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        // Take FUSE engine AS default engine
        let engine = engine.clone().unwrap_or(Engine::Fuse);
        let mut options: BTreeMap<String, String> = BTreeMap::new();
        for table_option in table_options.iter() {
            self.insert_table_option_with_validation(
                &mut options,
                table_option.0.to_lowercase(),
                table_option.1.to_string(),
            )?;
        }

        // If table is TRANSIENT, set a flag in table option
        if *transient {
            options.insert("TRANSIENT".to_owned(), "T".to_owned());
        }

        // Build table schema
        let (schema, field_comments) = match (&source, &as_query) {
            (Some(source), None) => {
                // `CREATE TABLE` without `AS SELECT ...`
                self.analyze_create_table_schema(source).await?
            }
            (None, Some(query)) => {
                // `CREATE TABLE AS SELECT ...` without column definitions
                let init_bind_context = BindContext::new();
                let (_s_expr, bind_context) = self.bind_query(&init_bind_context, query).await?;
                let fields = bind_context
                    .columns
                    .iter()
                    .map(|column_binding| {
                        DataField::new(
                            &column_binding.column_name,
                            *column_binding.data_type.clone(),
                        )
                    })
                    .collect();
                (DataSchemaRefExt::create(fields), vec![])
            }
            (Some(source), Some(query)) => {
                // e.g. `CREATE TABLE t (i INT) AS SELECT * from old_t` with columns speicified
                let (source_schema, source_coments) =
                    self.analyze_create_table_schema(source).await?;
                let init_bind_context = BindContext::new();
                let (_s_expr, bind_context) = self.bind_query(&init_bind_context, query).await?;
                let query_fields: Vec<DataField> = bind_context
                    .columns
                    .iter()
                    .map(|column_binding| {
                        DataField::new(
                            &column_binding.column_name,
                            *column_binding.data_type.clone(),
                        )
                    })
                    .collect();
                let source_fields = source_schema.fields().clone();
                let source_fields = self.concat_fields(source_fields, query_fields);
                (
                    DataSchemaRefExt::create(source_fields.to_vec()),
                    source_coments,
                )
            }
            _ => Err(ErrorCode::BadArguments(
                "Incorrect CREATE query: required list of column descriptions or AS section or SELECT..",
            ))?,
        };

        let mut table_meta = TableMeta {
            schema: schema.clone(),
            engine: engine.to_string(),
            options: options.clone(),
            field_comments,
            ..Default::default()
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
            table_meta
                .options
                .insert(OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());
        }

        let cluster_keys = self
            .analyze_cluster_keys(cluster_by, schema.clone())
            .await?;
        if !cluster_keys.is_empty() {
            let cluster_keys_sql = format!("({})", cluster_keys.join(", "));
            table_meta = table_meta.push_cluster_key(cluster_keys_sql);
        }

        let plan = CreateTablePlanV2 {
            if_not_exists: *if_not_exists,
            tenant: self.ctx.get_tenant(),
            catalog,
            database,
            table,
            table_meta,
            cluster_keys,
            as_select: if let Some(query) = as_query {
                let bind_context = BindContext::new();
                let stmt = Statement::Query(Box::new(*query.clone()));
                let select_plan = self.bind_statement(&bind_context, &stmt).await?;
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

    pub(in crate::sql::planner::binder) async fn bind_drop_table(
        &mut self,
        stmt: &DropTableStmt<'a>,
    ) -> Result<Plan> {
        let DropTableStmt {
            if_exists,
            catalog,
            database,
            table,
            all,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        Ok(Plan::DropTable(Box::new(DropTablePlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
            table,
            all: *all,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_undrop_table(
        &mut self,
        stmt: &UndropTableStmt<'a>,
    ) -> Result<Plan> {
        let UndropTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        Ok(Plan::UndropTable(Box::new(UndropTablePlan {
            tenant,
            catalog,
            database,
            table,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_alter_table(
        &mut self,
        stmt: &AlterTableStmt<'a>,
    ) -> Result<Plan> {
        let AlterTableStmt {
            if_exists,
            catalog,
            database,
            table,
            action,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        match action {
            AlterTableAction::RenameTable { new_table } => {
                let entities = vec![RenameTableEntity {
                    if_exists: *if_exists,
                    new_database: database.clone(),
                    new_table: normalize_identifier(new_table, &self.name_resolution_ctx).name,
                    catalog,
                    database,
                    table,
                }];

                Ok(Plan::RenameTable(Box::new(RenameTablePlan {
                    tenant,
                    entities,
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
        }
    }

    pub(in crate::sql::planner::binder) async fn bind_rename_table(
        &mut self,
        stmt: &RenameTableStmt<'a>,
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
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;
        let new_catalog = new_catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let new_database = new_database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let new_table = normalize_identifier(new_table, &self.name_resolution_ctx).name;

        if new_catalog != catalog {
            return Err(ErrorCode::BadArguments(
                "alter catalog not allowed while reanme table",
            ));
        }

        let entities = vec![RenameTableEntity {
            if_exists: *if_exists,
            catalog,
            database,
            table,
            new_database,
            new_table,
        }];

        Ok(Plan::RenameTable(Box::new(RenameTablePlan {
            tenant,
            entities,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_truncate_table(
        &mut self,
        stmt: &TruncateTableStmt<'a>,
    ) -> Result<Plan> {
        let TruncateTableStmt {
            catalog,
            database,
            table,
            purge,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        Ok(Plan::TruncateTable(Box::new(TruncateTablePlan {
            catalog,
            database,
            table,
            purge: *purge,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_optimize_table(
        &mut self,
        stmt: &OptimizeTableStmt<'a>,
    ) -> Result<Plan> {
        let OptimizeTableStmt {
            catalog,
            database,
            table,
            action,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;
        let action = action.map_or(OptimizeTableAction::Purge, |v| match v {
            AstOptimizeTableAction::All => OptimizeTableAction::All,
            AstOptimizeTableAction::Purge => OptimizeTableAction::Purge,
            AstOptimizeTableAction::Compact => OptimizeTableAction::Compact,
            AstOptimizeTableAction::Recluster => OptimizeTableAction::Recluster,
            AstOptimizeTableAction::ReclusterFinal => OptimizeTableAction::ReclusterFinal,
        });

        Ok(Plan::OptimizeTable(Box::new(OptimizeTablePlan {
            catalog,
            database,
            table,
            action,
        })))
    }

    pub(in crate::sql::planner::binder) async fn bind_exists_table(
        &mut self,
        stmt: &ExistsTableStmt<'a>,
    ) -> Result<Plan> {
        let ExistsTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        Ok(Plan::ExistsTable(Box::new(ExistsTablePlan {
            catalog,
            database,
            table,
        })))
    }

    async fn analyze_create_table_schema(
        &self,
        source: &CreateTableSource<'a>,
    ) -> Result<(DataSchemaRef, Vec<String>)> {
        let bind_context = BindContext::new();
        match source {
            CreateTableSource::Columns(columns) => {
                let mut scalar_binder = ScalarBinder::new(
                    &bind_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                );
                let mut fields = Vec::with_capacity(columns.len());
                let mut fields_comments = Vec::with_capacity(columns.len());
                for column in columns.iter() {
                    let name = normalize_identifier(&column.name, &self.name_resolution_ctx).name;
                    let data_type = TypeFactory::instance().get(column.data_type.to_string())?;

                    let field = DataField::new(&name, data_type.clone()).with_default_expr({
                        if let Some(default_expr) = &column.default_expr {
                            let (mut expr, expr_type) = scalar_binder.bind(default_expr).await?;
                            if compare_coercion(&data_type, &expr_type).is_err() {
                                return Err(ErrorCode::SemanticError(format!("column {name} is of type {} but default expression is of type {}", data_type, expr_type)));
                            }
                            if !expr_type.eq(&data_type) {
                                expr = Scalar::CastExpr(CastExpr {
                                    argument: Box::new(expr),
                                    from_type: Box::new(expr_type),
                                    target_type: Box::new(data_type),
                                })
                            }
                            let mut builder = PhysicalScalarBuilder;
                            let serializable_expr = builder.build(&expr)?;
                            Some(serde_json::to_string(&serializable_expr)?)
                        } else {
                            None
                        }
                    });
                    fields.push(field);
                    fields_comments.push(column.comment.clone().unwrap_or_default());
                }
                Ok((DataSchemaRefExt::create(fields), fields_comments))
            }
            CreateTableSource::Like {
                catalog,
                database,
                table,
            } => {
                let catalog = catalog
                    .as_ref()
                    .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database = database.as_ref().map_or_else(
                    || self.ctx.get_current_catalog(),
                    |ident| normalize_identifier(ident, &self.name_resolution_ctx).name,
                );
                let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;
                let table = self.ctx.get_table(&catalog, &database, &table_name).await?;
                Ok((table.schema(), table.field_comments().clone()))
            }
        }
    }

    fn insert_table_option_with_validation(
        &self,
        options: &mut BTreeMap<String, String>,
        key: String,
        value: String,
    ) -> Result<()> {
        if is_reserved_opt_key(&key) {
            Err(ErrorCode::BadOption(format!(
                "the following table options are reserved, please do not specify them in the CREATE TABLE statement: {}",
                key
            )))
        } else if options.insert(key.clone(), value).is_some() {
            Err(ErrorCode::BadOption(format!(
                "Duplicated table option: {key}"
            )))
        } else {
            Ok(())
        }
    }

    async fn analyze_cluster_keys(
        &mut self,
        cluster_by: &[Expr<'a>],
        schema: DataSchemaRef,
    ) -> Result<Vec<String>> {
        // Build a temporary BindContext to resolve the expr
        let mut bind_context = BindContext::new();
        for field in schema.fields() {
            let column = ColumnBinding {
                database_name: None,
                table_name: None,
                column_name: field.name().clone(),
                // A dummy index is fine, since we won't actually evaluate the expression
                index: 0,
                data_type: Box::new(field.data_type().clone()),
                visible_in_unqualified_wildcard: false,
            };
            bind_context.columns.push(column);
        }
        let mut scalar_binder = ScalarBinder::new(
            &bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );

        let mut cluster_keys = Vec::with_capacity(cluster_by.len());
        for cluster_by in cluster_by.iter() {
            let (cluster_key, _) = scalar_binder.bind(cluster_by).await?;
            if !cluster_key.is_deterministic() {
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

    fn concat_fields(
        &self,
        mut source_fields: Vec<DataField>,
        query_fields: Vec<DataField>,
    ) -> Vec<DataField> {
        let mut name_set = HashSet::new();
        for field in source_fields.iter() {
            name_set.insert(field.name().clone());
        }
        for query_field in query_fields.iter() {
            if !name_set.contains(query_field.name()) {
                source_fields.push(query_field.clone());
            }
        }
        source_fields
    }
}
