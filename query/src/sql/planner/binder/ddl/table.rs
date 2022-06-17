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

use common_ast::ast::*;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::NullableType;
use common_datavalues::ToDataType;
use common_datavalues::TypeFactory;
use common_datavalues::Vu8;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableMeta;
use common_planners::*;

use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::Binder;
use crate::sql::is_reserved_opt_key;
use crate::sql::plans::Plan;
use crate::sql::BindContext;
use crate::sql::ColumnBinding;
use crate::sql::ScalarExpr;
use crate::sql::OPT_KEY_DATABASE_ID;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_show_tables(
        &mut self,
        stmt: &ShowTablesStmt<'a>,
    ) -> Result<Plan> {
        let ShowTablesStmt {
            database,
            full,
            limit,
            with_history,
        } = stmt;

        let database = database.as_ref().map(|ident| ident.name.to_lowercase());
        let kind = match limit {
            Some(ShowLimit::Like { pattern }) => PlanShowKind::Like(pattern.clone()),
            Some(ShowLimit::Where { selection }) => PlanShowKind::Like(selection.to_string()),
            None => PlanShowKind::All,
        };

        Ok(Plan::ShowTables(Box::new(ShowTablesPlan {
            kind,
            showfull: *full,
            fromdb: database,
            with_history: *with_history,
        })))
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
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = table.name.to_lowercase();
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
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = table.name.to_lowercase();
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
        stmt: &ShowTablesStatusStmt<'a>,
    ) -> Result<Plan> {
        let ShowTablesStatusStmt { database, limit } = stmt;

        let database = database.as_ref().map(|ident| ident.name.to_lowercase());
        let kind = match limit {
            Some(ShowLimit::Like { pattern }) => PlanShowKind::Like(pattern.clone()),
            Some(ShowLimit::Where { selection }) => PlanShowKind::Like(selection.to_string()),
            None => PlanShowKind::All,
        };

        Ok(Plan::ShowTablesStatus(Box::new(ShowTablesStatusPlan {
            kind,
            fromdb: database,
        })))
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
            comment: _,
            transient,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = table.name.to_lowercase();

        // Take FUSE engine as default engine
        let mut engine = Engine::Fuse;
        let mut options: BTreeMap<String, String> = BTreeMap::new();
        for table_option in table_options.iter() {
            match table_option {
                TableOption::Engine(table_engine) => {
                    engine = table_engine.clone();
                }
                TableOption::Comment(comment) => self.insert_table_option_with_validation(
                    &mut options,
                    "COMMENT".to_string(),
                    comment.clone(),
                )?,
            }
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
                            column_binding.data_type.clone(),
                        )
                    })
                    .collect();
                (DataSchemaRefExt::create(fields), vec![])
            }
            // TODO(leiysky): Support `CREATE TABLE AS SELECT` with specified column definitions
            _ => Err(ErrorCode::UnImplement("Unsupported CREATE TABLE statement"))?,
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
            // here we keep the db id as an entry of `table_meta.options`.
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

        let plan = CreateTablePlan {
            if_not_exists: *if_not_exists,
            tenant: self.ctx.get_tenant(),
            catalog,
            database,
            table,
            table_meta,
            cluster_keys,
            as_select: if as_query.is_some() {
                Err(ErrorCode::UnImplement(
                    "Unsupported CREATE TABLE ... AS ...",
                ))?
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
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = table.name.to_lowercase();

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
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = table.name.to_lowercase();

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
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = table.name.to_lowercase();

        match action {
            AlterTableAction::RenameTable { new_table } => {
                let entities = vec![RenameTableEntity {
                    if_exists: *if_exists,
                    new_database: database.clone(),
                    new_table: new_table.name.clone(),
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
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = table.name.to_lowercase();
        let new_catalog = new_catalog
            .as_ref()
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let new_database = new_database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let new_table = new_table.name.to_lowercase();

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
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = table.name.to_lowercase();

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
            .map(|catalog| catalog.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = table.name.to_lowercase();
        let action = action.unwrap_or(OptimizeTableAction::Purge);

        Ok(Plan::OptimizeTable(Box::new(OptimizeTablePlan {
            catalog,
            database,
            table,
            action,
        })))
    }

    async fn analyze_create_table_schema(
        &self,
        source: &CreateTableSource<'a>,
    ) -> Result<(DataSchemaRef, Vec<String>)> {
        let bind_context = BindContext::new();
        match source {
            CreateTableSource::Columns(columns) => {
                let mut scalar_binder =
                    ScalarBinder::new(&bind_context, self.ctx.clone(), self.metadata.clone());
                let mut fields = Vec::with_capacity(columns.len());
                let mut fields_comments = Vec::with_capacity(columns.len());
                for column in columns.iter() {
                    let name = column.name.name.clone();
                    let mut data_type = TypeFactory::instance()
                        .get(column.data_type.to_string())?
                        .clone();
                    if column.nullable {
                        data_type = NullableType::new_impl(data_type);
                    }
                    let field = DataField::new(&name, data_type).with_default_expr({
                        if let Some(default_expr) = &column.default_expr {
                            scalar_binder.bind(default_expr).await?;
                            Some(default_expr.to_string())
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
                    .map(|catalog| catalog.name.to_lowercase())
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database = database.as_ref().map_or_else(
                    || self.ctx.get_current_catalog(),
                    |ident| ident.name.to_lowercase(),
                );
                let table_name = table.name.to_lowercase();
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
            Err(ErrorCode::BadOption(format!("the following table options are reserved, please do not specify them in the CREATE TABLE statement: {}",
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
                table_name: None,
                column_name: field.name().clone(),
                // A dummy index is fine, since we won't actually evaluate the expression
                index: 0,
                data_type: field.data_type().clone(),
                visible_in_unqualified_wildcard: false,
            };
            bind_context.columns.push(column);
        }
        let mut scalar_binder =
            ScalarBinder::new(&bind_context, self.ctx.clone(), self.metadata.clone());

        let mut cluster_keys = Vec::with_capacity(cluster_by.len());
        for cluster_by in cluster_by.iter() {
            let (cluster_key, _) = scalar_binder.bind(cluster_by).await?;
            if !cluster_key.is_deterministic() {
                return Err(ErrorCode::InvalidClusterKeys(format!(
                    "Cluster by expression `{:#}` is not deterministic",
                    cluster_by
                )));
            }
            cluster_keys.push(format!("{:#}", cluster_by));
        }

        Ok(cluster_keys)
    }
}
