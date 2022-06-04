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

use common_ast::ast::CreateTableSource;
use common_ast::ast::CreateTableStmt;
use common_ast::ast::Engine;
use common_ast::ast::Expr;
use common_ast::ast::TableOption;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::NullableType;
use common_datavalues::TypeFactory;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableMeta;
use common_planners::CreateTablePlan;

use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::Binder;
use crate::sql::is_reserved_opt_key;
use crate::sql::plans::Plan;
use crate::sql::BindContext;
use crate::sql::ColumnBinding;
use crate::sql::OPT_KEY_DATABASE_ID;

impl<'a> Binder {
    /// Basically copied from `DfCreateTable::table_meta `
    async fn analyze_create_table_schema(
        &self,
        source: &CreateTableSource<'a>,
    ) -> Result<DataSchemaRef> {
        let bind_context = BindContext::new();
        match source {
            CreateTableSource::Columns(columns) => {
                let mut scalar_binder =
                    ScalarBinder::new(&bind_context, self.ctx.clone(), self.metadata.clone());
                let mut fields = Vec::with_capacity(columns.len());
                for column in columns.iter() {
                    let name = column.name.name.clone();
                    let mut data_type = TypeFactory::instance()
                        .get(column.data_type.to_string())?
                        .clone();
                    if column.nullable {
                        data_type = NullableType::new_impl(data_type);
                    }
                    let field = DataField::new(name.as_str(), data_type).with_default_expr({
                        if let Some(default_expr) = &column.default_expr {
                            scalar_binder.bind(default_expr).await?;
                            Some(default_expr.to_string())
                        } else {
                            None
                        }
                    });
                    fields.push(field);
                }
                Ok(DataSchemaRefExt::create(fields))
            }
            CreateTableSource::Like { database, table } => {
                let catalog = self.ctx.get_current_catalog();
                let database = database.as_ref().map_or_else(
                    || self.ctx.get_current_catalog(),
                    |ident| ident.name.clone(),
                );
                let table_name = table.name.as_str();
                let table = self
                    .ctx
                    .get_table(catalog.as_str(), database.as_str(), table_name)
                    .await?;
                Ok(table.schema())
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

    async fn validate_expr(&self, schema: DataSchemaRef, expr: &Expr<'a>) -> Result<()> {
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
        scalar_binder.bind(expr).await?;

        Ok(())
    }

    pub(in crate::sql::planner::binder) async fn bind_create_table(
        &mut self,
        stmt: &CreateTableStmt<'a>,
    ) -> Result<Plan> {
        let catalog = self.ctx.get_current_catalog();
        let database = stmt
            .database
            .as_ref()
            .map(|ident| ident.name.to_lowercase())
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = stmt.table.name.to_lowercase();

        // Take FUSE engine as default engine
        let mut engine = Engine::Fuse;
        let mut table_options: BTreeMap<String, String> = BTreeMap::new();
        for table_option in stmt.table_options.iter() {
            match table_option {
                TableOption::Engine(table_engine) => {
                    engine = table_engine.clone();
                }
                TableOption::Comment(comment) => self.insert_table_option_with_validation(
                    &mut table_options,
                    "COMMENT".to_string(),
                    comment.clone(),
                )?,
            }
        }

        // Build table schema
        let schema = match (&stmt.source, &stmt.as_query) {
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
                            column_binding.column_name.as_str(),
                            column_binding.data_type.clone(),
                        )
                    })
                    .collect();
                DataSchemaRefExt::create(fields)
            }
            // TODO(leiysky): Support `CREATE TABLE AS SELECT` with specified column definitions
            _ => Err(ErrorCode::UnImplement("Unsupported CREATE TABLE statement"))?,
        };

        let mut meta = TableMeta {
            schema: schema.clone(),
            engine: engine.to_string(),
            options: table_options.clone(),
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
            let catalog = self.ctx.get_catalog(catalog.as_str())?;
            let db = catalog
                .get_database(self.ctx.get_tenant().as_str(), database.as_str())
                .await?;
            let db_id = db.get_db_info().ident.db_id;
            meta.options
                .insert(OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());
        }

        // Validate cluster keys and build the `cluster_keys` of `TableMeta`
        let mut cluster_keys = Vec::with_capacity(stmt.cluster_by.len());
        for cluster_key in stmt.cluster_by.iter() {
            self.validate_expr(schema.clone(), cluster_key).await?;
            cluster_keys.push(cluster_key.to_string());
        }
        if !cluster_keys.is_empty() {
            let order_keys_sql = format!("({})", cluster_keys.join(", "));
            meta.cluster_keys = Some(order_keys_sql);
        }

        let plan = CreateTablePlan {
            if_not_exists: stmt.if_not_exists,
            tenant: self.ctx.get_tenant(),
            catalog,
            db: database,
            table,
            table_meta: meta,
            cluster_keys,
            as_select: if stmt.as_query.is_some() {
                Err(ErrorCode::UnImplement(
                    "Unsupported CREATE TABLE ... AS ...",
                ))?
            } else {
                None
            },
        };
        Ok(Plan::CreateTable(plan))
    }
}
