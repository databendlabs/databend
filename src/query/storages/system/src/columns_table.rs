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

use std::sync::Arc;

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::StringType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::Planner;
use databend_common_storages_view::view_table::QUERY;
use databend_common_storages_view::view_table::VIEW_ENGINE;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::find_eq_filter;

pub struct ColumnsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for ColumnsTable {
    const NAME: &'static str = "system.columns";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let rows = self.dump_table_columns(ctx, push_downs).await?;
        let mut names: Vec<String> = Vec::with_capacity(rows.len());
        let mut tables: Vec<String> = Vec::with_capacity(rows.len());
        let mut databases: Vec<String> = Vec::with_capacity(rows.len());
        let mut types: Vec<String> = Vec::with_capacity(rows.len());
        let mut data_types: Vec<String> = Vec::with_capacity(rows.len());
        let mut default_kinds: Vec<String> = Vec::with_capacity(rows.len());
        let mut default_exprs: Vec<String> = Vec::with_capacity(rows.len());
        let mut is_nullables: Vec<String> = Vec::with_capacity(rows.len());
        let mut comments: Vec<String> = Vec::with_capacity(rows.len());
        for (database_name, table_name, comment, field) in rows.into_iter() {
            names.push(field.name().clone());
            tables.push(table_name);
            databases.push(database_name);
            types.push(field.data_type().wrapped_display());
            let data_type = field.data_type().remove_recursive_nullable().sql_name();
            data_types.push(data_type);

            let mut default_kind = "".to_string();
            let mut default_expr = "".to_string();
            if let Some(expr) = field.default_expr() {
                default_kind = "DEFAULT".to_string();
                default_expr = expr.to_string();
            }
            default_kinds.push(default_kind);
            default_exprs.push(default_expr);
            if field.is_nullable() {
                is_nullables.push("YES".to_string());
            } else {
                is_nullables.push("NO".to_string());
            }

            comments.push(comment);
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(databases),
            StringType::from_data(tables),
            StringType::from_data(types),
            StringType::from_data(data_types),
            StringType::from_data(default_kinds),
            StringType::from_data(default_exprs),
            StringType::from_data(is_nullables),
            StringType::from_data(comments),
        ]))
    }
}

impl ColumnsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("database", TableDataType::String),
            TableField::new("table", TableDataType::String),
            // inner wrapped display style
            TableField::new("type", TableDataType::String),
            // mysql display style for 3rd party tools
            TableField::new("data_type", TableDataType::String),
            TableField::new("default_kind", TableDataType::String),
            TableField::new("default_expression", TableDataType::String),
            TableField::new("is_nullable", TableDataType::String),
            TableField::new("comment", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'columns'".to_string(),
            name: "columns".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemColumns".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(ColumnsTable { table_info })
    }

    #[async_backtrace::framed]
    async fn dump_table_columns(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Vec<(String, String, String, TableField)>> {
        let database_and_tables = dump_tables(&ctx, push_downs).await?;

        let mut rows: Vec<(String, String, String, TableField)> = vec![];
        for (database, tables) in database_and_tables {
            for table in tables {
                if table.engine() != VIEW_ENGINE {
                    let schema = table.schema();
                    let field_comments = table.field_comments();
                    let n_fields = schema.fields().len();
                    for (idx, field) in schema.fields().iter().enumerate() {
                        // compatibility: creating table in the old planner will not have `fields_comments`
                        let comment = if field_comments.len() == n_fields
                            && !field_comments[idx].is_empty()
                        {
                            // can not use debug print, will add double quote
                            format!("'{}'", &field_comments[idx].as_str().replace('\'', "\\'"))
                        } else {
                            "".to_string()
                        };
                        rows.push((
                            database.clone(),
                            table.name().into(),
                            comment,
                            field.clone(),
                        ))
                    }
                } else {
                    let fields = generate_fields(&ctx, &table).await?;
                    for field in fields {
                        rows.push((
                            database.clone(),
                            table.name().into(),
                            "".to_string(),
                            field.clone(),
                        ))
                    }
                }
            }
        }

        Ok(rows)
    }
}

pub(crate) async fn dump_tables(
    ctx: &Arc<dyn TableContext>,
    push_downs: Option<PushDownInfo>,
) -> Result<Vec<(String, Vec<Arc<dyn Table>>)>> {
    let tenant = ctx.get_tenant();
    let catalog = ctx.get_catalog(CATALOG_DEFAULT).await?;

    let mut tables: Vec<String> = Vec::new();
    let mut databases: Vec<String> = Vec::new();

    if let Some(push_downs) = push_downs {
        if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
            let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
            find_eq_filter(&expr, &mut |col_name, scalar| {
                if col_name == "database" {
                    if let Scalar::String(database) = scalar {
                        if !databases.contains(database) {
                            databases.push(database.clone());
                        }
                    }
                } else if col_name == "table" {
                    if let Scalar::String(table) = scalar {
                        if !tables.contains(table) {
                            tables.push(table.clone());
                        }
                    }
                }
            });
        }
    }

    let visibility_checker = ctx.get_visibility_checker().await?;

    let mut final_dbs: Vec<(String, u64)> = Vec::new();

    if databases.is_empty() {
        let all_databases = catalog.list_databases(&tenant).await?;
        for db in all_databases {
            let db_id = db.get_db_info().ident.db_id;
            let db_name = db.name();
            if visibility_checker.check_database_visibility(CATALOG_DEFAULT, db_name, db_id) {
                final_dbs.push((db_name.to_string(), db_id));
            }
        }
    } else {
        for db in databases {
            let db_id = catalog
                .get_database(&tenant, &db)
                .await?
                .get_db_info()
                .ident
                .db_id;
            if visibility_checker.check_database_visibility(CATALOG_DEFAULT, &db, db_id) {
                final_dbs.push((db.to_string(), db_id));
            }
        }
    }

    let mut final_tables: Vec<(String, Vec<Arc<dyn Table>>)> = Vec::with_capacity(final_dbs.len());
    for (database, db_id) in final_dbs {
        let tables = if tables.is_empty() {
            if let Ok(table) = catalog.list_tables(&tenant, &database).await {
                table
            } else {
                vec![]
            }
        } else {
            let mut res = Vec::new();
            for table in &tables {
                if let Ok(table) = catalog.get_table(&tenant, &database, table).await {
                    res.push(table);
                }
            }
            res
        };
        let mut filtered_tables = Vec::with_capacity(tables.len());
        for table in tables {
            if visibility_checker.check_table_visibility(
                CATALOG_DEFAULT,
                &database,
                table.name(),
                db_id,
                table.get_id(),
            ) {
                filtered_tables.push(table);
            }
        }
        final_tables.push((database, filtered_tables));
    }
    Ok(final_tables)
}

async fn generate_fields(
    ctx: &Arc<dyn TableContext>,
    table: &Arc<dyn Table>,
) -> Result<Vec<TableField>> {
    if table.engine() != VIEW_ENGINE {
        return Ok(table.schema().fields().clone());
    }

    Ok(if let Some(query) = table.options().get(QUERY) {
        let mut planner = Planner::new(ctx.clone());
        match planner.plan_sql(query).await {
            Ok((plan, _)) => infer_table_schema(&plan.schema())?.fields().clone(),
            Err(_) => {
                // If VIEW SELECT QUERY plan err, should return empty. not destroy the query.
                vec![]
            }
        }
    } else {
        vec![]
    })
}
