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

use common_ast::ast::ShowColumnsStmt;
use common_ast::ast::Statement;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::types::StringType;
use common_expression::utils::FromData;
use common_expression::DataBlock;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_sql::normalize_identifier;
use common_sql::NameResolutionContext;
use common_sql::Planner;
use common_storages_view::view_table::QUERY;
use common_storages_view::view_table::VIEW_ENGINE;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

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
    async fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let query = ctx.get_query_str();
        let settings = ctx.get_shard_settings();
        let tokens = tokenize_sql(&query)?;
        let (stmt, _) = parse_sql(&tokens, settings.get_sql_dialect()?)?;
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let location = match stmt {
            Statement::ShowColumns(ShowColumnsStmt {
                catalog,
                database,
                table,
                ..
            }) => {
                let catalog_name = catalog
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &name_resolution_ctx).name)
                    .unwrap_or_else(|| ctx.get_current_catalog());
                let database_name = database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &name_resolution_ctx).name)
                    .unwrap_or_else(|| ctx.get_current_database());
                let table_name = normalize_identifier(&table, &name_resolution_ctx).name;
                Some((catalog_name, database_name, table_name))
            }
            _ => None,
        };
        let rows = self.dump_table_columns(ctx, location).await?;
        let mut names: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut tables: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut databases: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut types: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut data_types: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut default_kinds: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut default_exprs: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut is_nullables: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut comments: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        for (database_name, table_name, field) in rows.into_iter() {
            names.push(field.name().clone().into_bytes());
            tables.push(table_name.into_bytes());
            databases.push(database_name.into_bytes());
            types.push(field.data_type().wrapped_display().into_bytes());
            let data_type = field.data_type().remove_recursive_nullable().sql_name();
            data_types.push(data_type.into_bytes());

            let mut default_kind = "".to_string();
            let mut default_expr = "".to_string();
            if let Some(expr) = field.default_expr() {
                default_kind = "DEFAULT".to_string();
                default_expr = expr.to_string();
            }
            default_kinds.push(default_kind.into_bytes());
            default_exprs.push(default_expr.into_bytes());
            if field.is_nullable() {
                is_nullables.push("YES".to_string().into_bytes());
            } else {
                is_nullables.push("NO".to_string().into_bytes());
            }

            comments.push("".to_string().into_bytes());
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
        location: Option<(String, String, String)>,
    ) -> Result<Vec<(String, String, TableField)>> {
        let tenant = ctx.get_tenant();
        let mut rows: Vec<(String, String, TableField)> = vec![];
        if let Some((catalog_name, database_name, table_name)) = location {
            let catalog = ctx.get_catalog(&catalog_name)?;
            let table = catalog
                .get_table(&tenant, &database_name, &table_name)
                .await?;
            let fields = self.get_table_field(&ctx, &table).await?;
            for field in fields {
                rows.push((database_name.clone(), table.name().into(), field.clone()))
            }
        } else {
            let catalog = ctx.get_catalog(CATALOG_DEFAULT)?;
            let databases = catalog.list_databases(tenant.as_str()).await?;

            for database in databases {
                for table in catalog
                    .list_tables(tenant.as_str(), database.name())
                    .await?
                {
                    let fields = self.get_table_field(&ctx, &table).await?;
                    for field in fields {
                        rows.push((database.name().into(), table.name().into(), field.clone()))
                    }
                }
            }
        }

        Ok(rows)
    }

    async fn get_table_field(
        &self,
        ctx: &Arc<dyn TableContext>,
        table: &Arc<dyn Table>,
    ) -> Result<Vec<TableField>> {
        if table.engine() == VIEW_ENGINE {
            if let Some(query) = table.options().get(QUERY) {
                let mut planner = Planner::new(ctx.clone());
                let (plan, _) = planner.plan_sql(query).await?;
                let schema = infer_table_schema(&plan.schema())?;
                Ok(schema.fields().clone())
            } else {
                Err(ErrorCode::Internal(
                    "Logical error, View Table must have a SelectQuery inside.",
                ))
            }
        } else {
            Ok(table.schema().fields().clone())
        }
    }
}
