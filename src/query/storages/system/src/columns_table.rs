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

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::StringType;
use databend_common_expression::utils::FromData;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::Planner;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_stream::stream_table::StreamTable;
use log::warn;

use crate::generate_catalog_meta;
use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::collect_visible_tables;
use crate::util::disable_catalog_refresh;
use crate::util::extract_leveled_strings;

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
        let catalog_mgr = CatalogManager::instance();
        let catalog = catalog_mgr
            .get_catalog(
                ctx.get_tenant().tenant_name(),
                self.get_table_info().catalog(),
                ctx.session_state()?,
            )
            .await?;
        let rows = self.dump_table_columns(ctx, push_downs, &catalog).await?;
        let mut names: Vec<String> = Vec::with_capacity(rows.len());
        let mut tables: Vec<String> = Vec::with_capacity(rows.len());
        let mut databases: Vec<String> = Vec::with_capacity(rows.len());
        let mut types: Vec<String> = Vec::with_capacity(rows.len());
        let mut data_types: Vec<String> = Vec::with_capacity(rows.len());
        let mut default_kinds: Vec<String> = Vec::with_capacity(rows.len());
        let mut default_exprs: Vec<String> = Vec::with_capacity(rows.len());
        let mut is_nullables: Vec<String> = Vec::with_capacity(rows.len());
        let mut comments: Vec<String> = Vec::with_capacity(rows.len());
        for column_info in rows.into_iter() {
            names.push(column_info.column.name().clone());
            tables.push(column_info.table_name);
            databases.push(column_info.database_name);
            types.push(column_info.column.data_type().wrapped_display());
            let data_type = column_info
                .column
                .data_type()
                .remove_recursive_nullable()
                .sql_name();
            data_types.push(data_type);

            let mut default_kind = "".to_string();
            let mut default_expr = "".to_string();
            if let Some(expr) = column_info.column.default_expr() {
                default_kind = "DEFAULT".to_string();
                default_expr = expr.to_string();
            }
            default_kinds.push(default_kind);
            default_exprs.push(default_expr);
            if column_info.column.is_nullable() {
                is_nullables.push("YES".to_string());
            } else {
                is_nullables.push("NO".to_string());
            }

            comments.push(column_info.column_comment);
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

struct TableColumnInfo {
    database_name: String,
    table_name: String,

    column: TableField,
    column_comment: String,
}

impl ColumnsTable {
    pub fn create(table_id: u64, ctl_name: &str) -> Arc<dyn Table> {
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
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), ctl_name).into(),
                meta: generate_catalog_meta(ctl_name),
                ..Default::default()
            }),
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(ColumnsTable { table_info })
    }

    #[async_backtrace::framed]
    async fn dump_table_columns(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        catalog: &Arc<dyn Catalog>,
    ) -> Result<Vec<TableColumnInfo>> {
        let database_and_tables = dump_tables(&ctx, push_downs, catalog).await?;

        let mut rows: Vec<TableColumnInfo> = vec![];
        for (database, tables) in database_and_tables {
            for table in tables {
                match table.engine() {
                    VIEW_ENGINE => {
                        let fields = if let Some(query) = table.options().get(QUERY) {
                            let mut planner = Planner::new(ctx.clone());
                            match planner.plan_sql(query).await {
                                Ok((plan, _)) => {
                                    infer_table_schema(&plan.schema())?.fields().clone()
                                }
                                Err(e) => {
                                    // If VIEW SELECT QUERY plan err, should return empty. not destroy the query.
                                    warn!(
                                        "failed to get columns for {}: {}",
                                        table.get_table_info().desc,
                                        e
                                    );
                                    vec![]
                                }
                            }
                        } else {
                            vec![]
                        };
                        for field in fields {
                            rows.push(TableColumnInfo {
                                database_name: database.clone(),
                                table_name: table.name().into(),
                                column: field.clone(),
                                column_comment: "".to_string(),
                            })
                        }
                    }
                    STREAM_ENGINE => {
                        let stream = StreamTable::try_from_table(table.as_ref())?;
                        match stream.source_table(ctx.clone()).await {
                            Ok(source_table) => {
                                for field in source_table.schema().fields() {
                                    rows.push(TableColumnInfo {
                                        database_name: database.clone(),
                                        table_name: table.name().into(),
                                        column: field.clone(),
                                        column_comment: "".to_string(),
                                    })
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "failed to get columns for {}: {}",
                                    table.get_table_info().desc,
                                    e
                                );
                            }
                        }
                    }
                    _ => {
                        let schema = table.schema();
                        let field_comments = table.field_comments();

                        for (idx, field) in schema.fields().iter().enumerate() {
                            let comment = if field_comments.len() == schema.fields.len()
                                && !field_comments[idx].is_empty()
                            {
                                // can not use debug print, will add double quote
                                field_comments[idx].clone()
                            } else {
                                "".to_string()
                            };
                            rows.push(TableColumnInfo {
                                database_name: database.clone(),
                                table_name: table.name().into(),
                                column: field.clone(),
                                column_comment: comment,
                            })
                        }
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
    catalog: &Arc<dyn Catalog>,
) -> Result<Vec<(String, Vec<Arc<dyn Table>>)>> {
    // For performance considerations, we do not require the most up-to-date table information here
    let catalog = disable_catalog_refresh(catalog.clone())?;

    // Extract filters from push_downs
    let func_ctx = ctx.get_function_context()?;
    let (filtered_db_names, filtered_table_names) = extract_filters(&push_downs, &func_ctx)?;

    // Use unified visibility collection from util.rs
    let db_with_tables =
        collect_visible_tables(ctx, &catalog, &filtered_db_names, &filtered_table_names).await?;

    // Convert to the expected return format
    Ok(db_with_tables
        .into_iter()
        .map(|db| (db.name, db.tables))
        .collect())
}

fn extract_filters(
    push_downs: &Option<PushDownInfo>,
    func_ctx: &databend_common_expression::FunctionContext,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut filtered_db_names = vec![];
    let mut filtered_table_names = vec![];

    if let Some(push_downs) = push_downs {
        if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
            let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
            (filtered_db_names, filtered_table_names) =
                extract_leveled_strings(&expr, &["database", "table"], func_ctx)?;
        }
    }

    Ok((filtered_db_names, filtered_table_names))
}
