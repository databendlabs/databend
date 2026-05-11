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
use databend_common_catalog::catalog::RefApi;
use databend_common_catalog::catalog::meta_store_client;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::StringType;
use databend_common_expression::utils::FromData;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::Planner;
use databend_common_sql::check_table_ref_access;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_stream::stream_table::StreamTable;
use log::warn;

use crate::generate_default_catalog_meta;
use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::collect_visible_tables;
use crate::util::disable_catalog_refresh;
use crate::util::extract_push_down_string_filters;
use crate::util::push_down_filter_contains_column;

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
        let mut branches: Vec<String> = Vec::with_capacity(rows.len());
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
            branches.push(column_info.branch_name);
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
            StringType::from_data(branches),
        ]))
    }
}

struct TableColumnInfo {
    database_name: String,
    table_name: String,
    branch_name: String,

    column: TableField,
    column_comment: String,
}

pub(crate) struct DumpedTable {
    pub(crate) table_name: String,
    pub(crate) branch_name: String,
    pub(crate) table: Arc<dyn Table>,
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
            TableField::new("branch", TableDataType::String),
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
                meta: generate_default_catalog_meta(),
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
        for (database, table_sources) in database_and_tables {
            for table_source in table_sources {
                let DumpedTable {
                    table_name,
                    branch_name,
                    table,
                } = table_source;
                match table.engine() {
                    VIEW_ENGINE => {
                        let fields = if let Some(query) = table.options().get(QUERY) {
                            // Replay stored view SQL against base tables.
                            let mut planner =
                                Planner::new(ctx.clone()).with_suppress_wap_branch(true);
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
                                table_name: table_name.clone(),
                                branch_name: branch_name.clone(),
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
                                        table_name: table_name.clone(),
                                        branch_name: branch_name.clone(),
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
                                table_name: table_name.clone(),
                                branch_name: branch_name.clone(),
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
) -> Result<Vec<(String, Vec<DumpedTable>)>> {
    // For performance considerations, we do not require the most up-to-date table information here
    let catalog = disable_catalog_refresh(catalog.clone())?;

    // Extract filters from push_downs
    let func_ctx = ctx.get_function_context()?;
    let mut filters =
        extract_push_down_string_filters(&push_downs, &["database", "table", "branch"], &func_ctx)?;
    let filtered_db_names = filters.remove(0);
    let filtered_table_names = filters.remove(0);
    let filtered_branch_names = filters.remove(0);
    let branch_filter_present = push_down_filter_contains_column(&push_downs, "branch");
    let has_non_empty_branch_filter = filtered_branch_names
        .iter()
        .any(|branch_name| !branch_name.is_empty());
    // If a branch predicate exists but no equality branch names were extracted
    // (e.g. `branch != ''` or `branch LIKE 'dev%'`), enumerate all branches and
    // let the residual filter evaluate the predicate.
    let branch_filter_needs_full_scan = branch_filter_present && filtered_branch_names.is_empty();

    if has_non_empty_branch_filter || branch_filter_needs_full_scan {
        check_table_ref_access(ctx.as_ref())?;
    }

    // Use unified visibility collection from util.rs
    let db_with_tables =
        collect_visible_tables(ctx, &catalog, &filtered_db_names, &filtered_table_names).await?;

    // No branch access: expose only base table rows with `branch = ''`. This
    // includes metadata rewrites that push down `branch = ''` for base tables.
    if !has_non_empty_branch_filter && !branch_filter_needs_full_scan {
        return Ok(db_with_tables
            .into_iter()
            .map(|db| {
                let tables = db
                    .tables
                    .into_iter()
                    .map(|table| {
                        let table_name = table.name().to_string();
                        DumpedTable {
                            table_name,
                            branch_name: String::new(),
                            table,
                        }
                    })
                    .collect();
                (db.name, tables)
            })
            .collect());
    }

    let tenant = ctx.get_tenant();
    let meta_api = meta_store_client();
    let mut db_with_dumped_tables = Vec::new();
    for db in db_with_tables {
        let mut tables = Vec::new();
        for table in db.tables {
            let table_name = table.name().to_string();
            let branch_names = if branch_filter_needs_full_scan {
                let branches = meta_api
                    .list_table_branches(table.get_id())
                    .await
                    .map_err(ErrorCode::from)?;
                let mut branch_names = Vec::with_capacity(branches.len() + 1);
                branch_names.push(String::new());
                branch_names.extend(branches.into_iter().map(|branch| branch.branch_name));
                branch_names
            } else {
                filtered_branch_names.clone()
            };

            for branch_name in &branch_names {
                if branch_name.is_empty() {
                    tables.push(DumpedTable {
                        table_name: table_name.clone(),
                        branch_name: String::new(),
                        table: table.clone(),
                    });
                    continue;
                }

                match catalog
                    .get_table_branch(&tenant, &db.name, &table_name, branch_name, false)
                    .await
                {
                    Ok(branch_table) => tables.push(DumpedTable {
                        table_name: table_name.clone(),
                        branch_name: branch_name.clone(),
                        table: branch_table,
                    }),
                    Err(err) => {
                        warn!(
                            "failed to get branch columns for {}.{}/{}: {}",
                            db.name, table_name, branch_name, err
                        );
                    }
                }
            }
        }
        if !tables.is_empty() {
            db_with_dumped_tables.push((db.name, tables));
        }
    }

    Ok(db_with_dumped_tables)
}
