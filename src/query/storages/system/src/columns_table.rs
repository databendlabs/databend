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
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::database::Database;
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
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::Planner;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::QUERY;
use databend_common_storages_view::view_table::VIEW_ENGINE;
use log::warn;

use crate::generate_catalog_meta;
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
        let catalog_mgr = CatalogManager::instance();
        let catalog = catalog_mgr
            .get_catalog(
                ctx.get_tenant().tenant_name(),
                self.get_table_info().catalog(),
                ctx.session_state(),
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
    ) -> Result<Vec<(String, String, String, TableField)>> {
        let database_and_tables = dump_tables(&ctx, push_downs, catalog).await?;

        let mut rows: Vec<(String, String, String, TableField)> = vec![];
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
                            rows.push((
                                database.clone(),
                                table.name().into(),
                                "".to_string(),
                                field.clone(),
                            ))
                        }
                    }
                    STREAM_ENGINE => {
                        let stream = StreamTable::try_from_table(table.as_ref())?;
                        match stream.source_table(ctx.clone()).await {
                            Ok(source_table) => {
                                for field in source_table.schema().fields() {
                                    rows.push((
                                        database.clone(),
                                        table.name().into(),
                                        "".to_string(),
                                        field.clone(),
                                    ))
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

                            rows.push((
                                database.clone(),
                                table.name().into(),
                                comment,
                                field.clone(),
                            ))
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
    let tenant = ctx.get_tenant();

    // For performance considerations, we do not require the most up-to-date table information here:
    // - for regular tables, the data is certainly fresh
    // - for read-only attached tables, the data may be outdated
    let catalog = catalog.clone().disable_table_info_refresh()?;

    let mut filtered_db_names: Option<Vec<String>> = None;
    let mut filtered_table_names: Option<Vec<String>> = None;

    if let Some(push_downs) = push_downs {
        if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
            let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
            let mut databases: Vec<String> = Vec::new();
            let mut tables: Vec<String> = Vec::new();

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
                Ok(())
            });
            if !databases.is_empty() {
                filtered_db_names = Some(databases);
            }
            if !tables.is_empty() {
                filtered_table_names = Some(tables);
            }
        }
    }

    let visibility_checker = if catalog.is_external() {
        None
    } else {
        Some(ctx.get_visibility_checker(false).await?)
    };

    let mut final_dbs: Vec<Arc<dyn Database>> = Vec::new();

    match (filtered_db_names, &visibility_checker) {
        (Some(db_names), Some(checker)) => {
            // Filtered databases + Visibility check
            for db_name in db_names {
                let db = catalog.get_database(&tenant, &db_name).await?;
                let db_id = db.get_db_info().database_id.db_id;
                if checker.check_database_visibility(CATALOG_DEFAULT, &db_name, db_id) {
                    final_dbs.push(db);
                }
            }
        }
        (Some(db_names), None) => {
            // Filtered databases + No visibility check
            for db_name in db_names {
                let db = catalog.get_database(&tenant, &db_name).await?;
                final_dbs.push(db);
            }
        }
        (None, Some(checker)) => {
            // All databases + Visibility check
            let catalog_dbs = checker.get_visibility_database();
            if let Some(catalog_dbs) = catalog_dbs {
                if let Some(dbs_in_default_catalog) =
                    catalog_dbs.get(&ctx.get_default_catalog()?.name())
                {
                    let db_idents = dbs_in_default_catalog
                        .iter()
                        .filter_map(|(db_name, _)| *db_name) // Get only names provided by checker
                        .map(|db_name| DatabaseNameIdent::new(&tenant, db_name))
                        .collect::<Vec<DatabaseNameIdent>>();

                    let databases = catalog.mget_databases(&tenant, &db_idents).await?;
                    // mget_databases returns Vec<Arc<dyn Database>>, checker already filtered by ID/Name
                    for db in databases {
                        // Double check visibility in case mget_databases returned something unexpected,
                        // although checker should be the source of truth here.
                        let db_id = db.get_db_info().database_id.db_id;
                        if checker.check_database_visibility(CATALOG_DEFAULT, db.name(), db_id) {
                            final_dbs.push(db);
                        } else {
                            // This case should ideally not happen if checker is correct, but good for safety
                            warn!("Visibility checker returned database {} but check_database_visibility failed.", db.name());
                        }
                    }
                }
            } else {
                // User has global privileges, check all
                let all_databases = catalog.list_databases(&tenant).await?;
                for db in all_databases {
                    let db_id = db.get_db_info().database_id.db_id;
                    let db_name = db.name();
                    if checker.check_database_visibility(CATALOG_DEFAULT, db_name, db_id) {
                        final_dbs.push(db);
                    }
                }
            }
        }
        (None, None) => {
            // All databases + No visibility check
            final_dbs = catalog.list_databases(&tenant).await?;
        }
    }

    let mut final_tables: Vec<(String, Vec<Arc<dyn Table>>)> = Vec::with_capacity(final_dbs.len());

    for db in final_dbs {
        let db_name = db.name().to_string();
        let db_id = db.get_db_info().database_id.db_id;

        let tables_in_db = match &filtered_table_names {
            Some(table_names) => {
                // Filtered tables
                let mut res = Vec::new();
                for table_name in table_names {
                    // Use get_table for specific names
                    if let Ok(table) = catalog.get_table(&tenant, &db_name, table_name).await {
                        res.push(table);
                    }
                }
                res
            }
            None => {
                // All tables in database
                // Use list_tables for all tables, handle error by returning empty vec
                catalog
                    .list_tables(&tenant, &db_name)
                    .await
                    .unwrap_or_default()
            }
        };

        let mut filtered_tables = Vec::with_capacity(tables_in_db.len());
        for table in tables_in_db {
            // Apply table visibility check if checker exists
            let is_visible = match &visibility_checker {
                Some(checker) => checker.check_table_visibility(
                    CATALOG_DEFAULT,
                    &db_name,
                    table.name(),
                    db_id,
                    table.get_id(),
                ),
                None => true, // No checker, all tables are visible
            };

            if is_visible {
                filtered_tables.push(table);
            }
        }
        final_tables.push((db_name, filtered_tables));
    }

    Ok(final_tables)
}
