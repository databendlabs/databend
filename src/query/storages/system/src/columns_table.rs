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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
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
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::Planner;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use databend_common_users::has_table_name_grants;
use databend_common_users::is_table_visible_with_owner;
use log::warn;

use crate::generate_default_catalog_meta;
use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::extract_leveled_strings;
use crate::util::should_use_table_optimized_path;

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
    let tenant = ctx.get_tenant();
    const OPTIMIZED_PATH_THRESHOLD: usize = 20;

    // For performance considerations, we do not require the most up-to-date table information here:
    // - for regular tables, the data is certainly fresh
    // - for read-only attached tables, the data may be outdated
    let catalog = catalog.clone().disable_table_info_refresh()?;

    let func_ctx = ctx.get_function_context()?;
    let (filtered_db_names, filtered_table_names) = extract_filters(&push_downs, &func_ctx)?;

    // Check if optimized path should be used
    let use_optimized_path = should_use_table_optimized_path(
        filtered_db_names.len(),
        filtered_table_names.len(),
        OPTIMIZED_PATH_THRESHOLD,
        false,
        catalog.is_external(),
    );

    // Try optimized path first
    if use_optimized_path {
        if let Some(result) = try_collect_tables_optimized(
            ctx,
            &catalog,
            &tenant,
            &filtered_db_names,
            &filtered_table_names,
        )
        .await?
        {
            return Ok(result);
        }
    }

    // Fall back to slow path with full visibility checker
    collect_tables_with_visibility_checker(ctx, &catalog, &tenant, filtered_db_names, filtered_table_names).await
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

/// Optimized path: lightweight permission check without loading all ownerships.
/// Returns Some(tables) if optimized path is applicable, None to fall back to slow path.
async fn try_collect_tables_optimized(
    ctx: &Arc<dyn TableContext>,
    catalog: &Arc<dyn Catalog>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    filtered_db_names: &[String],
    filtered_table_names: &[String],
) -> Result<Option<Vec<(String, Vec<Arc<dyn Table>>)>>> {
    let current_user = ctx.get_current_user()?;
    let effective_roles = ctx.get_all_effective_roles().await?;

    // Fall back if grants use table names (need exact match)
    if has_table_name_grants(&current_user, &effective_roles) {
        return Ok(None);
    }

    let user_api = UserApiProvider::instance();
    let mut final_tables: Vec<(String, Vec<Arc<dyn Table>>)> = Vec::new();

    for db_name in filtered_db_names {
        let db = match catalog.get_database(tenant, db_name).await {
            Ok(db) => db,
            Err(err) => {
                warn!("Failed to get database {}: {}", db_name, err);
                continue;
            }
        };

        let db_id = db.get_db_info().database_id.db_id;
        let mut visible_tables = Vec::new();

        for table_name in filtered_table_names {
            let table = match catalog.get_table(tenant, db_name, table_name).await {
                Ok(t) => t,
                Err(err) => {
                    warn!("Failed to get table {}.{}: {}", db_name, table_name, err);
                    continue;
                }
            };

            let table_id = table.get_id();
            let ownership = user_api
                .get_ownership(tenant, &OwnershipObject::Table {
                    catalog_name: catalog.name().to_string(),
                    db_id,
                    table_id,
                })
                .await?;

            let owner_role = ownership.map(|o| o.role.clone());
            let is_visible = is_table_visible_with_owner(
                &current_user,
                &effective_roles,
                owner_role.as_deref(),
                &catalog.name(),
                db_name,
                db_id,
                table_id,
            );

            if is_visible {
                visible_tables.push(table);
            }
        }

        if !visible_tables.is_empty() {
            final_tables.push((db_name.clone(), visible_tables));
        }
    }

    Ok(Some(final_tables))
}

/// Slow path: collect tables with full visibility checker.
async fn collect_tables_with_visibility_checker(
    ctx: &Arc<dyn TableContext>,
    catalog: &Arc<dyn Catalog>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    filtered_db_names: Vec<String>,
    filtered_table_names: Vec<String>,
) -> Result<Vec<(String, Vec<Arc<dyn Table>>)>> {
    let visibility_checker = if catalog.is_external() {
        None
    } else {
        Some(ctx.get_visibility_checker(false, Object::All).await?)
    };

    let final_dbs = collect_visible_databases(
        ctx,
        catalog,
        tenant,
        &filtered_db_names,
        &visibility_checker,
    )
    .await?;

    let filtered_table_names = if filtered_table_names.is_empty() {
        None
    } else {
        Some(filtered_table_names)
    };

    collect_visible_tables(catalog, tenant, final_dbs, filtered_table_names, &visibility_checker).await
}

/// Collect visible databases based on filters and visibility checker.
async fn collect_visible_databases(
    ctx: &Arc<dyn TableContext>,
    catalog: &Arc<dyn Catalog>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    filtered_db_names: &[String],
    visibility_checker: &Option<databend_common_users::GrantObjectVisibilityChecker>,
) -> Result<Vec<(String, u64)>> {
    match (filtered_db_names.is_empty(), visibility_checker) {
        (false, Some(checker)) => {
            // Filtered databases + Visibility check
            let mut result = Vec::new();
            for db_name in filtered_db_names {
                let db = catalog.get_database(tenant, db_name).await?;
                let db_id = db.get_db_info().database_id.db_id;
                if checker.check_database_visibility(CATALOG_DEFAULT, db_name, db_id) {
                    result.push((db_name.clone(), db_id));
                }
            }
            Ok(result)
        }
        (false, None) => {
            // Filtered databases + No visibility check
            let mut result = Vec::new();
            for db_name in filtered_db_names {
                let db = catalog.get_database(tenant, db_name).await?;
                result.push((db_name.clone(), db.get_db_info().database_id.db_id));
            }
            Ok(result)
        }
        (true, Some(checker)) => {
            // All databases + Visibility check
            collect_all_visible_databases(ctx, catalog, tenant, checker).await
        }
        (true, None) => {
            // All databases + No visibility check
            Ok(catalog
                .list_databases(tenant)
                .await?
                .iter()
                .map(|db| (db.name().to_string(), db.get_db_info().database_id.db_id))
                .collect())
        }
    }
}

async fn collect_all_visible_databases(
    ctx: &Arc<dyn TableContext>,
    catalog: &Arc<dyn Catalog>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    checker: &databend_common_users::GrantObjectVisibilityChecker,
) -> Result<Vec<(String, u64)>> {
    if let Some(catalog_dbs) = checker.get_visibility_database() {
        if let Some(dbs_in_default_catalog) =
            catalog_dbs.get(ctx.get_default_catalog()?.name().as_str())
        {
            let mut id_list: Vec<u64> = Vec::new();
            let mut name_set: HashSet<String> = HashSet::new();

            for (db_name_opt, db_id_opt) in dbs_in_default_catalog.iter() {
                if let Some(db_name) = db_name_opt {
                    name_set.insert(db_name.to_string());
                }
                if let Some(db_id) = db_id_opt {
                    id_list.push(**db_id);
                }
            }

            let db_names = catalog.mget_database_names_by_ids(tenant, &id_list).await?;
            db_names.into_iter().flatten().for_each(|name| {
                name_set.insert(name);
            });

            let db_idents: Vec<DatabaseNameIdent> = name_set
                .iter()
                .map(|name| DatabaseNameIdent::new(tenant, name))
                .collect();

            let databases = catalog.mget_databases(tenant, &db_idents).await?;

            return Ok(databases
                .into_iter()
                .filter_map(|db| {
                    let db_id = db.get_db_info().database_id.db_id;
                    if checker.check_database_visibility(CATALOG_DEFAULT, db.name(), db_id) {
                        Some((db.name().to_string(), db_id))
                    } else {
                        warn!(
                            "Visibility checker returned database {} but check_database_visibility failed.",
                            db.name()
                        );
                        None
                    }
                })
                .collect());
        }
    }

    // User has global privileges, check all
    let all_databases = catalog.list_databases(tenant).await?;
    Ok(all_databases
        .into_iter()
        .filter_map(|db| {
            let db_id = db.get_db_info().database_id.db_id;
            if checker.check_database_visibility(CATALOG_DEFAULT, db.name(), db_id) {
                Some((db.name().to_string(), db_id))
            } else {
                None
            }
        })
        .collect())
}

async fn collect_visible_tables(
    catalog: &Arc<dyn Catalog>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    final_dbs: Vec<(String, u64)>,
    filtered_table_names: Option<Vec<String>>,
    visibility_checker: &Option<databend_common_users::GrantObjectVisibilityChecker>,
) -> Result<Vec<(String, Vec<Arc<dyn Table>>)>> {
    let mut final_tables: Vec<(String, Vec<Arc<dyn Table>>)> = Vec::with_capacity(final_dbs.len());

    for (db_name, db_id) in final_dbs {
        let tables_in_db = match &filtered_table_names {
            Some(table_names) => {
                let mut res = Vec::new();
                for table_name in table_names {
                    if let Ok(table) = catalog.get_table(tenant, &db_name, table_name).await {
                        res.push(table);
                    }
                }
                res
            }
            None => catalog.list_tables(tenant, &db_name).await.unwrap_or_default(),
        };

        let filtered_tables: Vec<_> = tables_in_db
            .into_iter()
            .filter(|table| {
                visibility_checker
                    .as_ref()
                    .map(|c| c.check_table_visibility(CATALOG_DEFAULT, &db_name, table.name(), db_id, table.get_id()))
                    .unwrap_or(true)
            })
            .collect();

        final_tables.push((db_name, filtered_tables));
    }

    Ok(final_tables)
}
