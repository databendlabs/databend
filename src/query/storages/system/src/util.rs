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
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::expr::*;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::type_check::check_string;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::IcebergCatalogOption;
use databend_common_meta_app::schema::IcebergRestCatalogOption;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use databend_common_users::is_role_owner;
use log::warn;

pub fn generate_catalog_meta(ctl_name: &str) -> CatalogMeta {
    if ctl_name.to_lowercase() == CATALOG_DEFAULT {
        CatalogMeta {
            catalog_option: CatalogOption::Default,
            created_on: Default::default(),
        }
    } else {
        CatalogMeta {
            catalog_option: CatalogOption::Iceberg(IcebergCatalogOption::Rest(
                IcebergRestCatalogOption {
                    uri: "".to_string(),
                    warehouse: "".to_string(),
                    props: Default::default(),
                },
            )),
            created_on: Default::default(),
        }
    }
}

pub fn find_gt_filter(expr: &Expr<String>, visitor: &mut impl FnMut(&str, &Scalar)) {
    match expr {
        Expr::Constant(_) | Expr::ColumnRef(_) => {}
        Expr::Cast(Cast { expr, .. }) => find_gt_filter(expr, visitor),
        Expr::FunctionCall(FunctionCall { function, args, .. }) => {
            if function.signature.name == "gt" || function.signature.name == "gte" {
                match args.as_slice() {
                    [
                        Expr::ColumnRef(ColumnRef { id, .. }),
                        Expr::Constant(Constant { scalar, .. }),
                    ]
                    | [
                        Expr::Constant(Constant { scalar, .. }),
                        Expr::ColumnRef(ColumnRef { id, .. }),
                    ] => {
                        visitor(id, scalar);
                    }
                    _ => {}
                }
            } else if function.signature.name == "and_filters" {
                // only support this:
                // 1. where xx and xx and xx
                // 2. filter: Column `table`, Column `database`
                for arg in args {
                    find_gt_filter(arg, visitor)
                }
            }
        }
        Expr::LambdaFunctionCall(LambdaFunctionCall { args, .. }) => {
            for arg in args {
                find_gt_filter(arg, visitor)
            }
        }
    }
}

pub fn find_lt_filter(expr: &Expr<String>, visitor: &mut impl FnMut(&str, &Scalar)) {
    match expr {
        Expr::Constant(_) | Expr::ColumnRef(_) => {}
        Expr::Cast(Cast { expr, .. }) => find_lt_filter(expr, visitor),
        Expr::FunctionCall(FunctionCall { function, args, .. }) => {
            if function.signature.name == "lt" || function.signature.name == "lte" {
                match args.as_slice() {
                    [
                        Expr::ColumnRef(ColumnRef { id, .. }),
                        Expr::Constant(Constant { scalar, .. }),
                    ]
                    | [
                        Expr::Constant(Constant { scalar, .. }),
                        Expr::ColumnRef(ColumnRef { id, .. }),
                    ] => {
                        visitor(id, scalar);
                    }
                    _ => {}
                }
            } else if function.signature.name == "and_filters" {
                // only support this:
                // 1. where xx and xx and xx
                // 2. filter: Column `table`, Column `database`
                for arg in args {
                    find_lt_filter(arg, visitor)
                }
            }
        }
        Expr::LambdaFunctionCall(LambdaFunctionCall { args, .. }) => {
            for arg in args {
                find_lt_filter(arg, visitor)
            }
        }
    }
}

pub fn extract_leveled_strings(
    expr: &Expr<String>,
    level_names: &[&str],
    func_ctx: &FunctionContext,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut res1 = vec![];
    let mut res2 = vec![];
    let leveld_results =
        FilterHelpers::find_leveled_eq_filters(expr, level_names, func_ctx, &BUILTIN_FUNCTIONS)?;

    for (i, scalars) in leveld_results.iter().enumerate() {
        for r in scalars.iter() {
            let e = Expr::Constant(Constant {
                span: None,
                scalar: r.clone(),
                data_type: r.as_ref().infer_data_type(),
            });

            if let Ok(s) = check_string::<usize>(None, func_ctx, &e, &BUILTIN_FUNCTIONS) {
                match i {
                    0 => res1.push(s),
                    1 => res2.push(s),
                    _ => unreachable!(),
                }
            }
        }
    }
    Ok((res1, res2))
}

// ============================================================================
// Unified Table Visibility Collection
// ============================================================================

/// Database info with its tables for visibility checking.
pub struct DatabaseWithTables {
    pub name: String,
    pub id: u64,
    pub tables: Vec<Arc<dyn Table>>,
}

/// Maximum number of db * table combinations for using optimized path.
/// This prevents excessive ownership checks for very large filters.
/// Example: 4 dbs * 5 tables = 20 (OK), 5 dbs * 5 tables = 25 (too many)
pub const MAX_OPTIMIZED_PATH_CHECKS: usize = 20;

/// Unified visibility checker strategy.
enum VisibilityStrategy {
    /// No permission check (external catalogs)
    NoCheck,
    /// Preloaded ownership checker (for list all queries)
    Preloaded(GrantObjectVisibilityChecker),
    /// Optimized path (for filtered queries)
    Optimized {
        grants_checker: GrantObjectVisibilityChecker,
        effective_roles: Vec<databend_common_meta_app::principal::RoleInfo>,
        catalog_name: String,
        tenant: Tenant,
    },
}

/// Collects visible tables with unified permission checking logic.
///
/// Decision logic:
/// - External catalog → no permission check
/// - Has precise filters (db + table) → lazy ownership loading
/// - Otherwise → preload all ownerships
///
/// # Arguments
/// * `ctx` - Table context for getting user/roles info
/// * `catalog` - The catalog to query
/// * `filtered_db_names` - Database names from filter (empty = list all)
/// * `filtered_table_names` - Table names from filter (empty = list all)
///
/// # Returns
/// Vector of (db_name, db_id, visible_tables)
pub async fn collect_visible_tables(
    ctx: &Arc<dyn TableContext>,
    catalog: &Arc<dyn Catalog>,
    filtered_db_names: &[String],
    filtered_table_names: &[String],
) -> Result<Vec<DatabaseWithTables>> {
    let tenant = ctx.get_tenant();

    // Determine visibility strategy
    let strategy = if catalog.is_external() {
        VisibilityStrategy::NoCheck
    } else if !filtered_db_names.is_empty()
        && !filtered_table_names.is_empty()
        && filtered_db_names.len() * filtered_table_names.len() <= MAX_OPTIMIZED_PATH_CHECKS
    {
        // Precise filters within reasonable size: use optimized path to avoid loading all ownerships
        let grants_checker = ctx.get_visibility_checker(true, Object::All).await?;
        let effective_roles = ctx.get_all_effective_roles().await?;
        VisibilityStrategy::Optimized {
            grants_checker,
            effective_roles,
            catalog_name: catalog.name().to_string(),
            tenant: tenant.clone(),
        }
    } else {
        // No filters, partial filters, or too many combinations: preload all ownerships
        let visibility_checker = ctx.get_visibility_checker(false, Object::All).await?;
        VisibilityStrategy::Preloaded(visibility_checker)
    };

    // Get databases and tables
    let databases = get_databases(catalog, &tenant, filtered_db_names).await?;

    let mut result = Vec::with_capacity(databases.len());
    for db in databases {
        let db_name = db.name().to_string();
        let db_id = db.get_db_info().database_id.db_id;

        // Check database visibility
        if !is_database_visible(&strategy, &db_name, db_id) {
            continue;
        }

        // Get tables
        let tables = match get_tables(catalog, &tenant, &db_name, filtered_table_names).await {
            Ok(tables) => tables,
            Err(err) => {
                warn!("Failed to get tables in database {}: {}", db_name, err);
                continue;
            }
        };

        // Filter visible tables
        let visible_tables = filter_visible_tables(&strategy, &db_name, db_id, tables).await?;

        if !visible_tables.is_empty() {
            result.push(DatabaseWithTables {
                name: db_name,
                id: db_id,
                tables: visible_tables,
            });
        }
    }

    Ok(result)
}

/// Check if database is visible based on strategy.
fn is_database_visible(strategy: &VisibilityStrategy, db_name: &str, db_id: u64) -> bool {
    match strategy {
        VisibilityStrategy::NoCheck => true,
        VisibilityStrategy::Preloaded(checker) => {
            checker.check_database_visibility(CATALOG_DEFAULT, db_name, db_id)
        }
        VisibilityStrategy::Optimized { .. } => {
            // For optimized path, we CANNOT filter databases early because:
            // 1. User might only have table-level ownership (no db grants)
            // 2. User might own the database (db ownership, no grants)
            // We must check at table level with full ownership info.
            true
        }
    }
}

/// Filter visible tables based on strategy.
async fn filter_visible_tables(
    strategy: &VisibilityStrategy,
    db_name: &str,
    db_id: u64,
    tables: Vec<Arc<dyn Table>>,
) -> Result<Vec<Arc<dyn Table>>> {
    match strategy {
        VisibilityStrategy::NoCheck => Ok(tables),

        VisibilityStrategy::Preloaded(checker) => Ok(tables
            .into_iter()
            .filter(|table| {
                checker.check_table_visibility(
                    CATALOG_DEFAULT,
                    db_name,
                    table.name(),
                    db_id,
                    table.get_id(),
                )
            })
            .collect()),

        VisibilityStrategy::Optimized {
            grants_checker,
            effective_roles,
            catalog_name,
            tenant,
        } => {
            filter_tables_with_optimized_path(
                grants_checker,
                effective_roles,
                catalog_name,
                tenant,
                db_name,
                db_id,
                tables,
            )
            .await
        }
    }
}

/// Filter tables with optimized path.
/// This only loads ownerships for tables that need it.
///
/// Logic:
/// 1. Check database ownership - if user owns db, all tables visible
/// 2. Otherwise, check each table's grants first, then ownership if needed
///    (defensive against edge cases where db-level visibility is absent)
async fn filter_tables_with_optimized_path(
    grants_checker: &GrantObjectVisibilityChecker,
    effective_roles: &[databend_common_meta_app::principal::RoleInfo],
    catalog_name: &str,
    tenant: &Tenant,
    db_name: &str,
    db_id: u64,
    tables: Vec<Arc<dyn Table>>,
) -> Result<Vec<Arc<dyn Table>>> {
    let user_api = UserApiProvider::instance();

    // Check database ownership first: db owner can see all tables regardless of grants.
    let db_ownership = user_api
        .get_ownership(tenant, &OwnershipObject::Database {
            catalog_name: catalog_name.to_string(),
            db_id,
        })
        .await?;
    if let Some(db_owner_info) = db_ownership {
        if is_role_owner(Some(&db_owner_info.role), effective_roles) {
            return Ok(tables);
        }
    }

    // Check table grants first, then ownership (defensive).
    filter_tables_with_grants_and_ownership(
        user_api.as_ref(),
        grants_checker,
        effective_roles,
        catalog_name,
        tenant,
        db_name,
        db_id,
        tables,
    )
    .await
}

async fn filter_tables_with_grants_and_ownership(
    user_api: &UserApiProvider,
    grants_checker: &GrantObjectVisibilityChecker,
    effective_roles: &[databend_common_meta_app::principal::RoleInfo],
    catalog_name: &str,
    tenant: &Tenant,
    db_name: &str,
    db_id: u64,
    tables: Vec<Arc<dyn Table>>,
) -> Result<Vec<Arc<dyn Table>>> {
    let mut visible = Vec::new();
    let mut need_ownership_check = Vec::new();

    for table in tables {
        if grants_checker.check_table_visibility(
            CATALOG_DEFAULT,
            db_name,
            table.name(),
            db_id,
            table.get_id(),
        ) {
            visible.push(table);
        } else {
            need_ownership_check.push(table);
        }
    }

    if !need_ownership_check.is_empty() {
        let ownership_objects: Vec<OwnershipObject> = need_ownership_check
            .iter()
            .map(|t| OwnershipObject::Table {
                catalog_name: catalog_name.to_string(),
                db_id,
                table_id: t.get_id(),
            })
            .collect();

        let ownerships = user_api.mget_ownerships(tenant, &ownership_objects).await?;

        for (table, ownership) in need_ownership_check.into_iter().zip(ownerships) {
            if let Some(owner_info) = ownership {
                if is_role_owner(Some(&owner_info.role), effective_roles) {
                    visible.push(table);
                }
            }
        }
    }

    Ok(visible)
}

/// Get databases: use mget if filtered, list if not.
async fn get_databases(
    catalog: &Arc<dyn Catalog>,
    tenant: &Tenant,
    filtered_db_names: &[String],
) -> Result<Vec<Arc<dyn Database>>> {
    if filtered_db_names.is_empty() {
        catalog.list_databases(tenant).await
    } else {
        let db_idents: Vec<DatabaseNameIdent> = filtered_db_names
            .iter()
            .map(|name| DatabaseNameIdent::new(tenant, name))
            .collect();
        catalog.mget_databases(tenant, &db_idents).await
    }
}

/// Get tables: use mget if filtered, list if not.
async fn get_tables(
    catalog: &Arc<dyn Catalog>,
    tenant: &Tenant,
    db_name: &str,
    filtered_table_names: &[String],
) -> Result<Vec<Arc<dyn Table>>> {
    if filtered_table_names.is_empty() {
        catalog.list_tables(tenant, db_name).await
    } else {
        catalog
            .mget_tables(tenant, db_name, filtered_table_names)
            .await
    }
}

/// Disable table info refresh for catalog (for performance).
pub fn disable_catalog_refresh(catalog: Arc<dyn Catalog>) -> Result<Arc<dyn Catalog>> {
    catalog.disable_table_info_refresh()
}
