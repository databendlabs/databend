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
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::expr::*;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::type_check::check_string;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::Object;
use databend_common_users::TableVisibilityTarget;
use databend_common_users::filter_db_tables_by_visibility;
use log::trace;
use log::warn;

pub fn generate_default_catalog_meta() -> CatalogMeta {
    CatalogMeta {
        catalog_option: CatalogOption::Default,
        created_on: Default::default(),
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

/// Maximum number of databases for db-only filter optimized path.
/// When only database filter is specified (no table filter), each db requires
/// listing all its tables + per-db ownership check. Keep this small.
pub const MAX_OPTIMIZED_PATH_DB_ONLY: usize = 5;

/// Returns whether the filtered query should use the optimized visibility path.
///
/// Optimized path is only used when the ownership check count is bounded:
/// - db only (no table filter): db_count <= MAX_OPTIMIZED_PATH_DB_ONLY
/// - tables only (no db filter): table_count <= MAX_OPTIMIZED_PATH_CHECKS
/// - tables + dbs: db_count * table_count <= MAX_OPTIMIZED_PATH_CHECKS
///
/// Note: the two branches are intentionally split so the tables-only case never
/// falls through to `0 * table_count`, which would always be true.
#[inline]
pub fn should_use_optimized_visibility_path(db_count: usize, table_count: usize) -> bool {
    if table_count == 0 {
        // db-only filter: e.g. WHERE database = 'db1'
        // Each db needs listing all tables + per-db ownership check,
        // so limit to a small number of dbs.
        return db_count > 0 && db_count <= MAX_OPTIMIZED_PATH_DB_ONLY;
    }

    if db_count == 0 {
        table_count <= MAX_OPTIMIZED_PATH_CHECKS
    } else {
        db_count.saturating_mul(table_count) <= MAX_OPTIMIZED_PATH_CHECKS
    }
}

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
    } else if should_use_optimized_visibility_path(
        filtered_db_names.len(),
        filtered_table_names.len(),
    ) {
        // Precise filters within reasonable size: use optimized path to avoid loading all ownerships
        let grants_checker = ctx.get_visibility_checker(true, Object::All).await?;
        if grants_checker.has_global_db_table_privilege() {
            VisibilityStrategy::NoCheck
        } else {
            let effective_roles = ctx.get_all_effective_roles().await?;
            VisibilityStrategy::Optimized {
                grants_checker,
                effective_roles,
                catalog_name: catalog.name().to_string(),
                tenant: tenant.clone(),
            }
        }
    } else {
        // No filters, partial filters, or too many combinations: preload all ownerships
        let visibility_checker = ctx.get_visibility_checker(false, Object::All).await?;
        VisibilityStrategy::Preloaded(visibility_checker)
    };

    let catalog_name = catalog.name();

    // Get databases and tables
    let t = std::time::Instant::now();
    let databases = get_databases(catalog, &tenant, filtered_db_names).await?;
    trace!(
        "collect_visible_tables: get_databases({}) took {:?}",
        databases.len(),
        t.elapsed()
    );

    let mut result = Vec::with_capacity(databases.len());
    for db in databases {
        let db_name = db.name().to_string();
        let db_id = db.get_db_info().database_id.db_id;

        if !is_database_visible(&strategy, &catalog_name, &db_name, db_id) {
            continue;
        }

        let t = std::time::Instant::now();
        let tables = match get_tables(catalog, &tenant, &db_name, filtered_table_names).await {
            Ok(tables) => tables,
            Err(err) => {
                warn!("Failed to get tables in database {}: {}", db_name, err);
                continue;
            }
        };
        trace!(
            "collect_visible_tables: get_tables('{}', {}) took {:?}",
            db_name,
            tables.len(),
            t.elapsed()
        );

        let visible_tables =
            filter_visible_tables(&strategy, &catalog_name, &db_name, db_id, tables).await?;

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
fn is_database_visible(
    strategy: &VisibilityStrategy,
    catalog_name: &str,
    db_name: &str,
    db_id: u64,
) -> bool {
    match strategy {
        VisibilityStrategy::NoCheck => true,
        VisibilityStrategy::Preloaded(checker) => {
            checker.check_database_visibility(catalog_name, db_name, db_id)
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
    catalog_name: &str,
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
                    catalog_name,
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

/// Filter tables with optimized path using the public lazy visibility helper.
/// This delegates to `filter_db_tables_by_visibility` in `databend-common-users`.
async fn filter_tables_with_optimized_path(
    grants_checker: &GrantObjectVisibilityChecker,
    effective_roles: &[databend_common_meta_app::principal::RoleInfo],
    catalog_name: &str,
    tenant: &Tenant,
    db_name: &str,
    db_id: u64,
    tables: Vec<Arc<dyn Table>>,
) -> Result<Vec<Arc<dyn Table>>> {
    let targets: Vec<TableVisibilityTarget> = tables
        .iter()
        .map(|t| TableVisibilityTarget {
            table_name: t.name(),
            table_id: t.get_id(),
        })
        .collect();

    let result = filter_db_tables_by_visibility(
        grants_checker,
        effective_roles,
        tenant,
        catalog_name,
        db_name,
        db_id,
        &targets,
    )
    .await?;

    let visible = result
        .visible_table_indexes
        .into_iter()
        .map(|i| tables[i].clone())
        .collect();

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
