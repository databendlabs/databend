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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::type_check::check_number;
use databend_common_expression::type_check::check_string;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::utils::FromData;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::CatalogType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_basic::NullTable;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_fuse::FuseTable;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use databend_common_users::check_table_visibility_with_roles;
use databend_common_users::has_table_name_grants;
use databend_common_users::is_role_owner;
use databend_storages_common_table_meta::table::is_internal_opt_key;
use log::warn;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::extract_leveled_strings;
use crate::util::generate_catalog_meta;

pub struct TablesTable<const WITH_HISTORY: bool, const WITHOUT_VIEW: bool> {
    table_info: TableInfo,
}

pub type TablesTableWithHistory = TablesTable<true, true>;
pub type TablesTableWithoutHistory = TablesTable<false, true>;
pub type ViewsTableWithHistory = TablesTable<true, false>;
pub type ViewsTableWithoutHistory = TablesTable<false, false>;

#[async_trait::async_trait]
pub trait HistoryAware {
    const TABLE_NAME: &'static str;
    async fn list_tables(
        catalog: &Arc<dyn Catalog>,
        tenant: &Tenant,
        database_name: &str,
        with_history: bool,
        without_view: bool,
        mock_table: bool,
    ) -> Result<Vec<Arc<dyn Table>>>;
}

macro_rules! impl_history_aware {
    ($with_history:expr, $without_view:expr, $table_name:expr) => {
        #[async_trait::async_trait]
        impl HistoryAware for TablesTable<$with_history, $without_view> {
            const TABLE_NAME: &'static str = $table_name;

            #[async_backtrace::framed]
            async fn list_tables(
                catalog: &Arc<dyn Catalog>,
                tenant: &Tenant,
                database_name: &str,
                with_history: bool,
                _without_view: bool,
                mock_table: bool,
            ) -> Result<Vec<Arc<dyn Table>>> {
                if !mock_table {
                    if with_history {
                        catalog.list_tables_history(tenant, database_name).await
                    } else {
                        catalog.list_tables(tenant, database_name).await
                    }
                } else {
                    let mut res = vec![];
                    let names = catalog.list_tables_names(tenant, database_name).await?;
                    let engine = match catalog.info().catalog_type() {
                        CatalogType::Default => {
                            return Err(ErrorCode::Internal(
                                "Catalog type is Default, can not mock table".to_string(),
                            ));
                        }
                        CatalogType::Iceberg => "Iceberg".to_string(),
                        CatalogType::Hive => "Hive".to_string(),
                    };

                    for name in names {
                        let t = TableInfo {
                            ident: TableIdent::new(0, 0),
                            desc: format!("'{}'.'{}'", database_name, name),
                            name: name.to_string(),
                            meta: TableMeta {
                                engine: engine.to_string(),
                                ..Default::default()
                            },
                            ..Default::default()
                        };
                        let table = NullTable::try_create(t)?;
                        res.push(Arc::from(table));
                    }
                    Ok(res)
                }
            }
        }
    };
}

impl_history_aware!(true, true, "tables_with_history");
impl_history_aware!(false, true, "tables");
impl_history_aware!(true, false, "views_with_history");
impl_history_aware!(false, false, "views");

#[async_trait::async_trait]
impl<const WITH_HISTORY: bool, const WITHOUT_VIEW: bool> AsyncSystemTable
    for TablesTable<WITH_HISTORY, WITHOUT_VIEW>
where TablesTable<WITH_HISTORY, WITHOUT_VIEW>: HistoryAware
{
    const NAME: &'static str = Self::TABLE_NAME;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let catalog_mgr = CatalogManager::instance();
        let catalog = catalog_mgr
            .get_catalog(
                tenant.tenant_name(),
                self.get_table_info().catalog(),
                ctx.session_state()?,
            )
            .await?
            .disable_table_info_refresh()?;

        // Optimization target:  Fast path for known iceberg catalog SHOW TABLES
        let func_ctx = ctx.get_function_context()?;
        if let Some((catalog_name, db_name)) =
            self.is_external_show_tables_query(&func_ctx, &push_downs, &catalog)
        {
            self.show_tables_from_external_catalog(ctx, catalog_name, db_name)
                .await
        } else {
            self.get_full_data_from_catalogs(ctx, push_downs, catalog)
                .await
        }
    }
}

impl<const WITH_HISTORY: bool, const WITHOUT_VIEW: bool> TablesTable<WITH_HISTORY, WITHOUT_VIEW>
where TablesTable<WITH_HISTORY, WITHOUT_VIEW>: HistoryAware
{
    pub fn schema() -> TableSchemaRef {
        if WITHOUT_VIEW {
            TableSchemaRefExt::create(vec![
                TableField::new("catalog", TableDataType::String),
                TableField::new("database", TableDataType::String),
                TableField::new("database_id", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new("name", TableDataType::String),
                TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new(
                    "total_columns",
                    TableDataType::Number(NumberDataType::UInt64),
                ),
                TableField::new("engine", TableDataType::String),
                TableField::new("engine_full", TableDataType::String),
                TableField::new("cluster_by", TableDataType::String),
                TableField::new("is_transient", TableDataType::String),
                TableField::new("is_attach", TableDataType::String),
                TableField::new("created_on", TableDataType::Timestamp),
                TableField::new(
                    "dropped_on",
                    TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
                ),
                TableField::new("updated_on", TableDataType::Timestamp),
                TableField::new(
                    "num_rows",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "data_size",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "data_compressed_size",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "index_size",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "bloom_index_size",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "ngram_index_size",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "inverted_index_size",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "vector_index_size",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "virtual_column_size",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "number_of_segments",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "number_of_blocks",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "owner",
                    TableDataType::Nullable(Box::new(TableDataType::String)),
                ),
                TableField::new("comment", TableDataType::String),
                TableField::new("table_type", TableDataType::String),
                TableField::new("is_external", TableDataType::Boolean),
                TableField::new("table_option", TableDataType::String),
                TableField::new("storage_param", TableDataType::String),
            ])
        } else {
            TableSchemaRefExt::create(vec![
                TableField::new("catalog", TableDataType::String),
                TableField::new("database", TableDataType::String),
                TableField::new("database_id", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new("name", TableDataType::String),
                TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new("engine", TableDataType::String),
                TableField::new("engine_full", TableDataType::String),
                TableField::new("created_on", TableDataType::Timestamp),
                TableField::new(
                    "dropped_on",
                    TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
                ),
                TableField::new("updated_on", TableDataType::Timestamp),
                TableField::new(
                    "owner",
                    TableDataType::Nullable(Box::new(TableDataType::String)),
                ),
                TableField::new("comment", TableDataType::String),
                TableField::new("view_query", TableDataType::String),
            ])
        }
    }

    /// Optimized path: lightweight permission check without loading all ownerships.
    /// Returns Some(...) with collected data if successful, None to fall back to slow path.
    #[allow(clippy::too_many_arguments)]
    async fn try_optimized_path(
        ctx: &Arc<dyn TableContext>,
        tenant: &Tenant,
        user_api: &Arc<UserApiProvider>,
        ctls: &[(String, Arc<dyn Catalog>)],
        db_names: &[String],
        tables_ids: &[u64],
        tables_names: &mut BTreeSet<String>,
    ) -> Result<
        Option<(
            Vec<String>,                // catalogs
            Vec<String>,                // databases
            Vec<u64>,                   // databases_ids
            Vec<Arc<dyn Table>>,        // database_tables
            Vec<Option<String>>,        // owners
        )>,
    > {
        let current_user = ctx.get_current_user()?;
        let effective_roles = ctx.get_all_effective_roles().await?;

        // Fall back if grants use table names (need exact match)
        if has_table_name_grants(&current_user, &effective_roles) {
            return Ok(None);
        }

        let mut catalogs = vec![];
        let mut databases = vec![];
        let mut databases_ids = vec![];
        let mut database_tables = vec![];
        let mut owners: Vec<Option<String>> = vec![];

        for (ctl_name, ctl) in ctls.iter() {
            // Collect databases
            let mut dbs = Vec::new();
            for db in db_names {
                match ctl.get_database(tenant, db.as_str()).await {
                    Ok(database) => dbs.push(database),
                    Err(err) => {
                        warn!("Failed to get database: {}, {}", db, err);
                    }
                }
            }

            // Resolve table IDs to names
            match ctl.mget_table_names_by_ids(tenant, tables_ids, false).await {
                Ok(new_tables) => {
                    let new_table_names: BTreeSet<_> = new_tables.into_iter().flatten().collect();
                    tables_names.extend(new_table_names);
                }
                Err(err) => {
                    warn!("Failed to get tables: {}, {}", ctl.name(), err);
                }
            }

            // Check visibility for each table
            for table_name in tables_names.iter() {
                for db in &dbs {
                    let table = match ctl.get_table(tenant, db.name(), table_name).await {
                        Ok(t) => t,
                        Err(err) => {
                            warn!(
                                "Failed to get table in database: {}.{}, {}",
                                ctl_name,
                                db.name(),
                                err
                            );
                            continue;
                        }
                    };

                    let db_id = db.get_db_info().database_id.db_id;
                    let table_id = table.get_id();

                    let ownership = user_api
                        .role_api(tenant)
                        .get_ownership(&OwnershipObject::Table {
                            catalog_name: ctl_name.to_string(),
                            db_id,
                            table_id,
                        })
                        .await?;

                    let owner_role = ownership.map(|o| o.role);
                    let is_owner = is_role_owner(owner_role.as_deref(), &effective_roles);
                    let is_visible = is_owner
                        || check_table_visibility_with_roles(
                            &current_user,
                            &effective_roles,
                            ctl_name,
                            db.name(),
                            db_id,
                            table_id,
                        );

                    if is_visible {
                        push_table_info(
                            &mut catalogs,
                            &mut databases,
                            &mut databases_ids,
                            &mut database_tables,
                            &mut owners,
                            ctl_name,
                            db.name(),
                            db_id,
                            table,
                            owner_role,
                        );
                    }
                }
            }
        }

        if catalogs.is_empty() {
            // No results from optimized path, fall back to slow path
            Ok(None)
        } else {
            Ok(Some((
                catalogs,
                databases,
                databases_ids,
                database_tables,
                owners,
            )))
        }
    }

    /// dump all the tables from all the catalogs with pushdown, this is used for `SHOW TABLES` command.
    /// please note that this function is intended to not wrapped with Result<>, because we do not want to
    /// break ALL the output on reading ANY of the catalog, database or table failed.
    #[async_backtrace::framed]
    async fn get_full_data_from_catalogs(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        catalog_impl: Arc<dyn Catalog>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();

        // Threshold for using optimized path without visibility_checker
        const OPTIMIZED_PATH_THRESHOLD: usize = 20;

        let mut catalogs = vec![];
        let mut databases = vec![];
        let mut databases_ids = vec![];

        let mut database_tables = vec![];
        let mut owners: Vec<Option<String>> = Vec::new();
        let user_api = UserApiProvider::instance();

        let mut dbs = Vec::new();
        let mut tables_names: BTreeSet<String> = BTreeSet::new();
        let mut tables_ids: Vec<u64> = Vec::new();
        let mut db_name: Vec<String> = Vec::new();
        let mut catalog_name: Vec<String> = Vec::new();

        let mut get_stats = true;
        let mut get_ownership = true;
        let mut owner_field_indexes: HashSet<usize> = HashSet::new();
        let mut stats_fields_indexes: HashSet<usize> = HashSet::new();
        let schema = TablesTable::<WITH_HISTORY, WITHOUT_VIEW>::schema();

        let mut name_field_index: usize = 0;
        let mut only_get_name = false;
        for (i, name) in schema.fields.iter().enumerate() {
            match name.name().as_str() {
                "num_rows"
                | "data_size"
                | "data_compressed_size"
                | "index_size"
                | "bloom_index_size"
                | "ngram_index_size"
                | "inverted_index_size"
                | "vector_index_size"
                | "virtual_column_size"
                | "number_of_segments"
                | "number_of_blocks" => {
                    stats_fields_indexes.insert(i);
                }
                "owner" => {
                    owner_field_indexes.insert(i);
                }
                "name" => {
                    name_field_index = i;
                }
                _ => {}
            }
        }

        let func_ctx = ctx.get_function_context()?;
        if let Some(push_downs) = &push_downs {
            if let Some(Projection::Columns(v)) = push_downs.projection.as_ref() {
                if v.len() == 1 && v[0] == name_field_index {
                    only_get_name = true;
                }
                get_stats = v
                    .iter()
                    .any(|field_index| stats_fields_indexes.contains(field_index));
                get_ownership = v
                    .iter()
                    .any(|field_index| owner_field_indexes.contains(field_index));
            }

            if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
                let expr = filter.as_expr(&BUILTIN_FUNCTIONS);

                let leveld_results = FilterHelpers::find_leveled_eq_filters(
                    &expr,
                    &["catalog", "database", "name", "table_id"],
                    &func_ctx,
                    &BUILTIN_FUNCTIONS,
                )?;

                for (i, scalars) in leveld_results.iter().enumerate() {
                    if i == 3 {
                        for r in scalars.iter() {
                            let e = Expr::Constant(Constant {
                                span: None,
                                scalar: r.clone(),
                                data_type: r.as_ref().infer_data_type(),
                            });

                            if let Ok(s) =
                                check_number::<u64, usize>(None, &func_ctx, &e, &BUILTIN_FUNCTIONS)
                            {
                                tables_ids.push(s);
                            }
                        }
                    } else {
                        for r in scalars.iter() {
                            let e = Expr::Constant(Constant {
                                span: None,
                                scalar: r.clone(),
                                data_type: r.as_ref().infer_data_type(),
                            });

                            if let Ok(s) =
                                check_string::<usize>(None, &func_ctx, &e, &BUILTIN_FUNCTIONS)
                            {
                                match i {
                                    0 => catalog_name.push(s),
                                    1 => db_name.push(s),
                                    2 => {
                                        let _ = tables_names.insert(s);
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        }
                    }
                }
            }
        }

        let ctl_name = catalog_impl.name();
        let is_external_catalog = catalog_impl.is_external();

        let ctls = if !catalog_name.is_empty() {
            let mut res = vec![];
            for name in &catalog_name {
                if *name == ctl_name {
                    let ctl = ctx.get_catalog(name).await?;
                    res.push((name.to_string(), ctl));
                }
            }
            // If empty return empty result
            res
        } else {
            vec![(ctl_name, catalog_impl)]
        };

        // Optimized path: when filter specifies limited databases and tables,
        // use lightweight permission check without loading all ownerships
        let filter_count = db_name.len() * (tables_names.len() + tables_ids.len()).max(1);
        let use_optimized_path = !db_name.is_empty()
            && (tables_names.len() + tables_ids.len()) > 0
            && filter_count <= OPTIMIZED_PATH_THRESHOLD
            && !WITH_HISTORY
            && !is_external_catalog;

        if use_optimized_path {
            if let Some((opt_catalogs, opt_databases, opt_databases_ids, opt_tables, opt_owners)) =
                Self::try_optimized_path(
                    &ctx,
                    &tenant,
                    &user_api,
                    &ctls,
                    &db_name,
                    &tables_ids,
                    &mut tables_names,
                )
                .await?
            {
                catalogs = opt_catalogs;
                databases = opt_databases;
                databases_ids = opt_databases_ids;
                database_tables = opt_tables;
                owners = opt_owners;
            } else {
                // Fall through to slow path
            }
        }

        if catalogs.is_empty() {
            // Slow path: need full visibility checker
            let visibility_checker = if is_external_catalog {
                None
            } else {
                Some(ctx.get_visibility_checker(false, Object::All).await?)
            };

            let catalog_dbs = visibility_checker
                .as_ref()
                .and_then(|c| c.get_visibility_database());
            for (ctl_name, ctl) in ctls.iter() {
                let default_catalog = ctl.info().catalog_type() == CatalogType::Default;

                if let Some(push_downs) = &push_downs {
                    if push_downs.filters.as_ref().map(|f| &f.filter).is_some() {
                        for db in &db_name {
                            match ctl.get_database(&tenant, db.as_str()).await {
                                Ok(database) => dbs.push(database),
                                Err(err) => {
                                    let msg = format!(
                                        "Failed to get database: {}.{}, {}",
                                        ctl.name(),
                                        db,
                                        err
                                    );
                                    warn!("{}", msg);
                                }
                            }
                        }

                        if visibility_checker.is_some() {
                            match ctl
                                .mget_table_names_by_ids(&tenant, &tables_ids, WITH_HISTORY)
                                .await
                            {
                                Ok(tables) => {
                                    for table in tables.into_iter().flatten() {
                                        tables_names.insert(table.clone());
                                    }
                                }
                                Err(err) => {
                                    let msg =
                                        format!("Failed to get tables: {}, {}", ctl.name(), err);
                                    warn!("{}", msg);
                                }
                            }
                        }
                    }
                }

                if dbs.is_empty() {
                    // Only Default catalog can use mget api
                    dbs = if catalog_dbs.is_some() && default_catalog {
                        let catalog_dbs = catalog_dbs.as_ref().unwrap();
                        let mut final_dbs = vec![];
                        for (catalog_name, dbs) in catalog_dbs {
                            if ctl.name() == *catalog_name {
                                let mut catalog_db_ids = vec![];
                                let mut catalog_db_names = vec![];
                                catalog_db_names.extend(
                                    dbs.iter()
                                        .filter_map(|(db_name, _)| *db_name)
                                        .map(|db_name| db_name.to_string()),
                                );
                                catalog_db_ids.extend(dbs.iter().filter_map(|(_, db_id)| *db_id));
                                if let Ok(databases) = ctl
                                    .mget_database_names_by_ids(&tenant, &catalog_db_ids)
                                    .await
                                {
                                    catalog_db_names.extend(databases.into_iter().flatten());
                                } else {
                                    let msg = format!(
                                        "Failed to get database name by id: {}",
                                        ctl.name()
                                    );
                                    warn!("{}", msg);
                                }
                                let db_idents = catalog_db_names
                                    .iter()
                                    .map(|name| DatabaseNameIdent::new(&tenant, name))
                                    .collect::<Vec<DatabaseNameIdent>>();
                                let dbs = ctl.mget_databases(&tenant, &db_idents).await?;
                                final_dbs.extend(dbs);
                            }
                        }
                        final_dbs
                    } else {
                        // None means has global level privileges
                        match ctl.list_databases(&tenant).await {
                            Ok(dbs) => dbs,
                            Err(err) => {
                                let msg = format!(
                                    "List databases failed on catalog {}: {}",
                                    ctl.name(),
                                    err
                                );
                                warn!("{}", msg);
                                ctx.push_warning(msg);

                                vec![]
                            }
                        }
                    }
                }

                let final_dbs = dbs
                    .clone()
                    .into_iter()
                    .filter(|db| {
                        visibility_checker
                            .as_ref()
                            .map(|c| {
                                c.check_database_visibility(
                                    ctl_name,
                                    db.name(),
                                    db.get_db_info().database_id.db_id,
                                )
                            })
                            .unwrap_or(true)
                    })
                    .collect::<Vec<_>>();
                // Now we get the final dbs, need to clear dbs vec.
                dbs.clear();

                let ownership = if get_ownership && visibility_checker.is_some() {
                    user_api.list_ownerships(&tenant).await.unwrap_or_default()
                } else {
                    HashMap::new()
                };
                let mock_table = ctl.is_external() && only_get_name;
                for db in final_dbs {
                    let db_id = db.get_db_info().database_id.db_id;
                    let db_name = db.name();
                    let tables = if tables_names.is_empty() || tables_names.len() > 10 {
                        match Self::list_tables(
                            ctl,
                            &tenant,
                            db_name,
                            WITH_HISTORY,
                            WITHOUT_VIEW,
                            mock_table,
                        )
                        .await
                        {
                            Ok(tables) => tables,
                            Err(err) => {
                                // swallow the errors related with remote database or tables, avoid ANY of bad table config corrupt ALL of the results.
                                // these databases might be:
                                // - sharing database
                                // - hive database
                                // - iceberg database
                                // - others
                                // TODO(liyz): return the warnings in the HTTP query protocol.
                                let msg = format!(
                                    "Failed to list tables in database: {}.{}, {}",
                                    ctl.name(),
                                    db_name,
                                    err
                                );
                                warn!("{}", msg);
                                ctx.push_warning(msg);

                                continue;
                            }
                        }
                    } else if WITH_HISTORY {
                        // Only can call get_table
                        let mut tables = Vec::new();
                        for table_name in &tables_names {
                            match ctl.get_table_history(&tenant, db_name, table_name).await {
                                Ok(t) => tables.extend(t),
                                Err(err) => {
                                    let msg = format!(
                                        "Failed to get_table_history tables in database: {}.{}, {}",
                                        ctl.name(),
                                        db_name,
                                        err
                                    );
                                    // warn no need to pad in ctx
                                    warn!("{}", msg);
                                    continue;
                                }
                            }
                        }
                        tables
                    } else {
                        // Only can call get_table
                        let mut tables = Vec::new();
                        for table_name in &tables_names {
                            match ctl.get_table(&tenant, db_name, table_name).await {
                                Ok(t) => tables.push(t),
                                Err(err) => {
                                    let msg = format!(
                                        "Failed to get table in database: {}.{}, {}",
                                        ctl.name(),
                                        db_name,
                                        err
                                    );
                                    // warn no need to pad in ctx
                                    warn!("{}", msg);
                                    continue;
                                }
                            }
                        }
                        tables
                    };

                    for table in tables {
                        let table_id = table.get_id();
                        let check_table_visibility = visibility_checker
                            .as_ref()
                            .map(|c| {
                                c.check_table_visibility(
                                    ctl_name,
                                    db_name,
                                    table.name(),
                                    db_id,
                                    table_id,
                                )
                            })
                            .unwrap_or(true);
                        // If db1 is visible, do not mean db1.table1 is visible. A user may have a grant about db1.table2, so db1 is visible
                        // for her, but db1.table1 may be not visible. So we need an extra check about table here after db visibility check.
                        if (table.get_table_info().engine() == "VIEW" || WITHOUT_VIEW)
                            && !table.is_stream()
                            && check_table_visibility
                        {
                            // system.tables store view name but not store view query
                            // decrease information_schema.tables union.
                            let role = if ownership.is_empty() {
                                None
                            } else {
                                ownership
                                    .get(&OwnershipObject::Table {
                                        catalog_name: ctl_name.to_string(),
                                        db_id,
                                        table_id,
                                    })
                                    .map(|role| role.to_string())
                            };
                            push_table_info(
                                &mut catalogs,
                                &mut databases,
                                &mut databases_ids,
                                &mut database_tables,
                                &mut owners,
                                ctl_name,
                                db.name(),
                                db.get_db_info().database_id.db_id,
                                table,
                                role,
                            );
                        }
                    }
                }
            }
        }

        let mut number_of_blocks: Vec<Option<u64>> = Vec::new();
        let mut number_of_segments: Vec<Option<u64>> = Vec::new();
        let mut num_rows: Vec<Option<u64>> = Vec::new();
        let mut data_sizes: Vec<Option<u64>> = Vec::new();
        let mut data_compressed_sizes: Vec<Option<u64>> = Vec::new();
        let mut index_sizes: Vec<Option<u64>> = Vec::new();
        let mut bloom_index_sizes: Vec<Option<u64>> = Vec::new();
        let mut ngram_index_sizes: Vec<Option<u64>> = Vec::new();
        let mut inverted_index_sizes: Vec<Option<u64>> = Vec::new();
        let mut vector_index_sizes: Vec<Option<u64>> = Vec::new();
        let mut virtual_column_sizes: Vec<Option<u64>> = Vec::new();

        if WITHOUT_VIEW {
            for tbl in &database_tables {
                // For performance considerations, allows using stale statistics data.
                let require_fresh = false;
                let stats = if get_stats {
                    match tbl.table_statistics(ctx.clone(), require_fresh, None).await {
                        Ok(stats) => stats,
                        Err(err) => {
                            let msg = format!(
                                "Unable to get table statistics on table {}: {}",
                                tbl.name(),
                                err
                            );
                            warn!("{}", msg);
                            ctx.push_warning(msg);

                            None
                        }
                    }
                } else {
                    None
                };
                num_rows.push(stats.as_ref().and_then(|v| v.num_rows));
                number_of_blocks.push(stats.as_ref().and_then(|v| v.number_of_blocks));
                number_of_segments.push(stats.as_ref().and_then(|v| v.number_of_segments));
                data_sizes.push(stats.as_ref().and_then(|v| v.data_size));
                data_compressed_sizes.push(stats.as_ref().and_then(|v| v.data_size_compressed));
                index_sizes.push(stats.as_ref().and_then(|v| v.index_size));
                bloom_index_sizes.push(stats.as_ref().and_then(|v| v.bloom_index_size));
                ngram_index_sizes.push(stats.as_ref().and_then(|v| v.ngram_index_size));
                inverted_index_sizes.push(stats.as_ref().and_then(|v| v.inverted_index_size));
                vector_index_sizes.push(stats.as_ref().and_then(|v| v.vector_index_size));
                virtual_column_sizes.push(stats.as_ref().and_then(|v| v.virtual_column_size));
            }
        }

        let names: Vec<String> = database_tables
            .iter()
            .map(|v| v.name().to_string())
            .collect();
        let tables_id: Vec<u64> = database_tables
            .iter()
            .map(|v| v.get_table_info().ident.table_id)
            .collect();
        let total_columns: Vec<u64> = database_tables
            .iter()
            .map(|v| v.get_table_info().schema().fields().len() as u64)
            .collect();
        let engines: Vec<String> = database_tables
            .iter()
            .map(|v| v.engine().to_string())
            .collect();
        let tables_types: Vec<String> = database_tables
            .iter()
            .map(|v| {
                if v.engine().to_uppercase() == "VIEW" {
                    "VIEW".to_string()
                } else {
                    "BASE TABLE".to_string()
                }
            })
            .collect();
        let engines_full: Vec<String> = engines.clone();
        let created_ons: Vec<i64> = database_tables
            .iter()
            .map(|v| v.get_table_info().meta.created_on.timestamp_micros())
            .collect();
        let dropped_ons: Vec<Option<i64>> = database_tables
            .iter()
            .map(|v| {
                v.get_table_info()
                    .meta
                    .drop_on
                    .map(|v| v.timestamp_micros())
            })
            .collect();
        let updated_ons = database_tables
            .iter()
            .map(|v| v.get_table_info().meta.updated_on.timestamp_micros())
            .collect::<Vec<_>>();

        let cluster_bys: Vec<String> = database_tables
            .iter()
            .map(|v| {
                v.get_table_info()
                    .meta
                    .cluster_key
                    .clone()
                    .unwrap_or_else(|| "".to_owned())
            })
            .collect();
        let is_transients: Vec<String> = database_tables
            .iter()
            .map(|v| {
                if v.options().contains_key("TRANSIENT") {
                    "TRANSIENT".to_string()
                } else {
                    "".to_string()
                }
            })
            .collect();
        let is_attaches: Vec<String> = database_tables
            .iter()
            .map(|v| {
                if FuseTable::is_table_attached(&v.get_table_info().meta.options) {
                    "ATTACH".to_string()
                } else {
                    "".to_string()
                }
            })
            .collect();
        let comments: Vec<String> = database_tables
            .iter()
            .map(|v| v.get_table_info().meta.comment.clone())
            .collect();

        let options: Vec<String> = database_tables
            .iter()
            .map(|v| {
                if v.get_table_info().engine().to_uppercase() == "VIEW" {
                    "".to_string()
                } else {
                    let mut opts = v.get_table_info().options().iter().collect::<Vec<_>>();
                    opts.sort_by_key(|(k, _)| *k);
                    opts.iter()
                        .filter(|(k, _)| !is_internal_opt_key(k))
                        .map(|(k, v)| format!(" {}='{}'", k.to_uppercase(), v))
                        .collect::<Vec<_>>()
                        .join("")
                }
            })
            .collect();

        let (is_externals, storage_params): (Vec<bool>, Vec<String>) = database_tables
            .iter()
            .map(|v| {
                let storage_params = &v.get_table_info().meta.storage_params;
                storage_params
                    .as_ref()
                    .map(|sp| (true, sp.to_string()))
                    .unwrap_or_else(|| (false, "".to_string()))
            })
            .unzip();

        let view_query: Vec<String> = database_tables
            .iter()
            .map(|v| -> String {
                let tbl_info = v.get_table_info();
                match tbl_info.engine() {
                    "VIEW" => {
                        let query = tbl_info.options().get(QUERY);
                        match query {
                            Some(query) => query.clone(),
                            None => String::from(""),
                        }
                    }
                    _ => String::from(""),
                }
            })
            .collect();

        if WITHOUT_VIEW {
            Self::generate_tables_block(
                catalogs,
                databases,
                databases_ids,
                names,
                tables_id,
                total_columns,
                engines,
                engines_full,
                cluster_bys,
                is_transients,
                is_attaches,
                created_ons,
                dropped_ons,
                updated_ons,
                num_rows,
                data_sizes,
                data_compressed_sizes,
                index_sizes,
                bloom_index_sizes,
                ngram_index_sizes,
                inverted_index_sizes,
                vector_index_sizes,
                virtual_column_sizes,
                number_of_segments,
                number_of_blocks,
                owners,
                comments,
                tables_types,
                is_externals,
                options,
                storage_params,
            )
        } else {
            Ok(DataBlock::new_from_columns(vec![
                StringType::from_data(catalogs),
                StringType::from_data(databases),
                UInt64Type::from_data(databases_ids),
                StringType::from_data(names),
                UInt64Type::from_data(tables_id),
                StringType::from_data(engines),
                StringType::from_data(engines_full),
                TimestampType::from_data(created_ons),
                TimestampType::from_opt_data(dropped_ons),
                TimestampType::from_data(updated_ons),
                StringType::from_opt_data(owners),
                StringType::from_data(comments),
                StringType::from_data(view_query),
            ]))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn generate_tables_block(
        catalogs: Vec<String>,
        databases: Vec<String>,
        databases_ids: Vec<u64>,
        names: Vec<String>,
        tables_id: Vec<u64>,
        total_columns: Vec<u64>,
        engines: Vec<String>,
        engines_full: Vec<String>,
        cluster_bys: Vec<String>,
        is_transients: Vec<String>,
        is_attaches: Vec<String>,
        created_ons: Vec<i64>,
        dropped_ons: Vec<Option<i64>>,
        updated_ons: Vec<i64>,
        num_rows: Vec<Option<u64>>,
        data_sizes: Vec<Option<u64>>,
        data_compressed_sizes: Vec<Option<u64>>,
        index_sizes: Vec<Option<u64>>,
        bloom_index_sizes: Vec<Option<u64>>,
        ngram_index_sizes: Vec<Option<u64>>,
        inverted_index_sizes: Vec<Option<u64>>,
        vector_index_sizes: Vec<Option<u64>>,
        virtual_column_sizes: Vec<Option<u64>>,
        number_of_segments: Vec<Option<u64>>,
        number_of_blocks: Vec<Option<u64>>,
        owners: Vec<Option<String>>,
        comments: Vec<String>,
        tables_type: Vec<String>,
        is_externals: Vec<bool>,
        options: Vec<String>,
        storage_params: Vec<String>,
    ) -> Result<DataBlock> {
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(catalogs),
            StringType::from_data(databases),
            UInt64Type::from_data(databases_ids),
            StringType::from_data(names),
            UInt64Type::from_data(tables_id),
            UInt64Type::from_data(total_columns),
            StringType::from_data(engines),
            StringType::from_data(engines_full),
            StringType::from_data(cluster_bys),
            StringType::from_data(is_transients),
            StringType::from_data(is_attaches),
            TimestampType::from_data(created_ons),
            TimestampType::from_opt_data(dropped_ons),
            TimestampType::from_data(updated_ons),
            UInt64Type::from_opt_data(num_rows),
            UInt64Type::from_opt_data(data_sizes),
            UInt64Type::from_opt_data(data_compressed_sizes),
            UInt64Type::from_opt_data(index_sizes),
            UInt64Type::from_opt_data(bloom_index_sizes),
            UInt64Type::from_opt_data(ngram_index_sizes),
            UInt64Type::from_opt_data(inverted_index_sizes),
            UInt64Type::from_opt_data(vector_index_sizes),
            UInt64Type::from_opt_data(virtual_column_sizes),
            UInt64Type::from_opt_data(number_of_segments),
            UInt64Type::from_opt_data(number_of_blocks),
            StringType::from_opt_data(owners),
            StringType::from_data(comments),
            StringType::from_data(tables_type),
            BooleanType::from_data(is_externals),
            StringType::from_data(options),
            StringType::from_data(storage_params),
        ]))
    }

    fn is_external_show_tables_query(
        &self,
        func_ctx: &FunctionContext,
        push_downs: &Option<PushDownInfo>,
        catalog: &Arc<dyn Catalog>,
    ) -> Option<(String, String)> {
        if !WITH_HISTORY && WITHOUT_VIEW {
            // Check projection
            if let Some(push_downs) = push_downs {
                if let Some(Projection::Columns(projection_indices)) = &push_downs.projection {
                    let schema = TablesTable::<WITH_HISTORY, WITHOUT_VIEW>::schema();
                    let name_fields_indexes: HashSet<usize> = schema
                        .fields
                        .iter()
                        .enumerate()
                        .filter_map(|(i, name)| match name.name().as_str() {
                            "name" | "database" | "catalog" | "table_type" => Some(i),
                            _ => None,
                        })
                        .collect();

                    if projection_indices.len() != 4
                        || !projection_indices
                            .iter()
                            .all(|field_index| name_fields_indexes.contains(field_index))
                    {
                        return None;
                    }

                    let mut filtered_catalog_names = vec![];
                    let mut filtered_db_names = vec![];

                    if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
                        let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
                        (filtered_catalog_names, filtered_db_names) =
                            extract_leveled_strings(&expr, &["catalog", "database"], func_ctx)
                                .ok()?;
                    }

                    // Check iceberg catalog existence
                    if filtered_catalog_names.len() == 1
                        && filtered_db_names.len() == 1
                        && catalog.name() == filtered_catalog_names[0].clone()
                    {
                        if let CatalogType::Iceberg = catalog.info().catalog_type() {
                            return Some((
                                filtered_catalog_names[0].clone(),
                                filtered_db_names[0].clone(),
                            ));
                        }
                    }
                }
            }
        }
        None
    }

    async fn show_tables_from_external_catalog(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog_name: String,
        db_name: String,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(&catalog_name).await?;
        let all_table_names = catalog.list_tables_names(&tenant, &db_name).await?;
        let rows = all_table_names.len();
        Self::generate_tables_block(
            vec![catalog_name; rows],
            vec![db_name; rows],
            vec![0; rows],
            all_table_names,
            vec![0; rows],
            vec![0; rows],
            vec!["".to_string(); rows],
            vec!["".to_string(); rows],
            vec!["".to_string(); rows],
            vec!["".to_string(); rows],
            vec!["".to_string(); rows],
            vec![0; rows],
            vec![Some(0); rows],
            vec![0; rows],
            vec![Some(0); rows],
            vec![Some(0); rows],
            vec![Some(0); rows],
            vec![Some(0); rows],
            vec![None; rows],
            vec![None; rows],
            vec![None; rows],
            vec![None; rows],
            vec![None; rows],
            vec![Some(0); rows],
            vec![Some(0); rows],
            vec![Some("".to_string()); rows],
            vec!["".to_string(); rows],
            vec!["BASE TABLE".to_string(); rows],
            vec![false; rows],
            vec!["".to_string(); rows],
            vec!["".to_string(); rows],
        )
    }

    pub fn create(table_id: u64, ctl_name: &str) -> Arc<dyn Table> {
        let name = Self::TABLE_NAME;
        let table_info = TableInfo {
            desc: format!("'system'.'{name}'"),
            name: Self::NAME.to_owned(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema: TablesTable::<WITH_HISTORY, WITHOUT_VIEW>::schema(),
                engine: "SystemTables".to_string(),

                ..Default::default()
            },
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), ctl_name).into(),
                meta: generate_catalog_meta(ctl_name),
                ..Default::default()
            }),
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(TablesTable::<WITH_HISTORY, WITHOUT_VIEW> { table_info })
    }
}

fn push_table_info(
    catalogs: &mut Vec<String>,
    databases: &mut Vec<String>,
    databases_ids: &mut Vec<u64>,
    database_tables: &mut Vec<Arc<dyn Table>>,
    owner: &mut Vec<Option<String>>,
    ctl_name: &str,
    db_name: &str,
    db_id: u64,
    table: Arc<dyn Table>,
    role: Option<String>,
) {
    catalogs.push(ctl_name.to_string());
    databases.push(db_name.to_string());
    databases_ids.push(db_id);
    database_tables.push(table);
    owner.push(role);
}
