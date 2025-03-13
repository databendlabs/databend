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
use databend_common_exception::Result;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_view::view_table::QUERY;
use databend_common_users::UserApiProvider;
use log::warn;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::find_eq_or_filter;

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
            ) -> Result<Vec<Arc<dyn Table>>> {
                if with_history {
                    catalog.list_tables_history(tenant, database_name).await
                } else {
                    catalog.list_tables(tenant, database_name).await
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
        let catalogs = catalog_mgr
            .list_catalogs(&tenant, ctx.session_state())
            .await?
            .into_iter()
            .map(|cat| cat.disable_table_info_refresh())
            .collect::<Result<Vec<_>>>()?;
        self.get_full_data_from_catalogs(ctx, push_downs, catalogs)
            .await
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

    /// dump all the tables from all the catalogs with pushdown, this is used for `SHOW TABLES` command.
    /// please note that this function is intended to not wrapped with Result<>, because we do not want to
    /// break ALL the output on reading ANY of the catalog, database or table failed.
    #[async_backtrace::framed]
    async fn get_full_data_from_catalogs(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        catalogs: Vec<Arc<dyn Catalog>>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();

        let ctls: Vec<(String, Arc<dyn Catalog>)> =
            catalogs.iter().map(|e| (e.name(), e.clone())).collect();

        let mut catalogs = vec![];
        let mut databases = vec![];
        let mut databases_ids = vec![];

        let mut database_tables = vec![];
        let mut owner: Vec<Option<String>> = Vec::new();
        let user_api = UserApiProvider::instance();

        let mut dbs = Vec::new();
        let mut tables_names: BTreeSet<String> = BTreeSet::new();
        let mut invalid_tables_ids = false;
        let mut tables_ids: Vec<u64> = Vec::new();
        let mut db_name: Vec<String> = Vec::new();

        let mut get_stats = true;
        let mut get_ownership = true;
        let mut owner_field_indexes: HashSet<usize> = HashSet::new();
        let mut stats_fields_indexes: HashSet<usize> = HashSet::new();
        let schema = TablesTable::<WITH_HISTORY, WITHOUT_VIEW>::schema();
        for (i, name) in schema.fields.iter().enumerate() {
            match name.name().as_str() {
                "num_rows"
                | "data_size"
                | "data_compressed_size"
                | "index_size"
                | "number_of_segments"
                | "number_of_blocks" => {
                    stats_fields_indexes.insert(i);
                }
                "owner" => {
                    owner_field_indexes.insert(i);
                }
                _ => {}
            }
        }

        let mut invalid_optimize = false;
        if let Some(push_downs) = &push_downs {
            if let Some(Projection::Columns(v)) = push_downs.projection.as_ref() {
                get_stats = v
                    .iter()
                    .any(|field_index| stats_fields_indexes.contains(field_index));
                get_ownership = v
                    .iter()
                    .any(|field_index| owner_field_indexes.contains(field_index));
            }

            if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
                let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
                invalid_optimize = find_eq_or_filter(
                    &expr,
                    &mut |col_name, scalar| {
                        if col_name == "database" {
                            if let Scalar::String(database) = scalar {
                                if !db_name.contains(database) {
                                    db_name.push(database.clone());
                                }
                            }
                        } else if col_name == "table_id" {
                            match check_number::<_, u64>(
                                None,
                                &FunctionContext::default(),
                                &Expr::<usize>::Constant {
                                    span: None,
                                    scalar: scalar.clone(),
                                    data_type: scalar.as_ref().infer_data_type(),
                                },
                                &BUILTIN_FUNCTIONS,
                            ) {
                                Ok(id) => tables_ids.push(id),
                                Err(_) => invalid_tables_ids = true,
                            }
                        } else if col_name == "name" {
                            if let Scalar::String(t_name) = scalar {
                                tables_names.insert(t_name.clone());
                            }
                        }
                        Ok(())
                    },
                    invalid_optimize,
                );
            }
        }

        let visibility_checker = ctx.get_visibility_checker(false).await?;
        // from system.tables where database = 'db' and name = 'name'
        // from system.tables where database = 'db' and table_id = 123
        if db_name.len() == 1
            && !invalid_optimize
            && tables_names.len() + tables_ids.len() == 1
            && !invalid_tables_ids
            && !WITH_HISTORY
        {
            for (ctl_name, ctl) in ctls.iter() {
                for db in &db_name {
                    match ctl.get_database(&tenant, db.as_str()).await {
                        Ok(database) => dbs.push(database),
                        Err(err) => {
                            let msg = format!("Failed to get database: {}, {}", db, err);
                            warn!("{}", msg);
                        }
                    }
                }
                match ctl
                    .mget_table_names_by_ids(&tenant, &tables_ids, false)
                    .await
                {
                    Ok(new_tables) => {
                        let new_table_names: BTreeSet<_> =
                            new_tables.into_iter().flatten().collect();
                        tables_names.extend(new_table_names);
                    }
                    Err(err) => {
                        // swallow the errors related with mget tables
                        warn!("Failed to get tables: {}, {}", ctl.name(), err);
                    }
                }

                for table_name in &tables_names {
                    for db in &dbs {
                        match ctl.get_table(&tenant, db.name(), table_name).await {
                            Ok(t) => {
                                let db_id = db.get_db_info().database_id.db_id;
                                let table_id = t.get_id();
                                let role = user_api
                                    .role_api(&tenant)
                                    .get_ownership(&OwnershipObject::Table {
                                        catalog_name: ctl_name.to_string(),
                                        db_id,
                                        table_id,
                                    })
                                    .await?
                                    .map(|o| o.role);
                                if visibility_checker.check_table_visibility(
                                    ctl_name,
                                    db.name(),
                                    table_name,
                                    db_id,
                                    t.get_id(),
                                ) {
                                    catalogs.push(ctl_name.as_str());
                                    databases.push(db.name().to_owned());
                                    databases_ids.push(db.get_db_info().database_id.db_id);
                                    database_tables.push(t);
                                    owner.push(role);
                                } else if let Some(role) = role {
                                    let roles = ctx.get_all_effective_roles().await?;
                                    if roles.iter().any(|r| r.name == role) {
                                        catalogs.push(ctl_name.as_str());
                                        databases.push(db.name().to_owned());
                                        databases_ids.push(db.get_db_info().database_id.db_id);
                                        database_tables.push(t);
                                        owner.push(Some(role));
                                    }
                                }
                            }
                            Err(err) => {
                                let msg = format!(
                                    "Failed to get table in database: {}, {}",
                                    db.name(),
                                    err
                                );
                                // warn no need to pad in ctx
                                warn!("{}", msg);
                                continue;
                            }
                        }
                    }
                }
            }
        } else {
            let catalog_dbs = visibility_checker.get_visibility_database();

            for (ctl_name, ctl) in ctls.iter() {
                if let Some(push_downs) = &push_downs {
                    if push_downs.filters.as_ref().map(|f| &f.filter).is_some() {
                        for db in &db_name {
                            match ctl.get_database(&tenant, db.as_str()).await {
                                Ok(database) => dbs.push(database),
                                Err(err) => {
                                    let msg = format!("Failed to get database: {}, {}", db, err);
                                    warn!("{}", msg);
                                }
                            }
                        }

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
                                let msg = format!("Failed to get tables: {}, {}", ctl.name(), err);
                                warn!("{}", msg);
                            }
                        }
                    }
                }

                if dbs.is_empty() || invalid_optimize {
                    // None means has global level privileges
                    dbs = if let Some(catalog_dbs) = &catalog_dbs {
                        let mut final_dbs = vec![];
                        for (catalog_name, dbs) in catalog_dbs {
                            if ctl.name() == catalog_name.to_string() {
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
                        visibility_checker.check_database_visibility(
                            ctl_name,
                            db.name(),
                            db.get_db_info().database_id.db_id,
                        )
                    })
                    .collect::<Vec<_>>();
                // Now we get the final dbs, need to clear dbs vec.
                dbs.clear();

                let ownership = if get_ownership {
                    user_api.get_ownerships(&tenant).await.unwrap_or_default()
                } else {
                    HashMap::new()
                };
                for db in final_dbs {
                    let db_id = db.get_db_info().database_id.db_id;
                    let db_name = db.name();
                    let tables = if tables_names.is_empty()
                        || tables_names.len() > 10
                        || invalid_tables_ids
                        || invalid_optimize
                    {
                        match Self::list_tables(ctl, &tenant, db_name, WITH_HISTORY, WITHOUT_VIEW)
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
                                    "Failed to list tables in database: {}, {}",
                                    db_name, err
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
                                        "Failed to get_table_history tables in database: {}, {}",
                                        db_name, err
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
                                        "Failed to get table in database: {}, {}",
                                        db_name, err
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
                        // If db1 is visible, do not mean db1.table1 is visible. A user may have a grant about db1.table2, so db1 is visible
                        // for her, but db1.table1 may be not visible. So we need an extra check about table here after db visibility check.
                        if (table.get_table_info().engine() == "VIEW" || WITHOUT_VIEW)
                            && !table.is_stream()
                            && visibility_checker.check_table_visibility(
                                ctl_name,
                                db_name,
                                table.name(),
                                db_id,
                                table_id,
                            )
                        {
                            // system.tables store view name but not store view query
                            // decrease information_schema.tables union.
                            catalogs.push(ctl_name.as_str());
                            databases.push(db_name.to_owned());
                            databases_ids.push(db.get_db_info().database_id.db_id);
                            database_tables.push(table);
                            if ownership.is_empty() {
                                owner.push(None);
                            } else {
                                owner.push(
                                    ownership
                                        .get(&OwnershipObject::Table {
                                            catalog_name: ctl_name.to_string(),
                                            db_id,
                                            table_id,
                                        })
                                        .map(|role| role.to_string()),
                                );
                            }
                        }
                    }
                }
            }
        }

        let mut number_of_blocks: Vec<Option<u64>> = Vec::new();
        let mut number_of_segments: Vec<Option<u64>> = Vec::new();
        let mut num_rows: Vec<Option<u64>> = Vec::new();
        let mut data_size: Vec<Option<u64>> = Vec::new();
        let mut data_compressed_size: Vec<Option<u64>> = Vec::new();
        let mut index_size: Vec<Option<u64>> = Vec::new();

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
                data_size.push(stats.as_ref().and_then(|v| v.data_size));
                data_compressed_size.push(stats.as_ref().and_then(|v| v.data_size_compressed));
                index_size.push(stats.as_ref().and_then(|v| v.index_size));
            }
        }

        let names: Vec<String> = database_tables
            .iter()
            .map(|v| v.name().to_string())
            .collect();
        let table_id: Vec<u64> = database_tables
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
        let tables_type: Vec<String> = database_tables
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
        let created_on: Vec<i64> = database_tables
            .iter()
            .map(|v| v.get_table_info().meta.created_on.timestamp_micros())
            .collect();
        let dropped_on: Vec<Option<i64>> = database_tables
            .iter()
            .map(|v| {
                v.get_table_info()
                    .meta
                    .drop_on
                    .map(|v| v.timestamp_micros())
            })
            .collect();
        let updated_on = database_tables
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
        let is_transient: Vec<String> = database_tables
            .iter()
            .map(|v| {
                if v.options().contains_key("TRANSIENT") {
                    "TRANSIENT".to_string()
                } else {
                    "".to_string()
                }
            })
            .collect();
        let is_attach: Vec<String> = database_tables
            .iter()
            .map(|v| {
                if FuseTable::is_table_attached(&v.get_table_info().meta.options) {
                    "ATTACH".to_string()
                } else {
                    "".to_string()
                }
            })
            .collect();
        let comment: Vec<String> = database_tables
            .iter()
            .map(|v| v.get_table_info().meta.comment.clone())
            .collect();

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
            Ok(DataBlock::new_from_columns(vec![
                StringType::from_data(catalogs),
                StringType::from_data(databases),
                UInt64Type::from_data(databases_ids),
                StringType::from_data(names),
                UInt64Type::from_data(table_id),
                UInt64Type::from_data(total_columns),
                StringType::from_data(engines),
                StringType::from_data(engines_full),
                StringType::from_data(cluster_bys),
                StringType::from_data(is_transient),
                StringType::from_data(is_attach),
                TimestampType::from_data(created_on),
                TimestampType::from_opt_data(dropped_on),
                TimestampType::from_data(updated_on),
                UInt64Type::from_opt_data(num_rows),
                UInt64Type::from_opt_data(data_size),
                UInt64Type::from_opt_data(data_compressed_size),
                UInt64Type::from_opt_data(index_size),
                UInt64Type::from_opt_data(number_of_segments),
                UInt64Type::from_opt_data(number_of_blocks),
                StringType::from_opt_data(owner),
                StringType::from_data(comment),
                StringType::from_data(tables_type),
            ]))
        } else {
            Ok(DataBlock::new_from_columns(vec![
                StringType::from_data(catalogs),
                StringType::from_data(databases),
                UInt64Type::from_data(databases_ids),
                StringType::from_data(names),
                UInt64Type::from_data(table_id),
                StringType::from_data(engines),
                StringType::from_data(engines_full),
                TimestampType::from_data(created_on),
                TimestampType::from_opt_data(dropped_on),
                TimestampType::from_data(updated_on),
                StringType::from_opt_data(owner),
                StringType::from_data(comment),
                StringType::from_data(view_query),
            ]))
        }
    }

    pub fn create(table_id: u64) -> Arc<dyn Table> {
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
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(TablesTable::<WITH_HISTORY, WITHOUT_VIEW> { table_info })
    }
}
