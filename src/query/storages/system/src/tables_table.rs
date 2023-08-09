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

use common_catalog::catalog::Catalog;
use common_catalog::catalog::CatalogManager;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::number::UInt64Type;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::utils::FromData;
use common_expression::DataBlock;
use common_expression::FromOptData;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_expression::TableSchemaRefExt;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use log::warn;

use crate::columns_table::GrantObjectVisibilityChecker;
use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::find_eq_filter;

pub struct TablesTable<const WITH_HISTORY: bool> {
    table_info: TableInfo,
}

pub type TablesTableWithHistory = TablesTable<true>;
pub type TablesTableWithoutHistory = TablesTable<false>;

#[async_trait::async_trait]
pub trait HistoryAware {
    const TABLE_NAME: &'static str;
    async fn list_tables(
        catalog: &Arc<dyn Catalog>,
        tenant: &str,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>>;
}

#[async_trait::async_trait]
impl HistoryAware for TablesTable<true> {
    const TABLE_NAME: &'static str = "tables_with_history";
    #[async_backtrace::framed]
    async fn list_tables(
        catalog: &Arc<dyn Catalog>,
        tenant: &str,
        database_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        catalog.list_tables_history(tenant, database_name).await
    }
}

#[async_trait::async_trait]
impl HistoryAware for TablesTable<false> {
    const TABLE_NAME: &'static str = "tables";
    #[async_backtrace::framed]
    async fn list_tables(
        catalog: &Arc<dyn Catalog>,
        tenant: &str,
        database_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        catalog.list_tables(tenant, database_name).await
    }
}

#[async_trait::async_trait]
impl<const T: bool> AsyncSystemTable for TablesTable<T>
where TablesTable<T>: HistoryAware
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
        let ctls: Vec<(String, Arc<dyn Catalog>)> = catalog_mgr
            .list_catalogs(&tenant)
            .await?
            .iter()
            .map(|e| (e.name(), e.clone()))
            .collect();

        let mut catalogs = vec![];
        let mut databases = vec![];

        let mut database_tables = vec![];

        let user = ctx.get_current_user()?;
        let roles = ctx.get_current_available_roles().await?;
        let visibility_checker = GrantObjectVisibilityChecker::new(&user, &roles);

        for (ctl_name, ctl) in ctls.into_iter() {
            let mut dbs = Vec::new();
            if let Some(push_downs) = &push_downs {
                let mut db_name = Vec::new();
                if let Some(filter) = &push_downs.filter {
                    let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
                    find_eq_filter(&expr, &mut |col_name, scalar| {
                        if col_name == "database" {
                            if let Scalar::String(s) = scalar {
                                if let Ok(database) = String::from_utf8(s.clone()) {
                                    db_name.push(database);
                                }
                            }
                        }
                    });
                    for db in db_name {
                        if let Ok(database) = ctl.get_database(tenant.as_str(), db.as_str()).await {
                            dbs.push(database);
                        }
                    }
                }
            }

            if dbs.is_empty() {
                dbs = ctl.list_databases(tenant.as_str()).await?;
            }
            let ctl_name: &str = Box::leak(ctl_name.into_boxed_str());

            let final_dbs = dbs
                .into_iter()
                .filter(|db| visibility_checker.check_database_visibility(ctl_name, db.name()))
                .collect::<Vec<_>>();
            for db in final_dbs {
                let name = db.name().to_string().into_boxed_str();
                let name: &str = Box::leak(name);
                let tables = match Self::list_tables(&ctl, tenant.as_str(), name).await {
                    Ok(tables) => tables,
                    Err(err) => {
                        // Swallow the errors related with sharing. Listing tables in a shared database
                        // is easy to get errors with invalid configs, but system.tables is better not
                        // to be affected by it.
                        if db.get_db_info().meta.from_share.is_some() {
                            warn!("list tables failed on sharing db {}: {}", db.name(), err);
                            continue;
                        }
                        return Err(err);
                    }
                };

                for table in tables {
                    // If db1 is visible, do not means db1.table1 is visible. An user may have a grant about db1.table2, so db1 is visible
                    // for her, but db1.table1 may be not visible. So we need an extra check about table here after db visibility check.
                    if visibility_checker.check_table_visibility(ctl_name, db.name(), table.name())
                    {
                        catalogs.push(ctl_name.as_bytes().to_vec());
                        databases.push(name.as_bytes().to_vec());
                        database_tables.push(table);
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

        for tbl in &database_tables {
            let stats = tbl.table_statistics()?;
            num_rows.push(stats.as_ref().and_then(|v| v.num_rows));
            number_of_blocks.push(stats.as_ref().and_then(|v| v.number_of_blocks));
            number_of_segments.push(stats.as_ref().and_then(|v| v.number_of_segments));
            data_size.push(stats.as_ref().and_then(|v| v.data_size));
            data_compressed_size.push(stats.as_ref().and_then(|v| v.data_size_compressed));
            index_size.push(stats.as_ref().and_then(|v| v.index_size));
        }

        let names: Vec<Vec<u8>> = database_tables
            .iter()
            .map(|v| v.name().as_bytes().to_vec())
            .collect();
        let table_id: Vec<u64> = database_tables
            .iter()
            .map(|v| v.get_table_info().ident.table_id)
            .collect();
        let engines: Vec<Vec<u8>> = database_tables
            .iter()
            .map(|v| v.engine().as_bytes().to_vec())
            .collect();
        let engines_full: Vec<Vec<u8>> = engines.clone();
        let created_owns: Vec<String> = database_tables
            .iter()
            .map(|v| {
                v.get_table_info()
                    .meta
                    .created_on
                    .format("%Y-%m-%d %H:%M:%S.%3f %z")
                    .to_string()
            })
            .collect();
        let created_owns: Vec<Vec<u8>> =
            created_owns.iter().map(|s| s.as_bytes().to_vec()).collect();
        let dropped_owns: Vec<String> = database_tables
            .iter()
            .map(|v| {
                v.get_table_info()
                    .meta
                    .drop_on
                    .map(|v| v.format("%Y-%m-%d %H:%M:%S.%3f %z").to_string())
                    .unwrap_or_else(|| "NULL".to_owned())
            })
            .collect();
        let dropped_owns: Vec<Vec<u8>> =
            dropped_owns.iter().map(|s| s.as_bytes().to_vec()).collect();

        let updated_on = database_tables
            .iter()
            .map(|v| v.get_table_info().meta.updated_on.timestamp_micros())
            .collect::<Vec<_>>();

        let cluster_bys: Vec<String> = database_tables
            .iter()
            .map(|v| {
                v.get_table_info()
                    .meta
                    .default_cluster_key
                    .clone()
                    .unwrap_or_else(|| "".to_owned())
            })
            .collect();
        let cluster_bys: Vec<Vec<u8>> = cluster_bys.iter().map(|s| s.as_bytes().to_vec()).collect();
        let is_transient: Vec<Vec<u8>> = database_tables
            .iter()
            .map(|v| {
                if v.options().contains_key("TRANSIENT") {
                    "TRANSIENT".as_bytes().to_vec()
                } else {
                    vec![]
                }
            })
            .collect();
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(catalogs),
            StringType::from_data(databases),
            StringType::from_data(names),
            UInt64Type::from_data(table_id),
            StringType::from_data(engines),
            StringType::from_data(engines_full),
            StringType::from_data(cluster_bys),
            StringType::from_data(is_transient),
            StringType::from_data(created_owns),
            StringType::from_data(dropped_owns),
            TimestampType::from_data(updated_on),
            UInt64Type::from_opt_data(num_rows),
            UInt64Type::from_opt_data(data_size),
            UInt64Type::from_opt_data(data_compressed_size),
            UInt64Type::from_opt_data(index_size),
            UInt64Type::from_opt_data(number_of_segments),
            UInt64Type::from_opt_data(number_of_blocks),
        ]))
    }
}

impl<const T: bool> TablesTable<T>
where TablesTable<T>: HistoryAware
{
    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("catalog", TableDataType::String),
            TableField::new("database", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("engine", TableDataType::String),
            TableField::new("engine_full", TableDataType::String),
            TableField::new("cluster_by", TableDataType::String),
            TableField::new("is_transient", TableDataType::String),
            TableField::new("created_on", TableDataType::String),
            TableField::new("dropped_on", TableDataType::String),
            TableField::new("updated_on", TableDataType::Timestamp),
            TableField::new(
                "num_rows",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "data_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "data_compressed_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "index_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "number_of_segments",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "number_of_blocks",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
        ])
    }

    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let name = Self::TABLE_NAME;
        let table_info = TableInfo {
            desc: format!("'system'.'{name}'"),
            name: Self::NAME.to_owned(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema: TablesTable::<T>::schema(),
                engine: "SystemTables".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(TablesTable::<T> { table_info })
    }
}
