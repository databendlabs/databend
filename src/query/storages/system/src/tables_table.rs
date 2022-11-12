// Copyright 2021 Datafuse Labs.
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
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::DataType;
use common_expression::NumberDataType;
use common_expression::SchemaDataType;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct TablesTable<const WITH_HISTROY: bool> {
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

    async fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<Chunk> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(CATALOG_DEFAULT)?;
        let databases = catalog.list_databases(tenant.as_str()).await?;

        let mut database_tables = vec![];
        for database in databases {
            let name = database.name();
            let tables = Self::list_tables(&catalog, tenant.as_str(), name).await?;
            for table in tables {
                database_tables.push((name.to_string(), table));
            }
        }

        let mut num_rows: Vec<Option<u64>> = Vec::new();
        let mut data_size: Vec<Option<u64>> = Vec::new();
        let mut data_compressed_size: Vec<Option<u64>> = Vec::new();
        let mut index_size: Vec<Option<u64>> = Vec::new();

        for (_, tbl) in &database_tables {
            let stats = tbl.table_statistics()?;
            num_rows.push(stats.as_ref().and_then(|v| v.num_rows));
            data_size.push(stats.as_ref().and_then(|v| v.data_size));
            data_compressed_size.push(stats.as_ref().and_then(|v| v.data_size_compressed));
            index_size.push(stats.and_then(|v| v.index_size));
        }

        let databases: Vec<&[u8]> = database_tables.iter().map(|(d, _)| d.as_bytes()).collect();
        let names: Vec<&[u8]> = database_tables
            .iter()
            .map(|(_, v)| v.name().as_bytes())
            .collect();
        let engines: Vec<&[u8]> = database_tables
            .iter()
            .map(|(_, v)| v.engine().as_bytes())
            .collect();
        let created_ons: Vec<String> = database_tables
            .iter()
            .map(|(_, v)| {
                v.get_table_info()
                    .meta
                    .created_on
                    .format("%Y-%m-%d %H:%M:%S.%3f %z")
                    .to_string()
            })
            .collect();
        let dropped_ons: Vec<String> = database_tables
            .iter()
            .map(|(_, v)| {
                v.get_table_info()
                    .meta
                    .drop_on
                    .map(|v| v.format("%Y-%m-%d %H:%M:%S.%3f %z").to_string())
                    .unwrap_or_else(|| "NULL".to_owned())
            })
            .collect();
        let created_ons: Vec<&[u8]> = created_ons.iter().map(|s| s.as_bytes()).collect();
        let cluster_bys: Vec<String> = database_tables
            .iter()
            .map(|(_, v)| {
                v.get_table_info()
                    .meta
                    .default_cluster_key
                    .clone()
                    .unwrap_or_else(|| "".to_owned())
            })
            .collect();

        let rows_len = databases.len();
        Ok(Chunk::new(
            vec![
                (
                    Value::Column(Column::from_data(databases)),
                    DataType::String,
                ),
                (Value::Column(Column::from_data(names)), DataType::String),
                (Value::Column(Column::from_data(engines)), DataType::String),
                (
                    Value::Column(Column::from_data(cluster_bys)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(created_ons)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(dropped_ons)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(num_rows)),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::from_data(data_size)),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::from_data(data_compressed_size)),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::from_data(index_size)),
                    DataType::Number(NumberDataType::UInt64),
                ),
            ],
            rows_len,
        ))
    }
}

impl<const T: bool> TablesTable<T>
where TablesTable<T>: HistoryAware
{
    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("database", SchemaDataType::String),
            DataField::new("name", SchemaDataType::String),
            DataField::new("engine", SchemaDataType::String),
            DataField::new("cluster_by", SchemaDataType::String),
            DataField::new("created_on", SchemaDataType::String),
            DataField::new("dropped_on", SchemaDataType::String),
            DataField::new_nullable("num_rows", SchemaDataType::Number(NumberDataType::UInt64)),
            DataField::new_nullable("data_size", SchemaDataType::Number(NumberDataType::UInt64)),
            DataField::new_nullable(
                "data_compressed_size",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new_nullable("index_size", SchemaDataType::Number(NumberDataType::UInt64)),
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
