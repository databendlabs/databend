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

use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::cache_manager::CacheRow;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct CachesTable {
    table_info: TableInfo,
}

#[derive(Default)]
struct CachesTableColumns {
    nodes: Vec<String>,
    names: Vec<String>,
    num_items: Vec<u64>,
    size: Vec<u64>,
    capacity: Vec<u64>,
    unit: Vec<String>,
    access: Vec<u64>,
    hit: Vec<u64>,
    miss: Vec<u64>,
}

impl SyncSystemTable for CachesTable {
    const NAME: &'static str = "system.caches";

    // Allow distributed query.
    const DISTRIBUTION_LEVEL: DistributionLevel = DistributionLevel::Warehouse;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let local_node = ctx.get_cluster().local_id.clone();
        let mut columns = CachesTableColumns::default();
        for row in CacheManager::instance().cache_rows() {
            Self::append_cache_row(row, &local_node, &mut columns);
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(columns.nodes),
            StringType::from_data(columns.names),
            UInt64Type::from_data(columns.num_items),
            UInt64Type::from_data(columns.size),
            UInt64Type::from_data(columns.capacity),
            StringType::from_data(columns.unit),
            UInt64Type::from_data(columns.access),
            UInt64Type::from_data(columns.hit),
            UInt64Type::from_data(columns.miss),
        ]))
    }
}

impl CachesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("num_items", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("capacity", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("unit", TableDataType::String),
            TableField::new("access", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("hit", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("miss", TableDataType::Number(NumberDataType::UInt64)),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'caches'".to_string(),
            name: "caches".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemCache".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };
        SyncOneBlockSystemTable::create(Self { table_info })
    }

    fn append_cache_row(row: CacheRow, local_node: &str, columns: &mut CachesTableColumns) {
        columns.nodes.push(local_node.to_string());
        columns.names.push(row.name);
        columns.num_items.push(row.num_items);
        columns.size.push(row.size);
        columns.capacity.push(row.capacity.unwrap_or(u64::MAX));
        columns.unit.push(row.unit.to_string());
        columns.access.push(row.access);
        columns.hit.push(row.hit);
        columns.miss.push(row.miss);
    }
}
