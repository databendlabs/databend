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

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_metrics::cache::get_cache_access_count;
use databend_common_metrics::cache::get_cache_hit_count;
use databend_common_metrics::cache::get_cache_miss_count;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CountableMeter;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_cache::Unit;
use databend_storages_common_cache::DISK_TABLE_DATA_CACHE_NAME;

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
    const IS_LOCAL: bool = false;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let local_node = ctx.get_cluster().local_id.clone();
        let cache_manager = CacheManager::instance();
        let table_snapshot_cache = cache_manager.get_table_snapshot_cache();
        let table_snapshot_statistic_cache = cache_manager.get_table_snapshot_statistics_cache();
        let segment_info_cache = cache_manager.get_table_segment_cache();
        let bloom_index_filter_cache = cache_manager.get_bloom_index_filter_cache();
        let bloom_index_meta_cache = cache_manager.get_bloom_index_meta_cache();
        let block_meta_cache = cache_manager.get_block_meta_cache();
        let inverted_index_meta_cache = cache_manager.get_inverted_index_meta_cache();
        let inverted_index_file_cache = cache_manager.get_inverted_index_file_cache();
        let prune_partitions_cache = cache_manager.get_prune_partitions_cache();
        let file_meta_data_cache = cache_manager.get_file_meta_data_cache();
        let table_data_cache = cache_manager.get_table_data_cache();
        let table_column_array_cache = cache_manager.get_table_data_array_cache();

        let mut columns = CachesTableColumns::default();

        if let Some(table_snapshot_cache) = table_snapshot_cache {
            Self::append_row(&table_snapshot_cache, &local_node, &mut columns);
        }
        if let Some(table_snapshot_statistic_cache) = table_snapshot_statistic_cache {
            Self::append_row(&table_snapshot_statistic_cache, &local_node, &mut columns);
        }

        if let Some(segment_info_cache) = segment_info_cache {
            Self::append_row(&segment_info_cache, &local_node, &mut columns);
        }

        if let Some(bloom_index_filter_cache) = bloom_index_filter_cache {
            Self::append_row(&bloom_index_filter_cache, &local_node, &mut columns);
        }

        if let Some(bloom_index_meta_cache) = bloom_index_meta_cache {
            Self::append_row(&bloom_index_meta_cache, &local_node, &mut columns);
        }

        if let Some(block_meta_cache) = block_meta_cache {
            Self::append_row(&block_meta_cache, &local_node, &mut columns);
        }

        if let Some(inverted_index_meta_cache) = inverted_index_meta_cache {
            Self::append_row(&inverted_index_meta_cache, &local_node, &mut columns);
        }

        if let Some(inverted_index_file_cache) = inverted_index_file_cache {
            Self::append_row(&inverted_index_file_cache, &local_node, &mut columns);
        }

        if let Some(prune_partitions_cache) = prune_partitions_cache {
            Self::append_row(&prune_partitions_cache, &local_node, &mut columns);
        }

        if let Some(file_meta_data_cache) = file_meta_data_cache {
            Self::append_row(&file_meta_data_cache, &local_node, &mut columns);
        }

        if let Some(cache) = table_data_cache {
            // table data cache is not a named cache yet
            columns.nodes.push(local_node.clone());
            columns.names.push(DISK_TABLE_DATA_CACHE_NAME.to_string());
            columns.num_items.push(cache.len() as u64);
            columns.size.push(cache.size());
            columns.capacity.push(cache.capacity());
            columns.unit.push(Unit::Bytes.to_string());
            let access = get_cache_access_count(DISK_TABLE_DATA_CACHE_NAME);
            let hit = get_cache_hit_count(DISK_TABLE_DATA_CACHE_NAME);
            let miss = get_cache_miss_count(DISK_TABLE_DATA_CACHE_NAME);
            columns.access.push(access);
            columns.hit.push(hit);
            columns.miss.push(miss);
        }

        if let Some(table_column_array_cache) = table_column_array_cache {
            Self::append_row(&table_column_array_cache, &local_node, &mut columns);
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

    fn append_row<V, M: CountableMeter<String, Arc<V>>>(
        cache: &InMemoryLruCache<V, M>,
        local_node: &str,
        columns: &mut CachesTableColumns,
    ) {
        columns.nodes.push(local_node.to_string());
        columns.names.push(cache.name().to_string());
        columns.num_items.push(cache.len() as u64);
        columns.size.push(cache.size());
        columns.capacity.push(cache.capacity());
        columns.unit.push(cache.unit().to_string());

        let access = get_cache_access_count(cache.name());
        let hit = get_cache_hit_count(cache.name());
        let miss = get_cache_miss_count(cache.name());

        columns.access.push(access);
        columns.hit.push(hit);
        columns.miss.push(miss);
    }
}
