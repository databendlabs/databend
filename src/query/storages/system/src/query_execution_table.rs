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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct QueryExecutionTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl SyncSystemTable for QueryExecutionTable {
    const NAME: &'static str = "system.query_execution";

    const DISTRIBUTION_LEVEL: DistributionLevel = DistributionLevel::Warehouse;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
    ) -> databend_common_exception::Result<DataBlock> {
        let running = ctx.get_query_execution_stats();
        let local_id = ctx.get_cluster().local_id.clone();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let valid_time_range = now - 10..now;

        // Create HashMap {timestamp:{query_id:process_rows}}
        let mut rows_by_timestamp: HashMap<u32, HashMap<String, u32>> = HashMap::new();

        // Create HashMap {timestamp:{query_id:process_times}}
        let mut times_by_timestamp: HashMap<u32, HashMap<String, u32>> = HashMap::new();

        // Aggregate data from all running queries
        for (query_id, stats) in running {
            // Process rows data
            for (timestamp, rows) in stats.process_rows {
                if !valid_time_range.contains(&timestamp) {
                    continue;
                }
                rows_by_timestamp
                    .entry(timestamp)
                    .or_insert_with(HashMap::new)
                    .insert(query_id.clone(), rows);
            }

            // Process times data
            for (timestamp, time_micros) in stats.process_time {
                if !valid_time_range.contains(&timestamp) {
                    continue;
                }
                times_by_timestamp
                    .entry(timestamp)
                    .or_insert_with(HashMap::new)
                    .insert(query_id.clone(), time_micros);
            }
        }

        let mut nodes = Vec::new();
        let mut timestamps = Vec::new();
        let mut process_rows_json = Vec::new();
        let mut process_times_json = Vec::new();

        let empty_map = HashMap::new();
        for timestamp in valid_time_range {
            nodes.push(local_id.clone());
            timestamps.push(timestamp as i64 * 1_000_000);

            let rows_for_timestamp = rows_by_timestamp.get(&timestamp).unwrap_or(&empty_map);
            let rows_json = serde_json::to_vec(rows_for_timestamp)?;
            process_rows_json.push(Some(rows_json));

            let times_for_timestamp = times_by_timestamp.get(&timestamp).unwrap_or(&empty_map);
            let times_json = serde_json::to_vec(times_for_timestamp)?;
            process_times_json.push(Some(times_json));
        }

        // Create DataBlock
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(nodes),
            TimestampType::from_data(timestamps),
            VariantType::from_opt_data(process_rows_json),
            VariantType::from_opt_data(process_times_json),
        ]))
    }
}

impl QueryExecutionTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("process_rows", TableDataType::Variant),
            TableField::new("process_time_in_micros", TableDataType::Variant),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'query_execution'".to_string(),
            name: "query_execution".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemProcesses".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(QueryExecutionTable { table_info })
    }
}
