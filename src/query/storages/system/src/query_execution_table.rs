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

use databend_common_base::runtime::ExecutorStatsSnapshot;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;
use crate::SystemLogElement;
use crate::SystemLogQueue;

pub type QueryExecutionStatsQueue = SystemLogQueue<QueryExecutionStatsElement>;
pub type QueryExecutionStatsElement = (String, ExecutorStatsSnapshot);

impl SystemLogElement for QueryExecutionStatsElement {
    const TABLE_NAME: &'static str = "DUMMY";

    fn schema() -> TableSchemaRef {
        unimplemented!("Only used log queue, not a table");
    }

    fn fill_to_data_block(
        &self,
        _columns: &mut Vec<ColumnBuilder>,
    ) -> databend_common_exception::Result<()> {
        unimplemented!("Only used log queue, not a table");
    }
}

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

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let mut running = ctx.get_running_query_execution_stats();
        let archive = self.get_archive()?;
        running.extend(archive);
        let local_id = ctx.get_cluster().local_id.clone();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let valid_time_range = now - 10..now;

        let mut rows_by_timestamp: HashMap<u32, HashMap<String, u32>> = HashMap::new();
        let mut times_by_timestamp: HashMap<u32, HashMap<String, u32>> = HashMap::new();

        for (query_id, stats) in running {
            aggregate_stats_by_timestamp(
                stats.process_rows,
                &query_id,
                &valid_time_range,
                &mut rows_by_timestamp,
            );
            aggregate_stats_by_timestamp(
                stats.process_time,
                &query_id,
                &valid_time_range,
                &mut times_by_timestamp,
            );
        }

        let columns = self.build_data_columns(
            local_id,
            valid_time_range,
            &rows_by_timestamp,
            &times_by_timestamp,
        )?;

        Ok(DataBlock::new_from_columns(columns))
    }
}

impl QueryExecutionTable {
    pub fn create(table_id: u64, max_rows: usize) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("query_id", TableDataType::String),
            TableField::new(
                "process_rows",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "process_time_in_micros",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'query_execution'".to_string(),
            name: "query_execution".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemQueryExecution".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        QueryExecutionStatsQueue::init(max_rows);

        SyncOneBlockSystemTable::create(QueryExecutionTable { table_info })
    }

    pub fn get_archive(&self) -> Result<Vec<QueryExecutionStatsElement>> {
        let archive = QueryExecutionStatsQueue::instance()?
            .data
            .read()
            .event_queue
            .iter()
            .filter_map(|e| e.as_ref().map(|e| e.clone()))
            .collect();
        Ok(archive)
    }

    fn build_data_columns(
        &self,
        local_id: String,
        valid_time_range: std::ops::Range<u32>,
        rows_by_timestamp: &HashMap<u32, HashMap<String, u32>>,
        times_by_timestamp: &HashMap<u32, HashMap<String, u32>>,
    ) -> Result<Vec<databend_common_expression::Column>> {
        let mut nodes = Vec::new();
        let mut timestamps = Vec::new();
        let mut query_ids = Vec::new();
        let mut process_rows = Vec::new();
        let mut process_times = Vec::new();

        let empty_map = HashMap::new();
        for timestamp in valid_time_range {
            let rows_for_timestamp = rows_by_timestamp.get(&timestamp).unwrap_or(&empty_map);
            let times_for_timestamp = times_by_timestamp.get(&timestamp).unwrap_or(&empty_map);

            let mut all_query_ids = std::collections::HashSet::new();
            all_query_ids.extend(rows_for_timestamp.keys());
            all_query_ids.extend(times_for_timestamp.keys());

            for query_id in all_query_ids {
                nodes.push(local_id.clone());
                timestamps.push(timestamp as i64 * 1_000_000);
                query_ids.push(query_id.clone());
                process_rows.push(rows_for_timestamp.get(query_id).copied().unwrap_or(0) as u64);
                process_times.push(times_for_timestamp.get(query_id).copied().unwrap_or(0) as u64);
            }
        }

        Ok(vec![
            StringType::from_data(nodes),
            TimestampType::from_data(timestamps),
            StringType::from_data(query_ids),
            UInt64Type::from_data(process_rows),
            UInt64Type::from_data(process_times),
        ])
    }
}

fn aggregate_stats_by_timestamp(
    stats_data: Vec<(u32, u32)>,
    query_id: &str,
    valid_time_range: &std::ops::Range<u32>,
    target_map: &mut HashMap<u32, HashMap<String, u32>>,
) {
    for (timestamp, value) in stats_data {
        if !valid_time_range.contains(&timestamp) {
            continue;
        }
        target_map
            .entry(timestamp)
            .or_default()
            .insert(query_id.to_string(), value);
    }
}
