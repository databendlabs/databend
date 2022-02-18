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

use common_base::tokio;
use common_datablocks::assert_blocks_sorted_eq;
use common_datablocks::DataBlock;
use common_datavalues::prelude::Series;
use common_datavalues::prelude::SeriesFrom;
use common_exception::Result;
use databend_query::storages::system::QueryLogTable;
use databend_query::storages::Table;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_log_table() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    ctx.get_settings().set_max_threads(2)?;

    let mut query_log = QueryLogTable::create(0);
    query_log.set_max_rows(2);
    let schema = query_log.schema();
    let table: Arc<dyn Table> = Arc::new(query_log);

    // Insert.
    {
        let block = DataBlock::create(schema.clone(), vec![Series::from_data(vec![1u32])]);
        let block2 = DataBlock::create(schema.clone(), vec![Series::from_data(vec![2u32])]);
        let block3 = DataBlock::create(schema.clone(), vec![Series::from_data(vec![3u32])]);
        let blocks = vec![Ok(block), Ok(block2), Ok(block3)];
        let input_stream = futures::stream::iter::<Vec<Result<DataBlock>>>(blocks.clone());
        table
            .append_data(ctx.clone(), Box::pin(input_stream))
            .await?;
    }

    // Check
    {
        let source_plan = table.read_plan(ctx.clone(), None).await?;
        let stream = table.read(ctx.clone(), &source_plan).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        assert_blocks_sorted_eq(
            vec![
                "+----------+--------------+-----------+------------+----------+----------------+---------------------+----------+------------+------------+------------+------------+------------------+-----------+--------+---------+-------------+--------------+---------------+-----------+------------+-------------------+------------+-------------------+-----------------+------------------+-------------+--------------+-----------+--------------+-------------+----------------+----------------+----------------+-------------+----------------+-------+",
                "| log_type | handler_type | tenant_id | cluster_id | sql_user | sql_user_quota | sql_user_privileges | query_id | query_kind | query_text | event_date | event_time | current_database | databases | tables | columns | projections | written_rows | written_bytes | scan_rows | scan_bytes | scan_byte_cost_ms | scan_seeks | scan_seek_cost_ms | scan_partitions | total_partitions | result_rows | result_bytes | cpu_usage | memory_usage | client_info | client_address | exception_code | exception_text | stack_trace | server_version | extra |",
                "+----------+--------------+-----------+------------+----------+----------------+---------------------+----------+------------+------------+------------+------------+------------------+-----------+--------+---------+-------------+--------------+---------------+-----------+------------+-------------------+------------+-------------------+-----------------+------------------+-------------+--------------+-----------+--------------+-------------+----------------+----------------+----------------+-------------+----------------+-------+",
                "| 2        |              |           |            |          |                |                     |          |            |            |            |            |                  |           |        |         |             |              |               |           |            |                   |            |                   |                 |                  |             |              |           |              |             |                |                |                |             |                |       |",
                "| 3        |              |           |            |          |                |                     |          |            |            |            |            |                  |           |        |         |             |              |               |           |            |                   |            |                   |                 |                  |             |              |           |              |             |                |                |                |             |                |       |",
                "+----------+--------------+-----------+------------+----------+----------------+---------------------+----------+------------+------------+------------+------------+------------------+-----------+--------+---------+-------------+--------------+---------------+-----------+------------+-------------------+------------+-------------------+-----------------+------------------+-------------+--------------+-----------+--------------+-------------+----------------+----------------+----------------+-------------+----------------+-------+",
            ],
            &result,
        );
    }

    Ok(())
}
