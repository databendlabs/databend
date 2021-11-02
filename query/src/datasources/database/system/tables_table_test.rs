// Copyright 2020 Datafuse Labs.
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
use common_exception::Result;
use futures::TryStreamExt;

use crate::catalogs::Table;
use crate::catalogs::ToReadDataSourcePlan;
use crate::datasources::database::system::TablesTable;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tables_table() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let table: Arc<dyn Table> = Arc::new(TablesTable::create(1));
    let io_ctx = ctx.get_single_node_table_io_context()?;
    let io_ctx = Arc::new(io_ctx);
    let source_plan = table.read_plan(
        io_ctx.clone(),
        None,
        Some(ctx.get_settings().get_max_threads()? as usize),
    )?;

    let stream = table.read(io_ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);

    let expected = vec![
    "+----------+--------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
    "| database | name         | engine             | schema                                                                                                                                                                                                                                                                                                                                                                                                    |",
    "+----------+--------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
    "| system   | clusters     | SystemClusters     | [{\"name\":\"name\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"host\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"port\",\"data_type\":\"UInt16\",\"nullable\":false}]                                                                                                                                                                                                                                       |",
    "| system   | configs      | SystemConfigs      | [{\"name\":\"name\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"value\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"group\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"description\",\"data_type\":\"String\",\"nullable\":false}]                                                                                                                                                                        |",
    "| system   | contributors | SystemContributors | [{\"name\":\"name\",\"data_type\":\"String\",\"nullable\":false}]                                                                                                                                                                                                                                                                                                                                                   |",
    "| system   | credits      | SystemCredits      | [{\"name\":\"name\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"version\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"license\",\"data_type\":\"String\",\"nullable\":false}]                                                                                                                                                                                                                                 |",
    "| system   | databases    | SystemDatabases    | [{\"name\":\"name\",\"data_type\":\"String\",\"nullable\":false}]                                                                                                                                                                                                                                                                                                                                                   |",
    "| system   | functions    | SystemFunctions    | [{\"name\":\"name\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"is_aggregate\",\"data_type\":\"Boolean\",\"nullable\":false}]                                                                                                                                                                                                                                                                                    |",
    "| system   | metrics      | SystemMetrics      | [{\"name\":\"metric\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"kind\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"labels\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"value\",\"data_type\":\"String\",\"nullable\":false}]                                                                                                                                                                            |",
    "| system   | one          | SystemOne          | [{\"name\":\"dummy\",\"data_type\":\"UInt8\",\"nullable\":false}]                                                                                                                                                                                                                                                                                                                                                   |",
    "| system   | processes    | SystemProcesses    | [{\"name\":\"id\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"type\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"host\",\"data_type\":\"String\",\"nullable\":true},{\"name\":\"state\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"database\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"extra_info\",\"data_type\":\"String\",\"nullable\":true},{\"name\":\"memory_usage\",\"data_type\":\"UInt64\",\"nullable\":true}] |",
    "| system   | settings     | SystemSettings     | [{\"name\":\"name\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"value\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"default_value\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"description\",\"data_type\":\"String\",\"nullable\":false}]                                                                                                                                                                |",
    "| system   | tables       | SystemTables       | [{\"name\":\"database\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"name\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"engine\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"schema\",\"data_type\":\"String\",\"nullable\":false}]                                                                                                                                                                         |",
    "| system   | tracing      | SystemTracing      | [{\"name\":\"v\",\"data_type\":\"Int64\",\"nullable\":false},{\"name\":\"name\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"msg\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"level\",\"data_type\":\"Int8\",\"nullable\":false},{\"name\":\"hostname\",\"data_type\":\"String\",\"nullable\":false},{\"name\":\"pid\",\"data_type\":\"Int64\",\"nullable\":false},{\"name\":\"time\",\"data_type\":\"String\",\"nullable\":false}]                   |",
    "+----------+--------------+--------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
