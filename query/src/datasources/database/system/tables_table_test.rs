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
    assert_eq!(block.num_columns(), 3);

    let expected = vec![
        "+----------+--------------+--------------------+",
        "| database | name         | engine             |",
        "+----------+--------------+--------------------+",
        "| system   | clusters     | SystemClusters     |",
        "| system   | columns      | SystemColumns      |",
        "| system   | configs      | SystemConfigs      |",
        "| system   | contributors | SystemContributors |",
        "| system   | credits      | SystemCredits      |",
        "| system   | databases    | SystemDatabases    |",
        "| system   | functions    | SystemFunctions    |",
        "| system   | metrics      | SystemMetrics      |",
        "| system   | one          | SystemOne          |",
        "| system   | processes    | SystemProcesses    |",
        "| system   | settings     | SystemSettings     |",
        "| system   | tables       | SystemTables       |",
        "| system   | tracing      | SystemTracing      |",
        "+----------+--------------+--------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
