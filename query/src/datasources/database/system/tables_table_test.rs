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

use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use futures::TryStreamExt;

use crate::catalogs::Table;
use crate::configs::Config;
use crate::datasources::database::system::TablesTable;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tables_table() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let table = TablesTable::create();
    let source_plan = table.read_plan(
        ctx.clone(),
        &ScanPlan::empty(),
        ctx.get_settings().get_max_threads()? as usize,
    )?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 3);

    let expected = vec![
        "+----------+---------------+--------------------+",
        "| database | name          | engine             |",
        "+----------+---------------+--------------------+",
        "| system   | clusters      | SystemClusters     |",
        "| system   | configs       | SystemConfigs      |",
        "| system   | contributors  | SystemContributors |",
        "| system   | credits       | SystemCredits      |",
        "| system   | databases     | SystemDatabases    |",
        "| system   | engines       | SystemEngines      |",
        "| system   | functions     | SystemFunctions    |",
        "| system   | numbers       | SystemNumbers      |",
        "| system   | numbers_local | SystemNumbersLocal |",
        "| system   | numbers_mt    | SystemNumbersMt    |",
        "| system   | one           | SystemOne          |",
        "| system   | processes     | SystemProcesses    |",
        "| system   | settings      | SystemSettings     |",
        "| system   | tables        | SystemTables       |",
        "| system   | tracing       | SystemTracing      |",
        "+----------+---------------+--------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
