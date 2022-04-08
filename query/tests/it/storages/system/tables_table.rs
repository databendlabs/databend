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

use common_base::tokio;
use common_exception::Result;
use databend_query::storages::system::TablesTable;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tables_table() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;
    let table = TablesTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);

    let expected = vec![
        "+--------------------+--------------+--------------------+-------------------------------+",
        "| database           | name         | engine             | created_on                    |",
        "+--------------------+--------------+--------------------+-------------------------------+",
        "| INFORMATION_SCHEMA | COLUMNS      | VIEW               | 2022-04-08 09:01:34.140 +0000 |",
        "| INFORMATION_SCHEMA | KEYWORDS     | VIEW               | 2022-04-08 09:01:34.140 +0000 |",
        "| INFORMATION_SCHEMA | SCHEMATA     | VIEW               | 2022-04-08 09:01:34.140 +0000 |",
        "| INFORMATION_SCHEMA | TABLES       | VIEW               | 2022-04-08 09:01:34.140 +0000 |",
        "| INFORMATION_SCHEMA | VIEWS        | VIEW               | 2022-04-08 09:01:34.140 +0000 |",
        "| information_schema | columns      | VIEW               | 2022-04-08 09:01:34.140 +0000 |",
        "| information_schema | keywords     | VIEW               | 2022-04-08 09:01:34.140 +0000 |",
        "| information_schema | schemata     | VIEW               | 2022-04-08 09:01:34.140 +0000 |",
        "| information_schema | tables       | VIEW               | 2022-04-08 09:01:34.140 +0000 |",
        "| information_schema | views        | VIEW               | 2022-04-08 09:01:34.140 +0000 |",
        "| system             | clusters     | SystemClusters     | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | columns      | SystemColumns      | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | configs      | SystemConfigs      | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | contributors | SystemContributors | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | credits      | SystemCredits      | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | databases    | SystemDatabases    | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | engines      | SystemEngines      | 2022-04-08 09:01:34.140 +0000 |",
        "| system             | functions    | SystemFunctions    | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | metrics      | SystemMetrics      | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | one          | SystemOne          | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | processes    | SystemProcesses    | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | query_log    | SystemQueryLog     | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | roles        | SystemRoles        | 2022-04-08 09:01:34.140 +0000 |",
        "| system             | settings     | SystemSettings     | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | tables       | SystemTables       | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | tracing      | SystemTracing      | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | users        | SystemUsers        | 2022-04-08 09:01:34.136 +0000 |",
        "| system             | warehouses   | SystemWarehouses   | 2022-04-08 09:01:34.136 +0000 |",
        "+--------------------+--------------+--------------------+-------------------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq_with_regex(expected, result.as_slice());

    Ok(())
}
