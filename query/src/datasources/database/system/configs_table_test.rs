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

use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::catalogs::Table;
use crate::clusters::ClusterDiscovery;
use crate::configs::Config;
use crate::datasources::database::system::ConfigsTable;
use crate::sessions::SessionManager;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_configs_table() -> Result<()> {
    let config = Config::default();
    let cluster = ClusterDiscovery::empty();

    let sessions = SessionManager::from_conf(config, cluster)?;
    let test_session = sessions.create_session("TestSession")?;
    let ctx = test_session.create_context();
    ctx.get_settings().set_max_threads(8)?;

    let table = ConfigsTable::create();
    let source_plan = table.read_plan(
        ctx.clone(),
        &ScanPlan::empty(),
        ctx.get_settings().get_max_threads()? as usize,
    )?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);
    assert_eq!(block.num_rows(), 31);

    let expected = vec![
        "+-----------------------------------+----------------+-------+-------------+",
        "| name                              | value          | group | description |",
        "+-----------------------------------+----------------+-------+-------------+",
        "| api_tls_server_cert               |                | query |             |",
        "| api_tls_server_key                |                | query |             |",
        "| api_tls_server_root_ca_cert       |                | query |             |",
        "| clickhouse_handler_host           | 127.0.0.1      | query |             |",
        "| clickhouse_handler_port           | 9000           | query |             |",
        "| disable_local_database_engine     | 0              | query |             |",
        "| flight_api_address                | 127.0.0.1:9090 | query |             |",
        "| http_api_address                  | 127.0.0.1:8080 | query |             |",
        "| log_dir                           | ./_logs        | log   |             |",
        "| log_level                         | INFO           | log   |             |",
        "| max_active_sessions               | 256            | query |             |",
        "| meta_address                      |                | meta  |             |",
        "| meta_password                     |                | meta  |             |",
        "| meta_username                     | root           | meta  |             |",
        "| metric_api_address                | 127.0.0.1:7070 | query |             |",
        "| mysql_handler_host                | 127.0.0.1      | query |             |",
        "| mysql_handler_port                | 3307           | query |             |",
        "| namespace                         |                | query |             |",
        "| num_cpus                          | 8              | query |             |",
        "| rpc_tls_meta_server_root_ca_cert  |                | meta  |             |",
        "| rpc_tls_meta_service_domain_name  | localhost      | meta  |             |",
        "| rpc_tls_query_server_root_ca_cert |                | query |             |",
        "| rpc_tls_query_service_domain_name | localhost      | query |             |",
        "| rpc_tls_server_cert               |                | query |             |",
        "| rpc_tls_server_key                |                | query |             |",
        "| rpc_tls_store_server_root_ca_cert |                | store |             |",
        "| rpc_tls_store_service_domain_name | localhost      | store |             |",
        "| store_address                     |                | store |             |",
        "| store_password                    |                | store |             |",
        "| store_username                    | root           | store |             |",
        "| tenant                            |                | query |             |",
        "+-----------------------------------+----------------+-------+-------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    Ok(())
}
