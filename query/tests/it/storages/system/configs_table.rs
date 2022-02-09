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
use common_exception::Result;
use databend_query::storages::system::ConfigsTable;
use databend_query::storages::Table;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_configs_table() -> Result<()> {
    let conf = crate::tests::ConfigBuilder::create().config();
    let ctx = crate::tests::create_query_context_with_config(conf)?;
    ctx.get_settings().set_max_threads(8)?;

    let table: Arc<dyn Table> = Arc::new(ConfigsTable::create(1));
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);
    assert_eq!(block.num_rows(), 60);

    let expected = vec![
        "+--------------------------------------+------------------+---------+-------------+",
        "| name                                 | value            | group   | description |",
        "+--------------------------------------+------------------+---------+-------------+",
        "| api_tls_server_cert                  |                  | query   |             |",
        "| api_tls_server_key                   |                  | query   |             |",
        "| api_tls_server_root_ca_cert          |                  | query   |             |",
        "| azure_storage_blob.account           |                  | storage |             |",
        "| azure_storage_blob.container         |                  | storage |             |",
        "| azure_storage_blob.master_key        |                  | storage |             |",
        "| clickhouse_handler_host              | 127.0.0.1        | query   |             |",
        "| clickhouse_handler_port              | 9000             | query   |             |",
        "| cluster_id                           |                  | query   |             |",
        "| database_engine_github_enabled       | true             | query   |             |",
        "| disk.data_path                       | _data            | storage |             |",
        "| disk.temp_data_path                  |                  | storage |             |",
        "| flight_api_address                   | 127.0.0.1:9090   | query   |             |",
        "| http_api_address                     | 127.0.0.1:8080   | query   |             |",
        "| http_handler_host                    | 127.0.0.1        | query   |             |",
        "| http_handler_port                    | 8000             | query   |             |",
        "| http_handler_result_timeout_millis   | 10000            | query   |             |",
        "| http_handler_tls_server_cert         |                  | query   |             |",
        "| http_handler_tls_server_key          |                  | query   |             |",
        "| http_handler_tls_server_root_ca_cert |                  | query   |             |",
        "| jwt_key_file                         |                  | query   |             |",
        "| log_dir                              | ./_logs          | log     |             |",
        "| log_level                            | INFO             | log     |             |",
        "| max_active_sessions                  | 256              | query   |             |",
        "| max_query_log_size                   | 10000            | query   |             |",
        "| meta_address                         |                  | meta    |             |",
        "| meta_client_timeout_in_second        | 10               | meta    |             |",
        "| meta_embedded_dir                    | ./_meta_embedded | meta    |             |",
        "| meta_password                        |                  | meta    |             |",
        "| meta_username                        | root             | meta    |             |",
        "| metric_api_address                   | 127.0.0.1:7070   | query   |             |",
        "| mysql_handler_host                   | 127.0.0.1        | query   |             |",
        "| mysql_handler_port                   | 3307             | query   |             |",
        "| num_cpus                             | 8                | query   |             |",
        "| management_mode                      | false            | query   |             |",
        "| rpc_tls_meta_server_root_ca_cert     |                  | meta    |             |",
        "| rpc_tls_meta_service_domain_name     | localhost        | meta    |             |",
        "| rpc_tls_query_server_root_ca_cert    |                  | query   |             |",
        "| rpc_tls_query_service_domain_name    | localhost        | query   |             |",
        "| rpc_tls_server_cert                  |                  | query   |             |",
        "| rpc_tls_server_key                   |                  | query   |             |",
        "| s3.access_key_id                     |                  | storage |             |",
        "| s3.bucket                            |                  | storage |             |",
        "| s3.enable_pod_iam_policy             | false            | storage |             |",
        "| s3.endpoint_url                      |                  | storage |             |",
        "| s3.region                            |                  | storage |             |",
        "| s3.secret_access_key                 |                  | storage |             |",
        "| storage_type                         | disk             | storage |             |",
        "| table_cache_block_meta_count         | 102400           | query   |             |",
        "| table_cache_enabled                  | false            | query   |             |",
        "| table_cache_segment_count            | 10240            | query   |             |",
        "| table_cache_snapshot_count           | 256              | query   |             |",
        "| table_disk_cache_mb_size             | 1024             | query   |             |",
        "| table_disk_cache_root                | _cache           | query   |             |",
        "| table_engine_csv_enabled             | false            | query   |             |",
        "| table_engine_memory_enabled          | true             | query   |             |",
        "| table_engine_parquet_enabled         | false            | query   |             |",
        "| table_memory_cache_mb_size           | 256              | query   |             |",
        "| tenant_id                            | test             | query   |             |",
        "| wait_timeout_mills                   | 5000             | query   |             |",
        "+--------------------------------------+------------------+---------+-------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    Ok(())
}
