// Copyright 2022 Datafuse Labs.
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

use databend_meta::api::http::v1::metrics::metrics_handler;
use databend_meta::metrics::network_metrics;
use databend_meta::metrics::raft_metrics;
use databend_meta::metrics::server_metrics;
use http::Method;
use http::StatusCode;
use http::Uri;
use log::info;
use maplit::btreeset;
use poem::get;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Request;
use poem::Route;
use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::start_metasrv_cluster;

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_metrics() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster(&[0, 1, 2]).await?;

    let leader = tcs[0]
        .grpc_srv
        .as_ref()
        .unwrap()
        .meta_handle
        .clone()
        .unwrap();

    // record some metrics to make the registry get initialized
    server_metrics::incr_leader_change();
    network_metrics::incr_recv_bytes(1);
    network_metrics::sample_rpc_read_delay(std::time::Duration::from_millis(10));
    network_metrics::sample_rpc_write_delay(std::time::Duration::from_millis(20));
    raft_metrics::network::incr_recvfrom_bytes("addr".to_string(), 1);
    raft_metrics::storage::incr_raft_storage_fail("fun", true);

    let cluster_router = Route::new()
        .at("/v1/metrics", get(metrics_handler))
        .data(leader);

    let mut response = cluster_router
        .call(
            Request::builder()
                .uri(Uri::from_static("/v1/metrics"))
                .method(Method::GET)
                .finish(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Sample output:
    //
    // metasrv_meta_network_recv_bytes_total 1
    // metasrv_meta_network_req_failed_total 0
    // metasrv_meta_network_req_inflights 0
    // metasrv_meta_network_req_success_total 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="+Inf"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="1.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="10.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="100.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="1000.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="10000.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="2.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="20.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="200.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="2000.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="30000.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="5.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="50.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="500.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="5000.0"} 0
    // metasrv_meta_network_rpc_delay_ms_bucket{le="60000.0"} 0
    // metasrv_meta_network_rpc_delay_ms_count 0
    // metasrv_meta_network_rpc_delay_ms_sum 0.0
    // metasrv_meta_network_sent_bytes_total 0
    // metasrv_meta_network_stream_get_item_sent_total 0
    // metasrv_meta_network_stream_list_item_sent_total 0
    // metasrv_meta_network_stream_mget_item_sent_total 0
    // metasrv_meta_network_watch_change_total 0
    // metasrv_meta_network_watch_initialization_total 0
    // metasrv_raft_network_active_peers{id="1",addr="127.0.0.1:29003"} 1
    // metasrv_raft_network_active_peers{id="2",addr="127.0.0.1:29006"} 1
    // metasrv_raft_network_append_sent_seconds_bucket{le="+Inf",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="+Inf",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.001",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.001",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.002",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.002",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.004",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.004",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.008",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.008",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.016",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.016",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.032",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.032",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.064",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.064",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.128",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.128",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.256",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.256",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.512",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="0.512",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="1.024",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="1.024",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="131.072",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="131.072",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="16.384",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="16.384",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="2.048",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="2.048",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="262.144",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="262.144",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="32.768",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="32.768",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="4.096",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="4.096",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="524.288",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="524.288",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="65.536",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="65.536",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_bucket{le="8.192",to="1"} 9
    // metasrv_raft_network_append_sent_seconds_bucket{le="8.192",to="2"} 6
    // metasrv_raft_network_append_sent_seconds_count{to="1"} 9
    // metasrv_raft_network_append_sent_seconds_count{to="2"} 6
    // metasrv_raft_network_append_sent_seconds_sum{to="1"} 0.0
    // metasrv_raft_network_append_sent_seconds_sum{to="2"} 0.0
    // metasrv_raft_network_recv_bytes_total{from="127.0.0.1:62962"} 1794
    // metasrv_raft_network_recv_bytes_total{from="127.0.0.1:62964"} 797
    // metasrv_raft_network_recv_bytes_total{from="127.0.0.1:62965"} 1728
    // metasrv_raft_network_recv_bytes_total{from="127.0.0.1:62967"} 537
    // metasrv_raft_network_recv_bytes_total{from="127.0.0.1:62968"} 537
    // metasrv_raft_network_recv_bytes_total{from="127.0.0.1:62969"} 533
    // metasrv_raft_network_recv_bytes_total{from="127.0.0.1:62970"} 673
    // metasrv_raft_network_recv_bytes_total{from="addr"} 1
    // metasrv_raft_network_sent_bytes_total{to="1"} 3661
    // metasrv_raft_network_sent_bytes_total{to="2"} 2938
    // metasrv_raft_storage_raft_store_write_failed_total{func="fun"} 1
    // metasrv_raft_storage_snapshot_building 0
    // metasrv_raft_storage_snapshot_written_entries_total 0
    // metasrv_server_applying_snapshot 0
    // metasrv_server_current_leader_id 0
    // metasrv_server_current_term 1
    // metasrv_server_is_leader 1
    // metasrv_server_last_log_index 9
    // metasrv_server_last_seq 0
    // metasrv_server_leader_changes_total 4
    // metasrv_server_node_is_health 1
    // metasrv_server_proposals_applied 9
    // metasrv_server_proposals_failed_total 0
    // metasrv_server_proposals_pending 0
    // metasrv_server_raft_log_cache_items 10
    // metasrv_server_raft_log_cache_used_size 771
    // metasrv_server_raft_log_size 1694
    // metasrv_server_raft_log_wal_closed_chunk_count 0
    // metasrv_server_raft_log_wal_closed_chunk_total_size 0
    // metasrv_server_raft_log_wal_offset 1694
    // metasrv_server_raft_log_wal_open_chunk_size 1694
    // metasrv_server_read_failed_total 0
    // metasrv_server_snapshot_avg_block_size 0
    // metasrv_server_snapshot_avg_keys_per_block 0
    // metasrv_server_snapshot_block_count 0
    // metasrv_server_snapshot_data_size 0
    // metasrv_server_snapshot_expire_index_count 0
    // metasrv_server_snapshot_index_size 0
    // metasrv_server_snapshot_key_count 0
    // metasrv_server_snapshot_primary_index_count 0
    // metasrv_server_snapshot_read_block 0
    // metasrv_server_snapshot_read_block_from_cache 0
    // metasrv_server_snapshot_read_block_from_disk 0
    // metasrv_server_watchers 0

    let b = response.take_body();
    let txt = b.into_string().await?;
    info!("metrics response text: {}", txt);
    println!("metrics response text: {}", txt);

    let metric_keys = {
        let lines = txt.split('\n');
        let mut metric_keys = btreeset! {};

        for line in lines {
            if line.starts_with('#') {
                continue;
            }
            if line.is_empty() {
                continue;
            }

            let mut segments = line.split(' ');
            let key = segments.next().unwrap();
            metric_keys.insert(key);
            info!("found response metric key: {:?}", key);

            // strip labels `foo{label=1, label=2}`
            let mut key_and_labels = key.split('{');
            if let Some(striped) = key_and_labels.next() {
                metric_keys.insert(striped);
                info!(
                    "found response metric key with label striped: {:?}",
                    striped
                );
            }
        }
        metric_keys
    };

    // Only static keys are checked.

    // Server metrics
    assert!(metric_keys.contains("metasrv_server_leader_changes_total"));
    assert!(metric_keys.contains("metasrv_server_last_log_index"));
    assert!(metric_keys.contains("metasrv_server_proposals_pending"));
    assert!(metric_keys.contains("metasrv_server_is_leader"));
    assert!(metric_keys.contains("metasrv_server_node_is_health"));
    assert!(metric_keys.contains("metasrv_server_last_seq"));
    assert!(metric_keys.contains("metasrv_server_proposals_applied"));
    assert!(metric_keys.contains("metasrv_server_current_leader_id"));
    assert!(metric_keys.contains("metasrv_server_current_term"));
    assert!(metric_keys.contains("metasrv_server_applying_snapshot"));
    assert!(metric_keys.contains("metasrv_server_proposals_failed_total"));
    assert!(metric_keys.contains("metasrv_server_read_failed_total"));
    assert!(metric_keys.contains("metasrv_server_watchers"));

    // Server raft log metrics
    assert!(metric_keys.contains("metasrv_server_raft_log_cache_items"));
    assert!(metric_keys.contains("metasrv_server_raft_log_cache_used_size"));
    assert!(metric_keys.contains("metasrv_server_raft_log_size"));
    assert!(metric_keys.contains("metasrv_server_raft_log_wal_closed_chunk_count"));
    assert!(metric_keys.contains("metasrv_server_raft_log_wal_closed_chunk_total_size"));
    assert!(metric_keys.contains("metasrv_server_raft_log_wal_offset"));
    assert!(metric_keys.contains("metasrv_server_raft_log_wal_open_chunk_size"));

    // Server snapshot metrics
    assert!(metric_keys.contains("metasrv_server_snapshot_key_count"));
    assert!(metric_keys.contains("metasrv_server_snapshot_primary_index_count"));
    assert!(metric_keys.contains("metasrv_server_snapshot_expire_index_count"));

    // Server snapshot internal metrics
    assert!(metric_keys.contains("metasrv_server_snapshot_block_count"));
    assert!(metric_keys.contains("metasrv_server_snapshot_data_size"));
    assert!(metric_keys.contains("metasrv_server_snapshot_index_size"));
    assert!(metric_keys.contains("metasrv_server_snapshot_avg_block_size"));
    assert!(metric_keys.contains("metasrv_server_snapshot_avg_keys_per_block"));
    assert!(metric_keys.contains("metasrv_server_snapshot_read_block"));
    assert!(metric_keys.contains("metasrv_server_snapshot_read_block_from_cache"));
    assert!(metric_keys.contains("metasrv_server_snapshot_read_block_from_disk"));

    // Meta network metrics
    assert!(metric_keys.contains("metasrv_meta_network_recv_bytes_total"));
    assert!(metric_keys.contains("metasrv_meta_network_req_failed_total"));
    assert!(metric_keys.contains("metasrv_meta_network_req_inflights"));
    assert!(metric_keys.contains("metasrv_meta_network_req_success_total"));
    assert!(metric_keys.contains("metasrv_meta_network_sent_bytes_total"));

    // Meta network RPC delay metrics (milliseconds)
    assert!(metric_keys.contains("metasrv_meta_network_rpc_delay_ms_bucket"));
    assert!(metric_keys.contains("metasrv_meta_network_rpc_delay_ms_count"));
    assert!(metric_keys.contains("metasrv_meta_network_rpc_delay_ms_sum"));

    // Meta network RPC read delay metrics (milliseconds)
    assert!(metric_keys.contains("metasrv_meta_network_rpc_delay_read_ms_bucket"));
    assert!(metric_keys.contains("metasrv_meta_network_rpc_delay_read_ms_count"));
    assert!(metric_keys.contains("metasrv_meta_network_rpc_delay_read_ms_sum"));

    // Meta network RPC write delay metrics (milliseconds)
    assert!(metric_keys.contains("metasrv_meta_network_rpc_delay_write_ms_bucket"));
    assert!(metric_keys.contains("metasrv_meta_network_rpc_delay_write_ms_count"));
    assert!(metric_keys.contains("metasrv_meta_network_rpc_delay_write_ms_sum"));

    // Raft network metrics
    assert!(metric_keys.contains("metasrv_raft_network_active_peers"));
    assert!(metric_keys.contains("metasrv_raft_network_recv_bytes_total"));
    assert!(metric_keys.contains("metasrv_raft_network_sent_bytes_total"));

    // Raft network append sent metrics
    assert!(metric_keys.contains("metasrv_raft_network_append_sent_seconds_bucket"));
    assert!(metric_keys.contains("metasrv_raft_network_append_sent_seconds_count"));
    assert!(metric_keys.contains("metasrv_raft_network_append_sent_seconds_sum"));

    // Raft storage metrics
    assert!(metric_keys.contains("metasrv_raft_storage_raft_store_write_failed_total"));
    assert!(metric_keys.contains("metasrv_raft_storage_snapshot_building"));
    assert!(metric_keys.contains("metasrv_raft_storage_snapshot_written_entries_total"));

    // Watch
    assert!(metric_keys.contains("metasrv_meta_network_watch_initialization_total"));
    assert!(metric_keys.contains("metasrv_meta_network_watch_change_total"));

    // Stream metrics
    assert!(metric_keys.contains("metasrv_meta_network_stream_get_item_sent_total"));
    assert!(metric_keys.contains("metasrv_meta_network_stream_mget_item_sent_total"));
    assert!(metric_keys.contains("metasrv_meta_network_stream_list_item_sent_total"));

    Ok(())
}
