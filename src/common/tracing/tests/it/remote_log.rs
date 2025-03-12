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

use databend_common_exception::Result;
use databend_common_tracing::convert_to_batch;
use databend_common_tracing::RemoteLog;
use databend_common_tracing::RemoteLogElement;
use opendal::services;
use opendal::Operator;

#[test]
fn test_convert_to_batch() {
    let _ = convert_to_batch(get_remote_log()).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_flush() -> Result<()> {
    let builder = services::Memory::default();
    let op = Operator::new(builder)?.finish();

    let path = "test_logs.parquet";

    let result = RemoteLog::do_flush(op.clone(), get_remote_log(), path).await;
    assert!(result.is_ok());

    let exists = op.exists(path).await?;
    assert!(exists);

    Ok(())
}

fn get_remote_log() -> Vec<RemoteLogElement> {
    vec![
        RemoteLogElement {
            timestamp: 1741680446186595,
            path: "test_path".to_string(),
            messages: r#"{"message":"MetaGrpcClient(0.0.0.0:9191)::worker spawned"}"#.to_string(),
            cluster_id: "cluster_id".to_string(),
            node_id: "node_id".to_string(),
            query_id: None,
            log_level: "INFO".to_string(),
        },
        RemoteLogElement {
            timestamp: 1741680446186596,
            path: "test_path2".to_string(),
            messages: r#"{"conf":"RpcClientConf { endpoints: [\"0.0.0.0:9191\"], username: \"root\", password: \"root\", tls_conf: None, timeout: Some(60s), auto_sync_interval: Some(60s), unhealthy_endpoint_evict_time: 120s }","message":"use remote meta"}"#.to_string(),
            cluster_id: "cluster_id".to_string(),
            node_id: "node_id".to_string(),
            query_id: Some("query_id".to_string()),
            log_level: "DEBUG".to_string(),
        },
    ]
}
