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

use std::time::Duration;

use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_runtime_api::TokioRuntime;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::protobuf as pb;
use log::info;
use pretty_assertions::assert_eq;
use test_harness::test;
use tokio::time::sleep;
use tokio_stream::StreamExt;

use crate::testing::meta_service_test_harness;

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_snapshot_keys_layout() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;

    // Small delay to let service fully initialize
    sleep(Duration::from_millis(500)).await;

    let client = tc.grpc_client().await?;

    info!("--- Setup hierarchical test data");
    let test_keys = [
        "databases/1/tables/users/columns/id",
        "databases/1/tables/users/columns/name",
        "databases/1/tables/users/columns/email",
        "databases/1/tables/orders/columns/id",
        "databases/1/tables/orders/columns/user_id",
        "databases/2/tables/products/columns/id",
        "databases/2/tables/products/columns/name",
        "system/config/max_connections",
        "system/stats/query_count",
    ];

    for key in test_keys {
        client.upsert_kv(UpsertKV::update(key, &b(key))).await?;
    }

    let meta_handle = tc
        .grpc_srv
        .as_ref()
        .map(|grpc_server| grpc_server.get_meta_handle())
        .unwrap();

    meta_handle.handle_trigger_snapshot().await??;

    // Wait for snapshot to be ready
    sleep(Duration::from_secs(2)).await;

    let mut grpc_client = client.make_established_client().await?;

    // Test 1: No depth limit - should return all hierarchical levels
    info!("--- Test no depth limit");
    {
        let response = grpc_client
            .snapshot_keys_layout(tonic::Request::new(pb::KeysLayoutRequest { depth: None }))
            .await?;

        let mut stream = response.into_inner();
        let mut results = vec![];
        while let Some(keys_count) = stream.next().await {
            results.push(keys_count?.to_string());
        }

        // Should include all levels with their counts (ordered by algorithm output)
        let want = [
            "databases/1/tables/orders/columns 2",
            "databases/1/tables/orders 2",
            "databases/1/tables/users/columns 3",
            "databases/1/tables/users 3",
            "databases/1/tables 5",
            "databases/1 5",
            "databases/2/tables/products/columns 2",
            "databases/2/tables/products 2",
            "databases/2/tables 2",
            "databases/2 2",
            "databases 7",
            "system/config 1",
            "system/stats 1",
            "system 2",
            " 9",
        ];

        assert_eq!(results, want);
    }

    // Test 2: Depth limit = 1 - should only return top-level prefixes
    info!("--- Test depth limit = 1");
    {
        let response = grpc_client
            .snapshot_keys_layout(tonic::Request::new(pb::KeysLayoutRequest {
                depth: Some(1),
            }))
            .await?;

        let mut stream = response.into_inner();
        let mut results = vec![];
        while let Some(keys_count) = stream.next().await {
            results.push(keys_count?.to_string());
        }

        let want = ["databases 7", "system 2", " 9"];

        assert_eq!(results, want);
    }

    // Test 3: Depth limit = 2 - should only return top-level prefixes
    info!("--- Test depth limit = 2");
    {
        let response = grpc_client
            .snapshot_keys_layout(tonic::Request::new(pb::KeysLayoutRequest {
                depth: Some(2),
            }))
            .await?;

        let mut stream = response.into_inner();
        let mut results = vec![];
        while let Some(keys_count) = stream.next().await {
            results.push(keys_count?.to_string());
        }

        let want = [
            "databases/1 5",
            "databases/2 2",
            "databases 7",
            "system/config 1",
            "system/stats 1",
            "system 2",
            " 9",
        ];

        assert_eq!(results, want);
    }

    Ok(())
}

fn b(s: impl ToString) -> Vec<u8> {
    s.to_string().into_bytes()
}
