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

//! Test kv_get_many() gRPC API.

use std::time::Duration;

use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_runtime_api::TokioRuntime;
use databend_common_meta_types::GrpcHelper;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::protobuf::KvGetManyRequest;
use futures::StreamExt;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::make_grpc_client;

fn make_key_stream(
    keys: Vec<&str>,
) -> impl futures::Stream<Item = KvGetManyRequest> + Send + 'static {
    let owned: Vec<_> = keys
        .into_iter()
        .map(|k| KvGetManyRequest { key: k.to_string() })
        .collect();
    futures::stream::iter(owned)
}

/// Test: KvGetMany on leader returns correct values for streamed keys.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_get_many_on_leader() -> anyhow::Result<()> {
    let (tc, _) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    // Insert test data
    client.upsert_kv(UpsertKV::update("k1", b"v1")).await?;
    client.upsert_kv(UpsertKV::update("k2", b"v2")).await?;
    client.upsert_kv(UpsertKV::update("k3", b"v3")).await?;

    // Test getting existing keys
    {
        let mut ec = client.make_established_client().await?;
        let resp = ec
            .kv_get_many(make_key_stream(vec!["k1", "k2", "k3"]))
            .await?;
        let results: Vec<_> = resp
            .into_inner()
            .filter_map(|r| async { r.ok().map(|i| (i.key, i.value.map(|v| v.data))) })
            .collect()
            .await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], ("k1".to_string(), Some(b"v1".to_vec())));
        assert_eq!(results[1], ("k2".to_string(), Some(b"v2".to_vec())));
        assert_eq!(results[2], ("k3".to_string(), Some(b"v3".to_vec())));
    }

    // Test getting mix of existing and non-existing keys
    {
        let mut ec = client.make_established_client().await?;
        let resp = ec
            .kv_get_many(make_key_stream(vec!["k1", "nonexistent", "k3"]))
            .await?;
        let results: Vec<_> = resp
            .into_inner()
            .filter_map(|r| async { r.ok().map(|i| (i.key, i.value.map(|v| v.data))) })
            .collect()
            .await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], ("k1".to_string(), Some(b"v1".to_vec())));
        assert_eq!(results[1], ("nonexistent".to_string(), None));
        assert_eq!(results[2], ("k3".to_string(), Some(b"v3".to_vec())));
    }

    // Test empty input
    {
        let mut ec = client.make_established_client().await?;
        let resp = ec.kv_get_many(make_key_stream(vec![])).await?;
        let results: Vec<_> = resp
            .into_inner()
            .filter_map(|r| async { r.ok().map(|i| (i.key, i.value.map(|v| v.data))) })
            .collect()
            .await;

        assert!(results.is_empty());
    }

    Ok(())
}

/// Test: KvGetMany on follower returns error with leader endpoint.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_get_many_on_follower_returns_leader() -> anyhow::Result<()> {
    let tcs = crate::tests::start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2]).await?;

    let leader_addr = &tcs[0].config.grpc.api_address().unwrap();
    let follower_addr = &tcs[1].config.grpc.api_address().unwrap();

    let client = make_grpc_client::<TokioRuntime>(vec![follower_addr.clone()])?;
    let mut ec = client.make_established_client().await?;

    let status = ec
        .kv_get_many(make_key_stream(vec!["k1"]))
        .await
        .unwrap_err();

    assert_eq!(tonic::Code::Unavailable, status.code());
    assert_eq!(
        leader_addr,
        &GrpcHelper::parse_leader_from_metadata(status.metadata())
            .unwrap()
            .to_string()
    );

    Ok(())
}

/// Test: KvGetMany on single node (no quorum) returns error.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_get_many_no_quorum_no_leader() -> anyhow::Result<()> {
    let mut tcs = crate::tests::start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2]).await?;

    let remaining_addr = tcs[2].config.grpc.api_address().unwrap().clone();

    tcs[0].grpc_srv.take().unwrap().do_stop(None).await;
    tcs[1].grpc_srv.take().unwrap().do_stop(None).await;

    // Wait for quorum loss detection
    tokio::time::sleep(Duration::from_secs(10)).await;

    let client = make_grpc_client::<TokioRuntime>(vec![remaining_addr])?;
    let mut ec = client.make_established_client().await?;

    let status = ec
        .kv_get_many(make_key_stream(vec!["k1"]))
        .await
        .unwrap_err();

    // Unavailable (not leader) or Cancelled (timeout) - both acceptable without quorum
    assert!(
        status.code() == tonic::Code::Unavailable || status.code() == tonic::Code::Cancelled,
        "expected Unavailable or Cancelled, got: {:?}",
        status.code()
    );

    Ok(())
}
