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

//! Test kv_list() gRPC API.

use std::time::Duration;

use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_runtime_api::TokioRuntime;
use databend_common_meta_types::GrpcHelper;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::protobuf::KvListRequest;
use futures::StreamExt;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::make_grpc_client;

fn req(prefix: &str, limit: Option<u64>) -> KvListRequest {
    KvListRequest {
        prefix: prefix.to_string(),
        limit,
    }
}

/// Test: KvList on leader returns stream and respects limit.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_list_on_leader() -> anyhow::Result<()> {
    let (tc, _) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("test/a", b"va")).await?;
    client.upsert_kv(UpsertKV::update("test/b", b"vb")).await?;
    client.upsert_kv(UpsertKV::update("test/c", b"vc")).await?;
    client.upsert_kv(UpsertKV::update("other/x", b"vx")).await?;

    let list = |prefix, limit| {
        let client = client.clone();
        async move {
            let mut ec = client.make_established_client().await.unwrap();
            let resp = ec.kv_list(req(prefix, limit)).await.unwrap();
            resp.into_inner()
                .filter_map(|r| async { r.ok().map(|i| i.key) })
                .collect::<Vec<_>>()
                .await
        }
    };

    assert_eq!(
        vec!["test/a", "test/b", "test/c"],
        list("test/", None).await
    );
    assert_eq!(vec!["test/a", "test/b"], list("test/", Some(2)).await);
    assert_eq!(Vec::<String>::new(), list("nonexistent/", None).await);

    Ok(())
}

/// Test: KvList on follower returns error with leader endpoint.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_list_on_follower_returns_leader() -> anyhow::Result<()> {
    let tcs = crate::tests::start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2]).await?;

    let leader_addr = tcs[0].config.grpc.api_address().unwrap();
    let follower_addr = tcs[1].config.grpc.api_address().unwrap();

    let client = make_grpc_client::<TokioRuntime>(vec![follower_addr.clone()])?;
    let mut ec = client.make_established_client().await?;

    let status = ec.kv_list(req("test/", None)).await.unwrap_err();

    assert_eq!(tonic::Code::Unavailable, status.code());
    assert_eq!(
        &leader_addr,
        &GrpcHelper::parse_leader_from_metadata(status.metadata())
            .unwrap()
            .to_string()
    );

    Ok(())
}

/// Test: KvList on single node (no quorum) returns error.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_list_no_quorum_no_leader() -> anyhow::Result<()> {
    let mut tcs = crate::tests::start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2]).await?;

    let remaining_addr = tcs[2].config.grpc.api_address().unwrap();

    tcs[0].grpc_srv.take().unwrap().do_stop(None).await;
    tcs[1].grpc_srv.take().unwrap().do_stop(None).await;

    // Wait for quorum loss detection
    tokio::time::sleep(Duration::from_secs(10)).await;

    let client = make_grpc_client::<TokioRuntime>(vec![remaining_addr])?;
    let mut ec = client.make_established_client().await?;

    let status = ec.kv_list(req("test/", None)).await.unwrap_err();

    // Unavailable (not leader) or Cancelled (timeout) - both acceptable without quorum
    assert!(
        status.code() == tonic::Code::Unavailable || status.code() == tonic::Code::Cancelled,
        "expected Unavailable or Cancelled, got: {:?}",
        status.code()
    );

    Ok(())
}
