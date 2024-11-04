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

//! Test metasrv SchemaApi by writing to one node and then reading from another,
//! on a restarted cluster.

use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::Stoppable;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MetaClientError;
use log::info;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::start_metasrv_cluster;

/// Ensure grpc-client will retry when failing to connect.
///
/// - Start a cluster of 3.
/// - Shutdown node 1.
/// - Test upsert kv, expect the client auto choose the running nodes.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_metasrv_connection_error() -> anyhow::Result<()> {
    info!("--- Start cluster 0,1,2");
    let mut tcs = start_metasrv_cluster(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc_api_address.clone())
        .collect::<Vec<_>>();

    let a0 = || addresses[0].clone();
    let a1 = || addresses[1].clone();
    let a2 = || addresses[2].clone();

    let mut stopped = tcs.remove(1);
    { stopped }.grpc_srv.take().unwrap().stop(None).await?;

    for addrs in [
        vec![a0(), a1(), a2()], // a1() is down
        vec![a1(), a2()],       // a1() is down
        vec![a0(), a1()],       // a1() is down
        vec![a0()],
        vec![a2()],
    ] {
        let addrs_str = addrs.join(",").to_string();
        info!("--- test write with api addresses: {}", addrs_str);

        let client = make_client(addrs.to_vec())?;
        test_write_read(&client, &format!("grpc-conn-error-{}", addrs_str)).await?;
    }

    info!("--- using only one crashed node won't work");
    {
        let client = make_client(vec![a1()])?;
        let res = test_write_read(&client, "crashed-node-1").await;
        assert!(res.is_err());
    }
    Ok(())
}

/// Ensure grpc-client will retry when failing to connect, too.
///
/// - Start a cluster of 3.
/// - Create a client to node 1 and 2.
/// - Shutdown follower node 1.
/// - Test upsert kv, expect the client to auto choose the running nodes.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_metasrv_one_client_follower_down() -> anyhow::Result<()> {
    info!("--- Start cluster 0,1,2");
    let mut tcs = start_metasrv_cluster(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc_api_address.clone())
        .collect::<Vec<_>>();

    let a1 = || addresses[1].clone();
    let a2 = || addresses[2].clone();

    // a1() will be shut down
    let client = make_client(vec![a1(), a2()])?;

    test_write_read(&client, "conn-error-one-client-node-1-running").await?;

    let mut stopped = tcs.remove(1);
    { stopped }.grpc_srv.take().unwrap().stop(None).await?;

    test_write_read(&client, "conn-error-one-client-node-1-down").await?;

    Ok(())
}

/// Ensure internal forward RPC will retry when failing to connect to the leader.
///
/// - Start a cluster of 3.
/// - Create a client to node 1 and 2.
/// - Shutdown leader node 0.
/// - Test upsert kv, expect the client to auto choose the running nodes.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_metasrv_one_client_leader_down() -> anyhow::Result<()> {
    info!("--- Start cluster 0,1,2");
    let mut tcs = start_metasrv_cluster(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc_api_address.clone())
        .collect::<Vec<_>>();

    let a1 = || addresses[1].clone();
    let a2 = || addresses[2].clone();

    // a0() will be shut down
    let client = make_client(vec![a1(), a2()])?;

    test_write_read(&client, "conn-error-one-client-node-0-running").await?;

    let mut stopped = tcs.remove(0);
    { stopped }.grpc_srv.take().unwrap().stop(None).await?;

    // Write/read operations will recover functioning after a new leader is elected.
    test_write_read(&client, "conn-error-one-client-node-0-down").await?;

    Ok(())
}

fn make_client(addresses: Vec<String>) -> Result<Arc<ClientHandle>, MetaClientError> {
    let client = MetaGrpcClient::try_create(
        addresses, // a1() will be shut down
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    Ok(client)
}

/// Test write and then read with a provided client
async fn test_write_read(client: &Arc<ClientHandle>, key: impl Display) -> anyhow::Result<()> {
    info!("--- test write/read: {}", key);

    let k = key.to_string();
    let res = client.upsert_kv(UpsertKVReq::update(&k, &b(&k))).await?;

    info!("--- upsert {} res: {:?}", k, res);

    let res = client.get_kv(&k).await?;
    let res = res.unwrap();

    assert_eq!(k.into_bytes(), res.data);

    Ok(())
}

fn b(s: impl ToString) -> Vec<u8> {
    s.to_string().into_bytes()
}
