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

use std::time::Duration;

use databend_common_base::base::Stoppable;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use log::info;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::start_metasrv_cluster;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::start_metasrv_with_context;

/// - Start a cluster of 3.
/// - Test upsert kv and read on different nodes.
/// - Stop and restart the cluster.
/// - Test upsert kv and read on different nodes.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_api_restart_cluster_write_read() -> anyhow::Result<()> {
    fn make_key(tc: &MetaSrvTestContext, k: impl std::fmt::Display) -> String {
        let x = &tc.config.raft_config;
        format!("t-restart-cluster-{}-{}-{}", x.config_id, x.id, k)
    }

    async fn test_write_read_on_every_node(
        tcs: &[MetaSrvTestContext],
        key_suffix: &str,
    ) -> anyhow::Result<()> {
        info!("--- test write on every node: {}", key_suffix);

        for tc in tcs.iter() {
            let client = tc.grpc_client().await?;

            let k = make_key(tc, key_suffix);
            let res = client.upsert_kv(UpsertKVReq::update(&k, &b(&k))).await?;

            info!("--- upsert res: {:?}", res);

            let res = client.get_kv(&k).await?;
            let res = res.unwrap();

            assert_eq!(k.into_bytes(), res.data);
        }

        Ok(())
    }

    let tcs = start_metasrv_cluster(&[0, 1, 2]).await?;

    info!("--- test write on a fresh cluster");
    test_write_read_on_every_node(&tcs, "1st").await?;

    info!("--- shutdown the cluster");
    let stopped_tcs = {
        let mut stopped_tcs = vec![];
        for mut tc in tcs {
            // TODO(xp): remove this field, or split MetaSrvTestContext into two struct:
            //           one for metasrv and one for meta_node
            assert!(tc.meta_node.is_none());

            let mut srv = tc.grpc_srv.take().unwrap();
            srv.stop(None).await?;

            stopped_tcs.push(tc);
        }
        stopped_tcs
    };

    info!("--- restart the cluster");
    let tcs = {
        let mut tcs = vec![];
        for mut tc in stopped_tcs {
            start_metasrv_with_context(&mut tc).await?;
            tcs.push(tc);
        }

        for tc in &tcs {
            info!("--- wait until a leader is observed");
            // Every tcs[i] contains one meta node in this context.
            let g = tc.grpc_srv.as_ref().unwrap();
            let meta_node = g.get_meta_node();
            let metrics = meta_node
                .raft
                .wait(timeout())
                .metrics(|m| m.current_leader.is_some(), "a leader is observed")
                .await?;

            info!("got leader, metrics: {:?}", metrics);
        }
        tcs
    };

    info!("--- test write on a restarted cluster");
    test_write_read_on_every_node(&tcs, "2nd").await?;

    Ok(())
}

/// - Start a cluster of 3.
/// - Test upsert kv and read on different nodes.
/// - Stop and restart the cluster.
/// - Test read kv using same grpc client.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_api_restart_cluster_token_expired() -> anyhow::Result<()> {
    fn make_key(tc: &MetaSrvTestContext, k: impl std::fmt::Display) -> String {
        let x = &tc.config.raft_config;
        format!("t-restart-cluster-{}-{}-{}", x.config_id, x.id, k)
    }

    async fn test_write_read_on_every_node(
        tcs: &[MetaSrvTestContext],
        client: &ClientHandle,
        key_suffix: &str,
    ) -> anyhow::Result<()> {
        info!("--- test write on every node: {}", key_suffix);

        for (i, tc) in tcs.iter().enumerate() {
            let k = make_key(tc, key_suffix);
            if i == 0 {
                let res = client.upsert_kv(UpsertKVReq::update(&k, &b(&k))).await?;
                info!("--- upsert res: {:?}", res);
            } else {
                let client = tc.grpc_client().await.unwrap();
                let res = client.upsert_kv(UpsertKVReq::update(&k, &b(&k))).await?;
                info!("--- upsert res: {:?}", res);
            }

            let res = client.get_kv(&k).await?;
            let res = res.unwrap();

            assert_eq!(k.into_bytes(), res.data);
        }

        Ok(())
    }

    let tcs = start_metasrv_cluster(&[0, 1, 2]).await?;
    let client = MetaGrpcClient::try_create(
        vec![tcs[0].config.grpc_api_address.clone()],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    info!("--- test write on a fresh cluster");
    let key_suffix = "1st";
    test_write_read_on_every_node(&tcs, &client, key_suffix).await?;

    info!("--- shutdown the cluster");
    let stopped_tcs = {
        let mut stopped_tcs = vec![];
        for mut tc in tcs {
            assert!(tc.meta_node.is_none());

            let mut srv = tc.grpc_srv.take().unwrap();
            srv.stop(None).await?;

            stopped_tcs.push(tc);
        }
        stopped_tcs
    };

    info!("--- restart the cluster");
    let tcs = {
        let mut tcs = vec![];
        for mut tc in stopped_tcs {
            start_metasrv_with_context(&mut tc).await?;
            tcs.push(tc);
        }

        for tc in &tcs {
            info!("--- wait until a leader is observed");
            // Every tcs[i] contains one meta node in this context.
            let g = tc.grpc_srv.as_ref().unwrap();
            let meta_node = g.get_meta_node();
            let metrics = meta_node
                .raft
                .wait(timeout())
                .metrics(|m| m.current_leader.is_some(), "a leader is observed")
                .await?;

            info!("got leader, metrics: {:?}", metrics);
        }
        tcs
    };

    info!("--- read use old client");
    let tc = &tcs[0];
    let k = make_key(tc, key_suffix);
    let res = client.get_kv(&k).await?;
    let res = res.unwrap();

    assert_eq!(b(k), res.data);

    Ok(())
}

// Election timeout is 8~12 sec.
// A raft node waits for a interval of election timeout before starting election
fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(30_000))
}

fn b(s: impl ToString) -> Vec<u8> {
    s.to_string().into_bytes()
}
