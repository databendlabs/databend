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

//! Test metasrv MetaApi by writing to one node and then reading from another,
//! on a restarted cluster.

use std::time::Duration;

use common_base::tokio;
use common_base::Stoppable;
use common_meta_api::KVApi;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::UpsertKVAction;
use common_tracing::tracing;

use crate::init_meta_ut;
use crate::tests::service::start_metasrv_cluster;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::start_metasrv_with_context;

/// - Start a cluster of 3.
/// - Test upsert kv and read on different nodes.
/// - Stop and restart the cluster.
/// - Test upsert kv and read on different nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_kv_api_restart_cluster_write_read() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    fn make_key(tc: &MetaSrvTestContext, k: impl std::fmt::Display) -> String {
        let x = &tc.config.raft_config;
        format!("t-restart-cluster-{}-{}-{}", x.config_id, x.id, k)
    }

    async fn test_write_read_on_every_node(
        tcs: &[MetaSrvTestContext],
        key_suffix: &str,
    ) -> anyhow::Result<()> {
        tracing::info!("--- test write on every node: {}", key_suffix);

        for tc in tcs.iter() {
            let client = tc.grpc_client().await?;

            let k = make_key(tc, key_suffix);
            let res = client
                .upsert_kv(UpsertKVAction {
                    key: k.clone(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(k.clone().into_bytes()),
                    value_meta: None,
                })
                .await?;

            tracing::info!("--- upsert res: {:?}", res);

            let res = client.get_kv(&k).await?;
            let res = res.unwrap();

            assert_eq!(k.into_bytes(), res.data);
        }

        Ok(())
    }

    let tcs = start_metasrv_cluster(&[0, 1, 2]).await?;

    tracing::info!("--- test write on a fresh cluster");
    test_write_read_on_every_node(&tcs, "1st").await?;

    tracing::info!("--- shutdown the cluster");
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

    tracing::info!("--- restart the cluster");
    let tcs = {
        let mut tcs = vec![];
        for mut tc in stopped_tcs {
            start_metasrv_with_context(&mut tc).await?;
            tcs.push(tc);
        }

        for tc in &tcs {
            tracing::info!("--- wait until a leader is observed");
            // Every tcs[i] contains one meta node in this context.
            let g = tc.grpc_srv.as_ref().unwrap();
            let meta_node = g.get_meta_node();
            let metrics = meta_node
                .raft
                .wait(timeout())
                .metrics(|m| m.current_leader.is_some(), "a leader is observed")
                .await?;

            tracing::info!("got leader, metrics: {:?}", metrics);
        }
        tcs
    };

    tracing::info!("--- test write on a restarted cluster");
    test_write_read_on_every_node(&tcs, "2nd").await?;

    Ok(())
}

/// - Start a cluster of 3.
/// - Test upsert kv and read on different nodes.
/// - Stop and restart the cluster.
/// - Test read kv using same grpc client.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_kv_api_restart_cluster_token_expired() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    fn make_key(tc: &MetaSrvTestContext, k: impl std::fmt::Display) -> String {
        let x = &tc.config.raft_config;
        format!("t-restart-cluster-{}-{}-{}", x.config_id, x.id, k)
    }

    async fn test_write_read_on_every_node(
        tcs: &[MetaSrvTestContext],
        client: &MetaGrpcClient,
        key_suffix: &str,
    ) -> anyhow::Result<()> {
        tracing::info!("--- test write on every node: {}", key_suffix);

        for (i, tc) in tcs.iter().enumerate() {
            let k = make_key(tc, key_suffix);
            if i == 0 {
                let res = client
                    .upsert_kv(UpsertKVAction {
                        key: k.clone(),
                        seq: MatchSeq::Any,
                        value: Operation::Update(k.clone().into_bytes()),
                        value_meta: None,
                    })
                    .await?;
                tracing::info!("--- upsert res: {:?}", res);
            } else {
                let client = tc.grpc_client().await.unwrap();
                let res = client
                    .upsert_kv(UpsertKVAction {
                        key: k.clone(),
                        seq: MatchSeq::Any,
                        value: Operation::Update(k.clone().into_bytes()),
                        value_meta: None,
                    })
                    .await?;
                tracing::info!("--- upsert res: {:?}", res);
            }

            let res = client.get_kv(&k).await?;
            let res = res.unwrap();

            assert_eq!(k.into_bytes(), res.data);
        }

        Ok(())
    }

    let tcs = start_metasrv_cluster(&[0, 1, 2]).await?;
    let client = MetaGrpcClient::try_create(
        tcs[0].config.grpc_api_address.clone().as_str(),
        "root",
        "xxx",
        None,
        None,
    )
    .await?;

    tracing::info!("--- test write on a fresh cluster");
    let key_suffix = "1st";
    test_write_read_on_every_node(&tcs, &client, key_suffix).await?;

    tracing::info!("--- shutdown the cluster");
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

    tracing::info!("--- restart the cluster");
    let tcs = {
        let mut tcs = vec![];
        for mut tc in stopped_tcs {
            start_metasrv_with_context(&mut tc).await?;
            tcs.push(tc);
        }

        for tc in &tcs {
            tracing::info!("--- wait until a leader is observed");
            // Every tcs[i] contains one meta node in this context.
            let g = tc.grpc_srv.as_ref().unwrap();
            let meta_node = g.get_meta_node();
            let metrics = meta_node
                .raft
                .wait(timeout())
                .metrics(|m| m.current_leader.is_some(), "a leader is observed")
                .await?;

            tracing::info!("got leader, metrics: {:?}", metrics);
        }
        tcs
    };

    tracing::info!("--- read use old client");
    let tc = &tcs[0];
    let k = make_key(tc, key_suffix);
    let res = client.get_kv(&k).await?;
    let res = res.unwrap();

    assert_eq!(k.into_bytes(), res.data);

    Ok(())
}
// Election timeout is 8~12 sec.
// A raft node waits for a interval of election timeout before starting election
// TODO: the raft should set the wait time by election_timeout.
//       For now, just use a large timeout.
fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(30_000))
}
