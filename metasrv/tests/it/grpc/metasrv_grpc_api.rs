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

//! Test arrow-grpc API of metasrv
use std::collections::HashSet;

use common_base::base::tokio;
use common_base::base::Stoppable;
use common_meta_api::KVApi;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::MatchSeq;
use common_meta_types::MetaError;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::UpsertKVReply;
use common_meta_types::UpsertKVReq;
use common_tracing::tracing;
use pretty_assertions::assert_eq;
use tokio::time::Duration;

use crate::init_meta_ut;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::start_metasrv_with_context;

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_restart() -> anyhow::Result<()> {
    // Fix: Issue 1134  https://github.com/datafuselabs/databend/issues/1134
    // - Start a metasrv server.
    // - create db and create table
    // - restart
    // - Test read the db and read the table.

    let (mut tc, addr) = crate::tests::start_metasrv().await?;

    let client = MetaGrpcClient::try_create(
        vec![addr.clone()],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    tracing::info!("--- upsert kv");
    {
        let res = client
            .upsert_kv(UpsertKVReq::new(
                "foo",
                MatchSeq::Any,
                Operation::Update(b"bar".to_vec()),
                None,
            ))
            .await;

        tracing::debug!("set kv res: {:?}", res);
        let res = res?;
        assert_eq!(
            UpsertKVReply::new(
                None,
                Some(SeqV {
                    seq: 1,
                    meta: None,
                    data: b"bar".to_vec(),
                })
            ),
            res,
            "upsert kv"
        );
    }

    tracing::info!("--- get kv");
    {
        let res = client.get_kv("foo").await;
        tracing::debug!("get kv res: {:?}", res);
        let res = res?;
        assert_eq!(
            Some(SeqV {
                seq: 1,
                meta: None,
                data: b"bar".to_vec(),
            }),
            res,
            "get kv"
        );
    }

    tracing::info!("--- stop metasrv");
    {
        let mut srv = tc.grpc_srv.take().unwrap();
        srv.stop(None).await?;

        drop(client);

        tokio::time::sleep(Duration::from_millis(1000)).await;

        crate::tests::start_metasrv_with_context(&mut tc).await?;
    }

    tokio::time::sleep(Duration::from_millis(10_000)).await;

    // try to reconnect the restarted server.
    let client = MetaGrpcClient::try_create(
        vec![addr],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    tracing::info!("--- get kv");
    {
        let res = client.get_kv("foo").await;
        tracing::debug!("get kv res: {:?}", res);
        let res = res?;
        assert_eq!(
            Some(SeqV {
                seq: 1,
                meta: None,
                data: b"bar".to_vec()
            }),
            res,
            "get kv"
        );
    }

    Ok(())
}

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_retry_join() -> anyhow::Result<()> {
    // - Start 2 metasrv.
    // - Join node-1 to node-0
    // - Test metasrv retry cluster case

    let mut tc0 = MetaSrvTestContext::new(0);
    start_metasrv_with_context(&mut tc0).await?;

    let bad_addr = "127.0.0.1:1".to_string();

    // first test join has only bad_addr case, MUST return JoinClusterFail
    {
        let mut tc1 = MetaSrvTestContext::new(1);
        tc1.config.raft_config.single = false;

        tc1.config.raft_config.join = vec![bad_addr.clone()];
        let ret = start_metasrv_with_context(&mut tc1).await;
        let expect = MetaError::MetaServiceError(format!(
            "join cluster via {:?} fail",
            tc1.config.raft_config.join
        ));

        match ret {
            Ok(_) => panic!("must return JoinClusterFail"),
            Err(e) => {
                assert_eq!(e.to_string(), expect.to_string());
            }
        }
    }

    // second test join has bad_addr and tc0 addr case, MUST return success
    {
        let mut tc1 = MetaSrvTestContext::new(1);
        tc1.config.raft_config.single = false;
        tc1.config.raft_config.join = vec![
            bad_addr,
            tc0.config.raft_config.raft_api_addr().await?.to_string(),
        ];
        let ret = start_metasrv_with_context(&mut tc1).await;
        match ret {
            Ok(_) => Ok(()),
            Err(e) => {
                panic!("must JoinCluster success: {:?}", e);
            }
        }
    }
}

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_join() -> anyhow::Result<()> {
    // - Start 2 metasrv.
    // - Join node-1 to node-0
    // - Test metasrv api

    let mut tc0 = MetaSrvTestContext::new(0);
    let mut tc1 = MetaSrvTestContext::new(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];

    start_metasrv_with_context(&mut tc0).await?;
    start_metasrv_with_context(&mut tc1).await?;

    let addr0 = tc0.config.grpc_api_address.clone();
    let addr1 = tc1.config.grpc_api_address.clone();

    let client0 = MetaGrpcClient::try_create(
        vec![addr0],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;
    let client1 = MetaGrpcClient::try_create(
        vec![addr1],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    let clients = vec![client0, client1];

    tracing::info!("--- upsert kv to every nodes");
    {
        for (i, cli) in clients.iter().enumerate() {
            let k = format!("join-{}", i);

            let res = cli
                .upsert_kv(UpsertKVReq::new(
                    k.as_str(),
                    MatchSeq::Any,
                    Operation::Update(k.clone().into_bytes()),
                    None,
                ))
                .await;

            tracing::debug!("set kv res: {:?}", res);
            let res = res?;
            assert_eq!(
                UpsertKVReply::new(
                    None,
                    Some(SeqV {
                        seq: 1 + i as u64,
                        meta: None,
                        data: k.into_bytes(),
                    })
                ),
                res,
                "upsert kv to node {}",
                i
            );
        }
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;

    tracing::info!("--- get every kv from every node");
    {
        for (icli, cli) in clients.iter().enumerate() {
            for i in 0..2 {
                let k = format!("join-{}", i);
                let res = cli.get_kv(k.as_str()).await;

                tracing::debug!("get kv {} from {}-th node,res: {:?}", k, icli, res);
                let res = res?;
                assert_eq!(k.into_bytes(), res.unwrap().data);
            }
        }
    }

    Ok(())
}

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_auto_sync_addr() -> anyhow::Result<()> {
    // - Start 3 metasrv.
    // - Join node-1, node-2 to node-0
    // - Test meta client auto sync node endpoints
    // - Terminal one node and check auto sync again
    // - Restart terminated node and rejoin to meta cluster
    // - Test auto sync again.

    let mut tc0 = MetaSrvTestContext::new(0);
    let mut tc1 = MetaSrvTestContext::new(1);
    let mut tc2 = MetaSrvTestContext::new(2);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];

    tc2.config.raft_config.single = false;
    tc2.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];

    start_metasrv_with_context(&mut tc0).await?;
    start_metasrv_with_context(&mut tc1).await?;
    start_metasrv_with_context(&mut tc2).await?;

    let addr0 = tc0.config.grpc_api_address.clone();
    let addr1 = tc1.config.grpc_api_address.clone();
    let addr2 = tc2.config.grpc_api_address.clone();

    let client = MetaGrpcClient::try_create(
        vec![addr1.clone()],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(5)),
        None,
    )?;

    let addrs = HashSet::from([addr0, addr1, addr2]);

    tracing::info!("--- upsert kv cluster");
    {
        let k = "join-k".to_string();

        let res = client
            .upsert_kv(UpsertKVReq::new(
                k.as_str(),
                MatchSeq::Any,
                Operation::Update(k.clone().into_bytes()),
                None,
            ))
            .await;

        tracing::debug!("set kv res: {:?}", res);
        let res = res?;
        assert_eq!(
            UpsertKVReply::new(
                None,
                Some(SeqV {
                    seq: 1_u64,
                    meta: None,
                    data: k.into_bytes(),
                })
            ),
            res,
            "upsert kv to cluster",
        );
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;

    tracing::info!("--- get kv from cluster");
    {
        let k = "join-k".to_string();

        let res = client.get_kv(k.as_str()).await;

        tracing::debug!("get kv {} from cluster, res: {:?}", k, res);
        let res = res?;
        assert_eq!(k.into_bytes(), res.unwrap().data);
    }

    tracing::info!("--- check endpoints are equal");
    {
        tokio::time::sleep(Duration::from_secs(20)).await;
        let res = client.get_endpoints().await?;
        let res: HashSet<String> = HashSet::from_iter(res.into_iter());

        assert_eq!(addrs, res, "endpoints should be equal");
    }

    tracing::info!("--- endpoints should changed when node is down");
    {
        let g = tc1.grpc_srv.as_ref().unwrap();
        let meta_node = g.get_meta_node();
        let old_term = meta_node.raft.metrics().borrow().current_term;

        let mut srv = tc0.grpc_srv.take().unwrap();
        srv.stop(None).await?;

        let metrics = meta_node
            .raft
            .wait(Some(Duration::from_millis(30_000)))
            .metrics(
                |m| m.current_leader.is_some() && m.current_term > old_term,
                "a leader is observed",
            )
            .await?;

        tracing::debug!("got leader, metrics: {:?}", metrics);

        // wait for auto sync work.
        tokio::time::sleep(Duration::from_secs(20)).await;

        let res = client.get_endpoints().await?;
        let res: HashSet<String> = HashSet::from_iter(res.into_iter());

        assert_eq!(3, res.len());

        // still can get kv from cluster
        let k = "join-k".to_string();

        let res = client.get_kv(k.as_str()).await;

        let res = res?;
        assert_eq!(k.into_bytes(), res.unwrap().data);
    }

    tracing::info!("--- endpoints should changed when add node");
    {
        let mut tc3 = MetaSrvTestContext::new(3);
        tc3.config.raft_config.single = false;
        tc3.config.raft_config.join = vec![
            tc1.config.raft_config.raft_api_addr().await?.to_string(),
            tc2.config.raft_config.raft_api_addr().await?.to_string(),
        ];

        start_metasrv_with_context(&mut tc3).await?;

        let addr3 = tc3.config.grpc_api_address.clone();

        tokio::time::sleep(Duration::from_secs(15)).await;

        let res = client.get_endpoints().await?;
        let res: HashSet<String> = HashSet::from_iter(res.into_iter());

        assert!(
            res.contains(&addr3),
            "endpoints should contains new addr when add node"
        );

        // still can get kv from cluster
        let k = "join-k".to_string();

        let res = client.get_kv(k.as_str()).await;

        let res = res?;
        assert_eq!(k.into_bytes(), res.unwrap().data);
    }

    Ok(())
}
