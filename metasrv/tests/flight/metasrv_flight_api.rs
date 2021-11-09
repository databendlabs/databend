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

//! Test arrow-flight API of metasrv

use common_base::tokio;
use common_meta_api::KVApi;
use common_meta_flight::MetaFlightClient;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::UpsertKVActionReply;
use common_tracing::tracing;
use databend_meta::init_meta_ut;
use databend_meta::tests::service::new_test_context;
use databend_meta::tests::start_metasrv_with_context;
use pretty_assertions::assert_eq;
use tokio::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_restart() -> anyhow::Result<()> {
    // Fix: Issue 1134  https://github.com/datafuselabs/databend/issues/1134
    // - Start a metasrv server.
    // - create db and create table
    // - restart
    // - Test read the db and read the table.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (mut tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    tracing::info!("--- upsert kv");
    {
        let res = client
            .upsert_kv(
                "foo",
                MatchSeq::Any,
                Operation::Update(b"bar".to_vec()),
                None,
            )
            .await;

        tracing::debug!("set kv res: {:?}", res);
        let res = res?;
        assert_eq!(
            UpsertKVActionReply::new(
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
            res.result,
            "get kv"
        );
    }

    tracing::info!("--- stop metasrv");
    {
        let (stop_tx, fin_rx) = tc.channels.take().unwrap();
        stop_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("fail to send"))?;

        fin_rx.await?;

        drop(client);

        tokio::time::sleep(Duration::from_millis(1000)).await;

        // restart by opening existent meta db
        tc.config.raft_config.boot = false;
        databend_meta::tests::start_metasrv_with_context(&mut tc).await?;
    }

    tokio::time::sleep(Duration::from_millis(10_000)).await;

    // try to reconnect the restarted server.
    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

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
            res.result,
            "get kv"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_join() -> anyhow::Result<()> {
    // - Start 2 metasrv.
    // - Join node-1 to node-0
    // - Test metasrv api

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let mut tc0 = new_test_context();
    let mut tc1 = new_test_context();

    tc0.config.raft_config.id = 0;

    tc1.config.raft_config.id = 1;
    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr()];

    start_metasrv_with_context(&mut tc0).await?;
    start_metasrv_with_context(&mut tc1).await?;

    let addr0 = tc0.config.flight_api_address.clone();
    let addr1 = tc0.config.flight_api_address.clone();

    let client0 = MetaFlightClient::try_create(addr0.as_str(), "root", "xxx").await?;
    let client1 = MetaFlightClient::try_create(addr1.as_str(), "root", "xxx").await?;

    let clients = vec![client0, client1];

    tracing::info!("--- upsert kv to every nodes");
    {
        for (i, cli) in clients.iter().enumerate() {
            let k = format!("join-{}", i);

            let res = cli
                .upsert_kv(
                    k.as_str(),
                    MatchSeq::Any,
                    Operation::Update(k.clone().into_bytes()),
                    None,
                )
                .await;

            tracing::debug!("set kv res: {:?}", res);
            let res = res?;
            assert_eq!(
                UpsertKVActionReply::new(
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
                assert_eq!(k.into_bytes(), res.result.unwrap().data);
            }
        }
    }

    Ok(())
}
