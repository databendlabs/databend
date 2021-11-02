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
use common_meta_api::KVApiTestSuite;
use common_meta_api::MetaApiTestSuite;
use common_meta_flight::MetaFlightClient;
use common_meta_types::MatchSeq;
use common_meta_types::SeqV;
use common_meta_types::UpsertKVActionReply;
use common_tracing::tracing;
use databend_meta::init_meta_ut;
use pretty_assertions::assert_eq;

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
            .upsert_kv("foo", MatchSeq::Any, Some(b"bar".to_vec()), None)
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

        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // restart by opening existent meta db
        tc.config.raft_config.boot = false;
        databend_meta::tests::start_metasrv_with_context(&mut tc).await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(10_000)).await;

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
async fn test_generic_kv_mget() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_mget(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_generic_kv_list() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_list(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_generic_kv_delete() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_delete(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_generic_kv_update() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_update(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_generic_kv_update_meta() -> anyhow::Result<()> {
    // Only update meta, do not touch the value part.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_meta(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_generic_kv_timeout() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_timeout(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_generic_kv() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_write_read(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_database_create_get_drop() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    MetaApiTestSuite {}.database_create_get_drop(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_database_list() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    MetaApiTestSuite {}.database_list(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_table_create_get_drop() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    MetaApiTestSuite {}.table_create_get_drop(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_table_list() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;

    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    MetaApiTestSuite {}.table_list(&client).await
}

// TODO(xp): uncomment following tests when the function is ready
// ------------------------------------------------------------

/*
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_get_database_meta_ddl_table() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;
    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    let test_db = "db1";
    let plan = CreateDatabasePlan {
        if_not_exists: false,
        db: test_db.to_string(),
        engine: "Local".to_string(),
        options: Default::default(),
    };
    client.create_database(plan).await?;

    // After `create db`, meta_ver will be increased to 1

    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "number",
        DataType::UInt64,
        false,
    )]));

    // create-tbl operation will increases meta_version
    let plan = CreateTablePlan {
        if_not_exists: true,
        db: test_db.to_string(),
        table: "tbl1".to_string(),
        schema: schema.clone(),
        options: Default::default(),
        engine: "JSON".to_string(),
    };

    client.create_table(plan.clone()).await?;

    let res = client.get_database_meta(None).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();
    assert_eq!(2, snapshot.meta_ver);
    assert_eq!(1, snapshot.db_metas.len());
    assert_eq!(1, snapshot.tbl_metas.len());

    // if lower_bound < current meta version, returns database meta
    let res = client.get_database_meta(Some(0)).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();
    assert_eq!(2, snapshot.meta_ver);
    assert_eq!(1, snapshot.db_metas.len());

    // if lower_bound equals current meta version, returns None
    let res = client.get_database_meta(Some(2)).await?;
    assert!(res.is_none());

    // failed ddl do not effect meta version
    //  recall: plan.if_not_exist == true
    let _r = client.create_table(plan).await?;
    let res = client.get_database_meta(Some(2)).await?;
    assert!(res.is_none());

    // drop-table will increase meta version
    let plan = DropTablePlan {
        if_exists: true,
        db: test_db.to_string(),
        table: "tbl1".to_string(),
    };

    client.drop_table(plan).await?;
    let res = client.get_database_meta(Some(2)).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();
    assert_eq!(3, snapshot.meta_ver);
    assert_eq!(1, snapshot.db_metas.len());
    assert_eq!(0, snapshot.tbl_metas.len());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_get_database_meta_empty_db() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;
    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    // Empty Database
    let res = client.get_database_meta(None).await?;
    assert!(res.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_get_database_meta_ddl_db() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    let (_tc, addr) = databend_meta::tests::start_metasrv().await?;
    let client = MetaFlightClient::try_create(addr.as_str(), "root", "xxx").await?;

    // create-db operation will increases meta_version
    let plan = CreateDatabasePlan {
        if_not_exists: false,
        db: "db1".to_string(),
        engine: "Local".to_string(),
        options: Default::default(),
    };
    client.create_database(plan).await?;

    let res = client.get_database_meta(None).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();
    assert_eq!(1, snapshot.meta_ver);
    assert_eq!(1, snapshot.db_metas.len());

    // if lower_bound < current meta version, returns database meta
    let res = client.get_database_meta(Some(0)).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();
    assert_eq!(1, snapshot.meta_ver);
    assert_eq!(1, snapshot.db_metas.len());

    // if lower_bound equals current meta version, returns None
    let res = client.get_database_meta(Some(1)).await?;
    assert!(res.is_none());

    // failed ddl do not effect meta version
    let plan = CreateDatabasePlan {
        if_not_exists: true, // <<--
        db: "db1".to_string(),
        engine: "Local".to_string(),
        options: Default::default(),
    };

    client.create_database(plan).await?;
    let res = client.get_database_meta(Some(1)).await?;
    assert!(res.is_none());

    // drop-db will increase meta version
    let plan = DropDatabasePlan {
        if_exists: true,
        db: "db1".to_string(),
    };

    client.drop_database(plan).await?;
    let res = client.get_database_meta(Some(1)).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();

    assert_eq!(2, snapshot.meta_ver);
    assert_eq!(0, snapshot.db_metas.len());

    Ok(())
}
*/
