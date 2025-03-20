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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_storage::DataOperator;
use databend_common_tracing::RemoteLog;
use databend_common_tracing::RemoteLogElement;
use databend_query::persistent_log::GlobalPersistentLog;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
pub async fn test_global_persistent_log_acquire_lock() -> Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.log.persistentlog.on = true;
    let _guard = TestFixture::setup_with_config(&config).await?;
    let res = GlobalPersistentLog::instance().try_acquire().await?;
    assert!(res, "should acquire lock");

    let res = GlobalPersistentLog::instance().try_acquire().await?;
    assert!(!res, "should not acquire lock before expire");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    let res = GlobalPersistentLog::instance().try_acquire().await?;
    assert!(res, "should acquire lock after expire");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_persistent_log_write() -> Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.log.persistentlog.on = true;
    config.log.persistentlog.interval = 1;
    config.log.file.on = false;

    let fixture = TestFixture::setup_with_config(&config).await?;

    // Add more workers to test acquire lock and copy into
    GlobalIORuntime::instance().spawn(async move {
        let _ = GlobalPersistentLog::instance().work().await;
    });

    GlobalIORuntime::instance().spawn(async move {
        let _ = GlobalPersistentLog::instance().work().await;
    });

    GlobalPersistentLog::instance().initialized();

    let random_sleep = rand::random::<u64>() % 3 + 1;
    for _i in 0..3 {
        write_remote_log(&config.log.persistentlog.stage_name).await?;
        tokio::time::sleep(std::time::Duration::from_secs(random_sleep)).await;
    }

    let res = fixture
        .execute_query("select count(*) from persistent_system.text_log")
        .await?;

    let data_blocks: Vec<DataBlock> = res.try_collect().await?;
    let cnt = data_blocks[0].clone().take_columns()[0]
        .clone()
        .value
        .into_scalar()
        .unwrap()
        .get_i64();

    assert_eq!(cnt, Some(2 * 3));

    GlobalPersistentLog::instance().stop();

    Ok(())
}

async fn write_remote_log(stage_name: &str) -> Result<()> {
    let path = format!(
        "stage/internal/{}/{}.parquet",
        stage_name,
        uuid::Uuid::new_v4()
    );
    let op = DataOperator::instance().operator();
    RemoteLog::do_flush(op, get_remote_log(), &path).await?;
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
