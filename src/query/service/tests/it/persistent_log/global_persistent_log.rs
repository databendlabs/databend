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
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let res = fixture
        .execute_query("select count(*) from persistent_system.query_log")
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
            timestamp: chrono::Local::now().timestamp_micros(),
            path: "databend_query::interpreters::common::query_log: query_log.rs:71".to_string(),
            target: "databend::log::query".to_string(),
            cluster_id: "test_cluster".to_string(),
            node_id: "izs9przqhAN4n5hbJanJm2".to_string(),
            query_id: Some("89ad07ad-83fe-4424-8005-4c5b318a7212".to_string()),
            warehouse_id: None,
            message: "test".to_string(),
            log_level: "INFO".to_string(),

            fields: r#"{"message":"test"}"#.to_string(),
        },
        RemoteLogElement {
            timestamp: chrono::Local::now().timestamp_micros(),
            path: "databend_query::interpreters::common::query_log: query_log.rs:71".to_string(),
            target: "databend::log::query".to_string(),
            cluster_id: "test_cluster".to_string(),
            node_id: "izs9przqhAN4n5hbJanJm2".to_string(),
            query_id: Some("89ad07ad-83fe-4424-8005-4c5b318a7212".to_string()),
            warehouse_id: None,
            log_level: "INFO".to_string(),
            message: "test2".to_string(),
            fields: r#"{"message":"test"}"#.to_string(),
        },
    ]
}
