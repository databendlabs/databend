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
use std::io::Write;

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_storage::DataOperator;
use databend_common_tracing::RemoteLog;
use databend_common_tracing::RemoteLogElement;
use databend_query::persistent_log::GlobalPersistentLog;
use databend_query::persistent_log::PersistentLogTable;
use databend_query::persistent_log::QueryDetailsTable;
use databend_query::persistent_log::QueryLogTable;
use databend_query::persistent_log::QueryProfileTable;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;
use goldenfile::Mint;

#[tokio::test(flavor = "multi_thread")]
pub async fn test_persistent_log_write() -> Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.log.persistentlog.retention = 0;
    let fixture = TestFixture::setup_with_config(&config).await?;
    write_remote_log(&config.log.persistentlog.stage_name).await?;

    let log_instance = GlobalPersistentLog::create_dummy(&config).await?;

    log_instance.prepare().await?;
    log_instance.do_copy_into().await?;
    check_count(&fixture, vec![3, 1, 1]).await?;

    // Extreme case: retention is 0, so all logs should be deleted
    log_instance.do_clean().await?;
    check_count(&fixture, vec![0, 0, 0]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_persistent_log_alter_if_schema_change() -> Result<()> {
    let config = ConfigBuilder::create().config();
    // Test the schema change of persistent log table
    let fixture = TestFixture::setup_with_config(&config).await?;

    let _ = fixture
        .execute_query("create database persistent_system")
        .await?;
    // create a table with the same name
    let _ = fixture
        .execute_query(
            "CREATE TABLE IF NOT EXISTS persistent_system.query_log (timestamp TIMESTAMP)",
        )
        .await?;

    let _ = fixture
        .execute_query("INSERT INTO persistent_system.query_log (timestamp) VALUES (now())")
        .await?;

    let log_instance = GlobalPersistentLog::create_dummy(&config).await?;

    log_instance.set_version_to_meta(0).await?;

    log_instance.prepare().await?;

    let context = fixture.new_query_ctx().await?;

    assert!(context
        .get_table(CATALOG_DEFAULT, "persistent_system", "query_log_v0")
        .await
        .is_ok());
    assert!(context
        .get_table(CATALOG_DEFAULT, "persistent_system", "query_profile")
        .await
        .is_ok());
    assert!(context
        .get_table(CATALOG_DEFAULT, "persistent_system", "query_details")
        .await
        .is_ok());
    assert!(context
        .get_table(CATALOG_DEFAULT, "persistent_system", "query_log")
        .await
        .is_ok());

    let res = fixture
        .execute_query("select count(*) from persistent_system.query_log_v0")
        .await?;
    let data_blocks: Vec<DataBlock> = res.try_collect().await?;
    let cnt = data_blocks[0].clone().take_columns()[0]
        .clone()
        .value
        .into_scalar()
        .unwrap()
        .get_i64();
    assert_eq!(cnt, Some(1));

    Ok(())
}

#[test]
pub fn log_table_schema_change_must_increase_version_number() -> Result<()> {
    // If this test failed, please increase version number for PERSISTENT_LOG_SCHEMA_VERSION
    // and rerun this test with `UPDATE_GOLDENFILES=1`
    let mut mint = Mint::new("tests/it/persistent_log/testdata");
    let file = &mut mint.new_goldenfile("persistent_log_tables_schema.txt")?;
    let tables: Vec<Box<dyn PersistentLogTable>> = vec![
        Box::new(QueryDetailsTable::new()),
        Box::new(QueryProfileTable::new()),
        Box::new(QueryLogTable::new()),
    ];
    for table in tables.iter() {
        writeln!(file, "{}", table.table_name())?;
        let schema = table.schema();
        for field in schema.fields().iter() {
            writeln!(file, "{}:{}", field.name(), field.data_type())?;
        }
    }
    Ok(())
}

async fn check_count(fixture: &TestFixture, expected: Vec<usize>) -> Result<()> {
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
    assert_eq!(cnt, Some(expected[0] as i64));
    let res = fixture
        .execute_query("select count(*) from persistent_system.query_details")
        .await?;
    let data_blocks: Vec<DataBlock> = res.try_collect().await?;
    let cnt = data_blocks[0].clone().take_columns()[0]
        .clone()
        .value
        .into_scalar()
        .unwrap()
        .get_i64();
    assert_eq!(cnt, Some(expected[1] as i64));
    let res = fixture
        .execute_query("select count(*) from persistent_system.query_profile")
        .await?;
    let data_blocks: Vec<DataBlock> = res.try_collect().await?;
    let cnt = data_blocks[0].clone().take_columns()[0]
        .clone()
        .value
        .into_scalar()
        .unwrap()
        .get_i64();
    assert_eq!(cnt, Some(expected[2] as i64));
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
            path: "databend_common_meta_semaphore::meta_event_subscriber::subscriber: subscriber.rs:52".to_string(),
            target: "databend_common_meta_semaphore::meta_event_subscriber::subscriber".to_string(),
            cluster_id: "test_cluster".to_string(),
            node_id: "izs9przqhAN4n5hbJanJm2".to_string(),
            query_id: Some("89ad07ad-83fe-4424-8005-4c5b318a7212".to_string()),
            warehouse_id: None,
            log_level: "WARN".to_string(),
            message: "test".to_string(),
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
            message: r#"{"log_type":1,"log_type_name":"Start","handler_type":"Dummy","tenant_id":"test_tenant","cluster_id":"test_cluster","node_id":"E5q3SYqfNRUF6iRBEU2Cw5","sql_user":"test_tenant-test_cluster-persistent-log","query_id":"5cf4e80b-a169-46ee-8b4e-57cd9a675557","query_kind":"Insert","query_text":"INSERT INTO persistent_system.query_details SELECT * FROM (SELECT m['log_type'], m['log_type_name'], m['handler_type'], m['tenan...[1166 more characters]","query_hash":"","query_parameterized_hash":"","event_date":"2025-04-09","event_time":"2025-04-09 11:20:47.795743","query_start_time":"2025-04-09 11:20:47.714015","query_duration_ms":0,"query_queued_duration_ms":0,"current_database":"default","databases":"","tables":"","columns":"","projections":"","written_rows":0,"written_bytes":0,"written_io_bytes":0,"written_io_bytes_cost_ms":0,"scan_rows":0,"scan_bytes":0,"scan_io_bytes":0,"scan_io_bytes_cost_ms":0,"scan_partitions":0,"total_partitions":0,"result_rows":0,"result_bytes":0,"cpu_usage":8,"memory_usage":0,"join_spilled_bytes":0,"join_spilled_rows":0,"agg_spilled_bytes":0,"agg_spilled_rows":0,"group_by_spilled_bytes":0,"group_by_spilled_rows":0,"bytes_from_remote_disk":0,"bytes_from_local_disk":0,"bytes_from_memory":0,"client_info":"","client_address":"","user_agent":"null","exception_code":0,"exception_text":"","stack_trace":"","server_version":"v1.2.718-nightly-e82fb056d0(rust-1.85.0-nightly-2025-04-09T10:02:05.358810000Z)","query_tag":"","extra":"","has_profiles":false,"txn_state":"AutoCommit","txn_id":"","peek_memory_usage":{}}"#.to_string(),
            fields: "{}".to_string()
        },
        RemoteLogElement {
            timestamp: chrono::Local::now().timestamp_micros(),
            path: "databend_query::interpreters::interpreter: interpreter.rs:315".to_string(),
            target: "databend::log::profile".to_string(),
            log_level: "INFO".to_string(),
            cluster_id: "test_cluster".to_string(),
            node_id: "Io95Mk1ULcoWkZ8FIFUog1".to_string(),
            warehouse_id: None,
            query_id: Some("992fa371-4636-43a8-9879-90f7e75b15e8".to_string()),
            message: r#"{"query_id":"992fa371-4636-43a8-9879-90f7e75b15e8","profiles":[{"id":0,"name":"EvalScalar","parent_id":null,"title":"123","labels":[{"name":"List of Expressions","value":["123"]}],"statistics":[29875,0,0,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"errors":[]},{"id":1,"name":"TableScan","parent_id":0,"title":"default.'system'.'one'","labels":[{"name":"Columns (1 / 1)","value":["dummy"]},{"name":"Total partitions","value":["1"]},{"name":"Full table name","value":["default.'system'.'one'"]}],"statistics":[19459,0,0,0,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"errors":[]}],"statistics_desc":{"CpuTime":{"desc":"The time spent to process in nanoseconds","display_name":"cpu time","index":0,"unit":"NanoSeconds","plain_statistics":false},"WaitTime":{"desc":"The time spent to wait in nanoseconds, usually used to measure the time spent on waiting for I/O","display_name":"wait time","index":1,"unit":"NanoSeconds","plain_statistics":false},"ExchangeRows":{"desc":"The number of data rows exchange between nodes in cluster mode","display_name":"exchange rows","index":2,"unit":"Rows","plain_statistics":true},"ExchangeBytes":{"desc":"The number of data bytes exchange between nodes in cluster mode","display_name":"exchange bytes","index":3,"unit":"Bytes","plain_statistics":true},"OutputRows":{"desc":"The number of rows from the physical plan output to the next physical plan","display_name":"output rows","index":4,"unit":"Rows","plain_statistics":true},"OutputBytes":{"desc":"The number of bytes from the physical plan output to the next physical plan","display_name":"output bytes","index":5,"unit":"Bytes","plain_statistics":true},"ScanBytes":{"desc":"The bytes scanned of query","display_name":"bytes scanned","index":6,"unit":"Bytes","plain_statistics":true},"ScanCacheBytes":{"desc":"The bytes scanned from cache of query","display_name":"bytes scanned from cache","index":7,"unit":"Bytes","plain_statistics":true},"ScanPartitions":{"desc":"The partitions scanned of query","display_name":"partitions scanned","index":8,"unit":"Count","plain_statistics":true},"RemoteSpillWriteCount":{"desc":"The number of remote spilled by write","display_name":"numbers remote spilled by write","index":9,"unit":"Count","plain_statistics":true},"RemoteSpillWriteBytes":{"desc":"The bytes remote spilled by write","display_name":"bytes remote spilled by write","index":10,"unit":"Bytes","plain_statistics":true},"RemoteSpillWriteTime":{"desc":"The time spent to write remote spill in millisecond","display_name":"remote spilled time by write","index":11,"unit":"MillisSeconds","plain_statistics":false},"RemoteSpillReadCount":{"desc":"The number of remote spilled by read","display_name":"numbers remote spilled by read","index":12,"unit":"Count","plain_statistics":true},"RemoteSpillReadBytes":{"desc":"The bytes remote spilled by read","display_name":"bytes remote spilled by read","index":13,"unit":"Bytes","plain_statistics":true},"RemoteSpillReadTime":{"desc":"The time spent to read remote spill in millisecond","display_name":"remote spilled time by read","index":14,"unit":"MillisSeconds","plain_statistics":false},"LocalSpillWriteCount":{"desc":"The number of local spilled by write","display_name":"numbers local spilled by write","index":15,"unit":"Count","plain_statistics":true},"LocalSpillWriteBytes":{"desc":"The bytes local spilled by write","display_name":"bytes local spilled by write","index":16,"unit":"Bytes","plain_statistics":true},"LocalSpillWriteTime":{"desc":"The time spent to write local spill in millisecond","display_name":"local spilled time by write","index":17,"unit":"MillisSeconds","plain_statistics":false},"LocalSpillReadCount":{"desc":"The number of local spilled by read","display_name":"numbers local spilled by read","index":18,"unit":"Count","plain_statistics":true},"LocalSpillReadBytes":{"desc":"The bytes local spilled by read","display_name":"bytes local spilled by read","index":19,"unit":"Bytes","plain_statistics":true},"LocalSpillReadTime":{"desc":"The time spent to read local spill in millisecond","display_name":"local spilled time by read","index":20,"unit":"MillisSeconds","plain_statistics":false},"RuntimeFilterPruneParts":{"desc":"The partitions pruned by runtime filter","display_name":"parts pruned by runtime filter","index":21,"unit":"Count","plain_statistics":true},"MemoryUsage":{"desc":"The real time memory usage","display_name":"memory usage","index":22,"unit":"Bytes","plain_statistics":false},"ExternalServerRetryCount":{"desc":"The count of external server retry times","display_name":"external server retry count","index":23,"unit":"Count","plain_statistics":true},"ExternalServerRequestCount":{"desc":"The count of external server request times","display_name":"external server request count","index":24,"unit":"Count","plain_statistics":true}}}"#.to_string(),
            fields: "{}".to_string(),

        }
    ]
}
