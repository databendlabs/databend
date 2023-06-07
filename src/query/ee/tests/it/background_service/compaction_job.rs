use std::sync::Arc;
use arrow_array::StringArray;
use futures_util::TryStreamExt;
use background_service::{BackgroundServiceHandler, get_background_service_handler};
use background_service::background_service::BackgroundServiceHandlerWrapper;
use databend_query::test_kits::TestFixture;
use common_exception::Result;
use enterprise_query::background_service::{CompactionJob, RealBackgroundService};
use common_base::base::{GlobalInstance, tokio};
use common_config::InnerConfig;
use common_meta_app::principal::{AuthInfo, GrantObject, PasswordHashMethod, UserInfo, UserPrivilegeSet};

#[tokio::test(flavor = "multi_thread")]
async fn test_get_compaction_advice_sql() -> Result<()> {
    let sql = CompactionJob::get_compaction_advice_sql("db1".to_string(), "tbl1".to_string(), 10, 100, 50);
    assert_eq!(sql.trim(), "select\n        IF(segment_count > 10 and block_count / segment_count < 100, TRUE, FALSE) AS segment_advice,\n        IF(bytes_uncompressed / block_count / 1024 / 1024 < 50, TRUE, FALSE) AS block_advice,\n        row_count, bytes_uncompressed, bytes_compressed, index_size,\n        segment_count, block_count,\n        block_count/segment_count,\n        humanize_size(bytes_uncompressed / block_count) AS per_block_uncompressed_size_string\n        from fuse_snapshot('db1', 'tbl1') order by timestamp ASC LIMIT 1;");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_segment_compaction_sql() -> Result<()> {
    let sql = CompactionJob::get_segment_compaction_sql("db1".to_string(), "tbl1".to_string(), None);
    assert_eq!(sql.trim(), "OPTIMIZE TABLE db1.tbl1 COMPACT SEGMENT;");
    let sql = CompactionJob::get_segment_compaction_sql("db1".to_string(), "tbl1".to_string(), Option::Some(100));
    assert_eq!(sql.trim(), "OPTIMIZE TABLE db1.tbl1 COMPACT SEGMENT LIMIT 100;");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_block_compaction_sql() -> Result<()> {
    let sql = CompactionJob::get_block_compaction_sql("db1".to_string(), "tbl1".to_string(), None);
    assert_eq!(sql.trim(), "OPTIMIZE TABLE db1.tbl1 COMPACT;");
    let sql = CompactionJob::get_block_compaction_sql("db1".to_string(), "tbl1".to_string(), Option::Some(100));
    assert_eq!(sql.trim(), "OPTIMIZE TABLE db1.tbl1 COMPACT LIMIT 100;");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_advices() -> Result<()> {
    let fixture = TestFixture::new().await;
    let tbl_name = fixture.default_table_name();
    let db_name = fixture.default_db_name();
    fixture.create_normal_table().await?;

    // insert 5 times
    let n = 5;
    for _ in 0..n {
        let table = fixture.latest_default_table().await?;
        let num_blocks = 1;
        let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;
    }

    let conf = InnerConfig::default();
    let mut svc = RealBackgroundService::new(&conf).await?;
    svc.set_context(fixture.ctx()).await;

    let wrapper = BackgroundServiceHandlerWrapper::new(Box::new(svc));
    GlobalInstance::set(Arc::new(wrapper));
    let svc = get_background_service_handler();
    // // it should be a candidate
    let candidates = CompactionJob::do_get_all_target_tables(&svc).await;
    assert!(candidates.is_ok());
    let candidates = candidates.unwrap().unwrap();
    assert!(candidates.num_rows() > 0);
    // let dbs = candidates.column_by_name("database").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
    // let tbs = candidates.column_by_name("table").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
    // let mut found = false;
    // for i in 0..candidates.num_rows() {
    //     if dbs.value(i) == db_name && tbs.value(i) == tbl_name {
    //         found = true;
    //         break;
    //     }
    // }
    // assert!(found);
    //
    // let (should_sg, should_blk, stats) = CompactionJob::do_check_table(&svc, db_name, tbl_name, 0, 1, 0).await?;
    // assert!(should_sg);
    // assert!(should_blk);
    Ok(())
}