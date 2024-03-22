use databend_common_catalog::query_kind::QueryKind;
use databend_common_exception::Result;
use databend_common_sql::Planner;
use databend_common_storages_fuse::TableContext;
use databend_query::sessions::SessionType;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_query_kind() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    let http_session = fixture
        .new_session_with_type(SessionType::HTTPQuery)
        .await?;
    let ctx = http_session.create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql("COPY INTO xx.xx from 's3://xx/xx/xx/' pattern='.*' FILE_FORMAT = (TYPE = 'parquet') PURGE=true FORCE=true max_files=10000;").await?;
    let kind = ctx.get_query_kind();
    assert_eq!(kind, QueryKind::CopyIntoTable);
    Ok(())
}
