use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::session_type::SessionType;
use databend_common_exception::ErrorCode;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;
use databend_query::sessions::TableContextQueryInfo;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::HILBERT_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;

#[tokio::test(flavor = "multi_thread")]
async fn test_query_kind() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let http_session = fixture
        .new_session_with_type(SessionType::HTTPQuery)
        .await?;
    let ctx = http_session
        .create_query_context(&databend_common_version::BUILD_INFO)
        .await?;
    let mut planner = Planner::new(ctx.clone());
    let sql = format!(
        "COPY INTO {}.{} from  @~/ pattern='.*' FILE_FORMAT = (TYPE = 'csv') PURGE=true FORCE=true max_files=10000;",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    let (_, _) = planner.plan_sql(&sql).await?;
    let kind = ctx.get_query_kind();
    assert_eq!(kind, QueryKind::CopyIntoTable);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_hilbert_function_call_uses_normal_function_resolution() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx);

    let err = planner
        .plan_sql("CREATE TABLE default.bad_hilbert (a INT, b INT) CLUSTER BY (hilbert(a, b))")
        .await
        .unwrap_err();

    assert_eq!(err.code(), ErrorCode::UNKNOWN_FUNCTION);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_hilbert_dynamic_table_preserves_cluster_type() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx);
    let sql = "CREATE DYNAMIC TABLE default.hilbert_dynamic (a INT, b INT) \
               CLUSTER BY HILBERT(a, b) TARGET_LAG = DOWNSTREAM \
               AS SELECT 1 AS a, 2 AS b";

    let (plan, _) = planner.plan_sql(sql).await?;
    let Plan::CreateDynamicTable(plan) = plan else {
        panic!("expected create dynamic table plan");
    };
    assert_eq!(plan.cluster_key.as_deref(), Some("(a, b)"));
    assert_eq!(
        plan.options.get(OPT_KEY_CLUSTER_TYPE).map(String::as_str),
        Some(HILBERT_CLUSTER_TYPE)
    );
    Ok(())
}
