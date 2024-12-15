use databend_common_catalog::query_kind::QueryKind;
use databend_common_exception::Result;
use databend_common_sql::Planner;
use databend_common_storages_fuse::TableContext;
use databend_query::sessions::SessionType;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_query_kind() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let http_session = fixture
        .new_session_with_type(SessionType::HTTPQuery)
        .await?;
    let ctx = http_session.create_query_context().await?;
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
async fn test_xxx() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    fixture.execute_command("CREATE FUNCTION weighted_avg (Int32 NULL, Int32 NULL) RETURNS Float32 NULL LANGUAGE javascript HANDLER = 'fake_name' AS $$
export function create_state() {
    return {sum: 0, weight: 0};
}
export function accumulate(state, value, weight) {
    state.sum += value * weight;
    state.weight += weight;
    return state;
}
export function retract(state, value, weight) {
    state.sum -= value * weight;
    state.weight -= weight;
    return state;
}
export function merge(state1, state2) {
    state1.sum += state2.sum;
    state1.weight += state2.weight;
    return state1;
}
export function finish(state) {
    return state.sum / state.weight;
}
$$").await?;

    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql("SELECT weighted_avg(number + 1, number + 2), weighted_avg(number + 1, number + 2) FROM numbers(5)").await?;

    println!("{:?}", plan);

    Ok(())
}
