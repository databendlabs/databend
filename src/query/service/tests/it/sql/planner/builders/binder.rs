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

use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::session_type::SessionType;
use databend_common_sql::FormatOptions;
use databend_common_sql::Planner;
use databend_common_sql::optimizer::ir::StatContext;
use databend_query::sessions::TableContextQueryInfo;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;

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
async fn test_query_level_sql_dialect_is_ignored_with_warning() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;

    for sql in [
        "SETTINGS (sql_dialect = 'mysql', max_threads = 3) SELECT 1",
        "SELECT /*+ SET_VAR(sql_dialect='mysql') SET_VAR(max_threads=3) */ 1",
    ] {
        let session = fixture
            .new_session_with_type(SessionType::HTTPQuery)
            .await?;
        let ctx = session
            .create_query_context(&databend_common_version::BUILD_INFO)
            .await?;
        let initial_dialect = ctx.get_settings().get_sql_dialect()?;

        Planner::new(ctx.clone()).plan_sql(sql).await?;

        assert_eq!(ctx.get_settings().get_sql_dialect()?, initial_dialect);
        assert_eq!(ctx.get_settings().get_max_threads()?, 3);
        assert_eq!(ctx.pop_warnings(), vec![
            "Query-level setting 'sql_dialect' is ignored because it cannot change the SQL \
             dialect of the current query"
                .to_string()
        ]);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_immediate_uses_inner_timezone_for_statistics() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command("CREATE TABLE default.execute_immediate_stats AS SELECT DATE '2022-02-02' AS d FROM numbers(10)")
        .await?;

    let mut plans = Vec::new();
    for sql in [
        "SETTINGS (timezone = 'UTC')
         SELECT count(*) FROM default.execute_immediate_stats
         WHERE to_timestamp(d) < 1643745600000000::TIMESTAMP",
        "SETTINGS (timezone = 'Asia/Shanghai')
         SELECT count(*) FROM default.execute_immediate_stats
         WHERE to_timestamp(d) < 1643745600000000::TIMESTAMP",
        "EXECUTE IMMEDIATE $$SETTINGS (timezone = 'Asia/Shanghai')
         SELECT count(*) FROM default.execute_immediate_stats
         WHERE to_timestamp(d) < 1643745600000000::TIMESTAMP$$",
    ] {
        let session = fixture
            .new_session_with_type(SessionType::HTTPQuery)
            .await?;
        let ctx = session
            .create_query_context(&databend_common_version::BUILD_INFO)
            .await?;
        ctx.get_settings()
            .set_setting("timezone".to_string(), "UTC".to_string())?;
        let (plan, _) = Planner::new(ctx.clone()).plan_sql(sql).await?;
        plans.push(plan.format_indent(
            FormatOptions { verbose: true },
            &StatContext::new(ctx.get_function_context()?),
        )?);
    }

    assert_ne!(
        plans[0], plans[1],
        "timezone must affect the derived statistics"
    );
    assert_eq!(
        plans[1], plans[2],
        "dynamic SQL must use its inner timezone"
    );
    Ok(())
}
