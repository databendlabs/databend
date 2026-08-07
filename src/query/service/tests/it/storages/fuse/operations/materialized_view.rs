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

use databend_common_exception::ErrorCode;
use databend_common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_rejects_stale_binding_at_execute_entry() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let database = fixture.default_db_name();
    let source = "mv_refresh_source";
    let materialized_view = "mv_refresh";
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{source} (a INT, b INT) CHANGE_TRACKING = TRUE"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE MATERIALIZED VIEW {database}.{materialized_view} AS \
             SELECT a FROM {database}.{source}"
        ))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());
    let refresh_sql = format!("REFRESH MATERIALIZED VIEW {database}.{materialized_view}");
    let (refresh_plan, _) = planner.plan_sql(&refresh_sql).await?;

    // Invalidate the binding after planning. Executing the already-built plan must consult the
    // exact binding again at the interpreter entry instead of trusting planner-time metadata.
    fixture
        .execute_command(&format!(
            "ALTER TABLE {database}.{source} RENAME COLUMN b TO renamed_b"
        ))
        .await?;

    // A stale binding is an execution admission error, not a binder error.
    let _ = planner.plan_sql(&refresh_sql).await?;

    let interpreter = InterpreterFactory::get(ctx.clone(), &refresh_plan).await?;
    let error = interpreter
        .execute(ctx)
        .await
        .err()
        .expect("refresh with a stale source binding must fail");
    assert_eq!(error.code(), ErrorCode::INVALID_MATERIALIZED_VIEW);

    Ok(())
}
