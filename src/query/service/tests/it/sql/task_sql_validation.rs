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
use databend_query::test_kits::TestFixture;

// `CREATE TASK` validation runs entirely in the binder (`plan_sql`), so these
// tests only plan the statement — the interpreter / cloud-control server is not
// needed and the task is never executed.
async fn plan_err(fixture: &TestFixture, sql: &str) -> Option<ErrorCode> {
    let ctx = fixture.new_query_ctx().await.unwrap();
    let mut planner = Planner::new(ctx.clone());
    planner.plan_sql(sql).await.err()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_task_sql_validation() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;

    // Syntactically invalid SQL is rejected (SyntaxException).
    let err = plan_err(
        &fixture,
        "CREATE TASK t WAREHOUSE = 'w' SCHEDULE = 1 SECOND AS selec a",
    )
    .await
    .expect("syntax error should be rejected");
    assert_eq!(err.code(), ErrorCode::SYNTAX_EXCEPTION);

    // Semantically invalid SQL is rejected even though it parses fine:
    // array subscript `[]` only accepts a literal index, `number % 3` is not.
    let err = plan_err(
        &fixture,
        "CREATE TASK t WAREHOUSE = 'w' SCHEDULE = 1 SECOND AS \
         select ARRAY('a','b','c')[number % 3] from numbers(3)",
    )
    .await
    .expect("semantic error should be rejected");
    assert_eq!(err.code(), ErrorCode::SEMANTIC_ERROR);

    // A syntactically valid statement that references a not-yet-existing table
    // must still succeed: we do not fail on missing objects.
    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());
    planner
        .plan_sql(
            "CREATE TASK t WAREHOUSE = 'w' SCHEDULE = 1 SECOND AS \
             select * from a_table_that_does_not_exist",
        )
        .await
        .expect("missing table must be tolerated at task creation");

    // Missing columns now use UnknownColumn, so task validation must tolerate them.
    for sql in [
        "SELECT missing_column FROM numbers(3)",
        "SELECT \"missing_column\" FROM numbers(3)",
    ] {
        let err = plan_err(&fixture, sql)
            .await
            .expect("column must not exist");
        assert_eq!(err.code(), ErrorCode::UNKNOWN_COLUMN);

        for prefix in [
            "CREATE TASK t WAREHOUSE = 'w' SCHEDULE = 1 SECOND AS",
            "ALTER TASK t MODIFY AS",
        ] {
            let ctx = fixture.new_query_ctx().await?;
            Planner::new(ctx)
                .plan_sql(&format!("{prefix} {sql}"))
                .await?;
        }
    }

    // A valid statement plans fine.
    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());
    planner
        .plan_sql("CREATE TASK t WAREHOUSE = 'w' SCHEDULE = 1 SECOND AS select 1")
        .await
        .expect("valid task sql must plan");

    Ok(())
}
