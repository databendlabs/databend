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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

async fn execute_query_rows(sql: &str) -> Result<usize> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = interpreter.execute(ctx).await?;
    let blocks: Vec<DataBlock> = stream.try_collect().await?;
    Ok(DataBlock::concat(&blocks)?.num_rows())
}

#[tokio::test(flavor = "multi_thread")]
async fn correlated_exists_subquery_over_union_regression() -> anyhow::Result<()> {
    let sql = r"
        SELECT *
        FROM (VALUES (1)) t(f1)
        WHERE EXISTS (
          SELECT 1
          UNION
          SELECT 2 WHERE f1 = 1
        );
    ";

    let rows = execute_query_rows(sql).await?;
    assert_eq!(rows, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn correlated_exists_subquery_over_union_all_regression() -> anyhow::Result<()> {
    let sql = r"
        SELECT *
        FROM (VALUES (1)) t(f1)
        WHERE EXISTS (
          SELECT 1
          UNION ALL
          SELECT 2 WHERE f1 = 1
        );
    ";

    let rows = execute_query_rows(sql).await?;
    assert_eq!(rows, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn correlated_join_condition_subquery_regression() -> anyhow::Result<()> {
    let cases = [
        (
            "inner_join_on",
            r"
                SELECT *
                FROM (VALUES (1), (2)) t1(a)
                WHERE EXISTS (
                    SELECT 1
                    FROM (VALUES (1), (2)) t2(a)
                    JOIN (VALUES (1)) t3(c) ON t2.a = t1.a
                )
            ",
            2,
        ),
        (
            "non_equi_join_on",
            r"
                SELECT *
                FROM (VALUES (1), (2)) t1(a)
                WHERE EXISTS (
                    SELECT 1
                    FROM (VALUES (1), (2)) t2(a)
                    JOIN (VALUES (1)) t3(c) ON t2.a > t1.a
                )
            ",
            1,
        ),
        (
            "left_join_on",
            r"
                SELECT *
                FROM (VALUES (1), (2)) t1(a)
                WHERE EXISTS (
                    SELECT 1
                    FROM (VALUES (1), (2)) t2(a)
                    LEFT JOIN (VALUES (1)) t3(c) ON t2.a = t1.a
                )
            ",
            2,
        ),
    ];

    for (case_name, sql, expected_rows) in cases {
        let rows = execute_query_rows(sql)
            .await
            .map_err(|err| anyhow::anyhow!("{case_name}: {err}"))?;
        assert_eq!(rows, expected_rows, "{case_name}");
    }

    let additional_cases = [
        (
            "aggregate_group_by",
            r"
                SELECT *
                FROM (VALUES (1), (2)) t1(a)
                WHERE EXISTS (
                    SELECT t2.b
                    FROM (VALUES (10), (20)) t2(b)
                    GROUP BY t1.a, t2.b
                )
            ",
            2,
        ),
        (
            "distinct_outer_column",
            r"
                SELECT *
                FROM (VALUES (1), (2)) t1(a)
                WHERE EXISTS (
                    SELECT DISTINCT t1.a
                    FROM (VALUES (10), (20)) t2(b)
                )
            ",
            2,
        ),
        (
            "window_partition_by_outer_column",
            r"
                SELECT (SELECT row_number() OVER (
                            PARTITION BY t1.a ORDER BY t2.b
                        )
                        FROM (VALUES (10), (20)) t2(b)
                        LIMIT 1)
                FROM (VALUES (1), (2)) t1(a)
            ",
            2,
        ),
        (
            "window_order_by_outer_column",
            r"
                SELECT (SELECT row_number() OVER (
                            ORDER BY t1.a, t2.b
                        )
                        FROM (VALUES (10), (20)) t2(b)
                        LIMIT 1)
                FROM (VALUES (1), (2)) t1(a)
            ",
            2,
        ),
        (
            "window_argument_outer_column",
            r"
                SELECT (SELECT sum(t1.a + t2.b) OVER ()
                        FROM (VALUES (10), (20)) t2(b)
                        LIMIT 1)
                FROM (VALUES (1), (2)) t1(a)
            ",
            2,
        ),
    ];

    for (case_name, sql, expected_rows) in additional_cases {
        let rows = execute_query_rows(sql)
            .await
            .map_err(|err| anyhow::anyhow!("{case_name}: {err}"))?;
        assert_eq!(rows, expected_rows, "{case_name}");
    }

    Ok(())
}
