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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::setup_context;

async fn optimize(case: &SqlTestCase, settings: &[(&str, &str)]) -> Result<()> {
    let ctx = setup_context(case).await?;
    for (name, value) in settings {
        ctx.get_settings()
            .set_setting((*name).to_string(), (*value).to_string())?;
    }
    let plan = ctx.bind_sql(case.sql).await?;
    ctx.optimize_plan(plan).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_optimizer_rewrites_keep_function_return_types_consistent() -> Result<()> {
    let grouping_sets = SqlTestCase {
        name: "grouping_sets_rewrites_refresh_function_return_types",
        description: "",
        setup_sqls: &["CREATE TABLE grouping_type_t(a Int64, b Int64, c Int64)"],
        sql: "SELECT a, b, c, a + 8, b + c \
              FROM grouping_type_t GROUP BY ROLLUP(a, b, c)",
    };
    for grouping_sets_to_union in ["0", "1"] {
        optimize(&grouping_sets, &[(
            "grouping_sets_to_union",
            grouping_sets_to_union,
        )])
        .await?;
    }

    let cases = [
        SqlTestCase {
            name: "filter_join_rewrite_refreshes_function_return_types",
            description: "",
            setup_sqls: &[
                "CREATE TABLE rewrite_type_t1(a Int64, b Int64)",
                "CREATE TABLE rewrite_type_t2(a Int64, b Int64)",
            ],
            sql: "SELECT * FROM rewrite_type_t1 t1 JOIN rewrite_type_t2 t2 \
                  ON t1.a = t2.a AND t1.b BETWEEN t2.b AND t2.b + 2 WHERE t2.b = 3",
        },
        SqlTestCase {
            name: "in_subquery_rewrite_refreshes_function_return_types",
            description: "",
            setup_sqls: &["CREATE TABLE rewrite_type_t3(a Int64, b Int64)"],
            sql: "SELECT * FROM rewrite_type_t3 \
                  WHERE a IN (SELECT x FROM (VALUES (1), (2), (3), (4), (5), (6)) t(x))",
        },
        SqlTestCase {
            name: "self_join_refreshes_function_return_types",
            description: "",
            setup_sqls: &["CREATE TABLE rewrite_settings(name String, value String)"],
            sql: "SELECT e1.name, e2.name, e1.value, e2.value FROM rewrite_settings e1 \
                  LEFT JOIN rewrite_settings e2 ON e1.name = e2.name \
                  WHERE e1.name = 'max_threads'",
        },
        SqlTestCase {
            name: "nullable_tuple_cast_to_variant_keeps_function_signature",
            description: "",
            setup_sqls: &["CREATE TABLE tuple_variant_t(a Nullable(Tuple(x Int64, y String)))"],
            sql: "SELECT CAST(a AS VARIANT) FROM tuple_variant_t",
        },
    ];
    for case in &cases {
        optimize(case, &[]).await?;
    }

    let folded_in = SqlTestCase {
        name: "folded_in_predicate_preserves_nullability",
        description: "",
        setup_sqls: &["CREATE TABLE folded_in_type_t(a Nullable(Int64))"],
        sql: "SELECT * FROM folded_in_type_t WHERE a IN (1, 2, 3, 1, 2, 3)",
    };
    optimize(&folded_in, &[
        ("inlist_to_join_threshold", "6"),
        ("max_inlist_to_or", "2"),
    ])
    .await
}
