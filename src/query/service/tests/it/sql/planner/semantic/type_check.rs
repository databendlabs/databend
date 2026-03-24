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

use databend_common_catalog::table_context::TableContext;
use databend_common_expression::ColumnIndex;
use databend_common_expression::Expr;
use databend_common_sql::Planner;
use databend_common_sql::parse_exprs;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_query_overflow() -> anyhow::Result<()> {
    // Construct the SQL query with many OR conditions
    let mut query = String::from("1 = 1 AND (");
    let condition = "(timestamp = '2024-05-05 18:05:20' AND type = '1' AND id = 'xx')";

    for _ in 0..299 {
        // Adjust the count based on your specific test needs
        query.push_str(condition);
        query.push_str(" OR ");
    }
    query.push_str(condition);
    query.push_str(");");

    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    fixture
        .execute_command("CREATE table default.t1(timestamp timestamp, id int, type string);")
        .await?;
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog
        .get_table(&fixture.default_tenant(), "default", "t1")
        .await?;

    parse_exprs(ctx.clone(), table, query.as_str())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_inlist_with_null_builds_shallow_or_tree() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings()
        .set_setting("max_inlist_to_or".to_string(), "1000".to_string())?;

    fixture
        .execute_command("CREATE table default.t_inlist_balanced_or(a string);")
        .await?;
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog
        .get_table(&fixture.default_tenant(), "default", "t_inlist_balanced_or")
        .await?;

    let mut query = String::from("a in (");
    for i in 0..1000 {
        if i > 0 {
            query.push(',');
        }
        query.push('\'');
        query.push_str(&format!("value_{i}"));
        query.push('\'');
    }
    query.push_str(",NULL)");

    let exprs = parse_exprs(ctx.clone(), table, query.as_str())?;
    assert_eq!(exprs.len(), 1);
    let depth = max_or_depth(&exprs[0]);
    assert!(depth > 0, "expected OR predicates in rewritten IN list");
    assert!(
        depth <= 16,
        "expected balanced OR tree depth <= 16, got {depth}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invalid_grouping_returns_semantic_error() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());
    fixture
        .execute_command("CREATE TABLE students(course STRING, type STRING)")
        .await?;

    for sql in [
        "SELECT GROUPING()",
        "SELECT GROUPING() FROM students",
        "SELECT count() FROM students WHERE GROUPING() = 0 GROUP BY course",
        "SELECT count() OVER () FROM students GROUP BY course QUALIFY GROUPING() = 0",
    ] {
        let err = planner
            .plan_sql(sql)
            .await
            .expect_err("invalid grouping() should return a semantic error");
        assert!(
            err.message()
                .contains("grouping requires at least one argument"),
            "unexpected error for `{sql}`: {err}"
        );
    }

    Ok(())
}

fn max_or_depth<I: ColumnIndex>(expr: &Expr<I>) -> usize {
    match expr {
        Expr::Cast(cast) => max_or_depth(&cast.expr),
        Expr::FunctionCall(function_call) => {
            let child_depth = function_call
                .args
                .iter()
                .map(max_or_depth)
                .max()
                .unwrap_or(0);
            if function_call.function.signature.name == "or" {
                child_depth + 1
            } else {
                child_depth
            }
        }
        _ => 0,
    }
}
