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

use std::sync::Arc;
use std::time::Duration;

use databend_common_expression::ColumnIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_sql::MetadataRef;
use databend_common_sql::Planner;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_query::physical_plans::PhysicalPlanBuilder;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_planner_cache_folds_statement_stable_subquery_per_physical_plan() -> anyhow::Result<()>
{
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command(
            "CREATE TABLE default.cache_statement_stable_subquery(ts TIMESTAMP) ENGINE = FUSE",
        )
        .await?;
    fixture
        .execute_command(
            "INSERT INTO default.cache_statement_stable_subquery VALUES ('2020-01-01 00:00:00')",
        )
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings()
        .set_setting("enable_planner_cache".to_string(), "1".to_string())?;
    let sql = "SELECT * FROM default.cache_statement_stable_subquery \
               WHERE ts < (SELECT now())";

    let (first_metadata, first_now) = plan_and_extract_scan_timestamp(ctx.clone(), sql).await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let (second_metadata, second_now) = plan_and_extract_scan_timestamp(ctx.clone(), sql).await?;

    assert!(
        Arc::ptr_eq(&first_metadata, &second_metadata),
        "the second statement should reuse the cached logical plan"
    );
    assert!(
        second_now > first_now,
        "the Scan pushdown must use the current statement timestamp: \
         first={first_now}, second={second_now}"
    );

    let plan_shaping_sql = "SELECT t.ts FROM default.cache_statement_stable_subquery AS t, \
                            numbers(to_uint64(to_second(now())))";
    let first = plan_query(ctx.clone(), plan_shaping_sql).await?;
    let second = plan_query(ctx.clone(), plan_shaping_sql).await?;
    assert!(
        !Arc::ptr_eq(query_metadata(&first), query_metadata(&second)),
        "execution-dependent table-function arguments must be rebound"
    );

    ctx.get_settings()
        .set_setting("inlist_to_join_threshold".to_string(), "4".to_string())?;
    let in_list_sql = "SELECT * FROM default.cache_statement_stable_subquery \
                       WHERE to_second(ts) IN (100.0, 101.0, 102.0, to_second(now()))";
    let first = plan_query(ctx.clone(), in_list_sql).await?;
    let first_second = constant_scan_last_second(&first);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let second = plan_query(ctx, in_list_sql).await?;
    let second_second = constant_scan_last_second(&second);
    assert!(
        !Arc::ptr_eq(query_metadata(&first), query_metadata(&second)),
        "a statement-local constant table must not enter the planner cache"
    );
    assert_ne!(
        first_second, second_second,
        "the stable IN-list value must be materialized again for each statement"
    );
    Ok(())
}

async fn plan_query(ctx: Arc<QueryContext>, sql: &str) -> anyhow::Result<Plan> {
    let mut planner = Planner::new(ctx);
    let (plan, _) = planner.plan_sql(sql).await?;
    Ok(plan)
}

fn query_metadata(plan: &Plan) -> &MetadataRef {
    let Plan::Query { metadata, .. } = plan else {
        panic!("expected query plan")
    };
    metadata
}

fn constant_scan_last_second(plan: &Plan) -> i64 {
    let Plan::Query { s_expr, .. } = plan else {
        panic!("expected query plan")
    };
    let scan = find_constant_scan(s_expr).expect("expected a ConstantTableScan");
    let value = scan.values[0]
        .index(scan.num_rows - 1)
        .expect("expected the last IN-list value");
    match value {
        ScalarRef::Decimal(value) => value.to_float64() as i64,
        other => panic!("expected a decimal second, got {other:?}"),
    }
}

fn find_constant_scan(s_expr: &SExpr) -> Option<&databend_common_sql::plans::ConstantTableScan> {
    if let RelOperator::ConstantTableScan(scan) = s_expr.plan() {
        return Some(scan);
    }
    s_expr.children().find_map(find_constant_scan)
}

async fn plan_and_extract_scan_timestamp(
    ctx: Arc<QueryContext>,
    sql: &str,
) -> anyhow::Result<(MetadataRef, i64)> {
    let plan = plan_query(ctx.clone(), sql).await?;
    let Plan::Query {
        s_expr,
        metadata,
        bind_context,
        ..
    } = plan
    else {
        panic!("expected query plan")
    };

    let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx, false);
    let physical = builder.build(&s_expr, bind_context.column_set()).await?;
    let source = physical
        .try_find_single_data_source()
        .expect("expected one table scan");
    let filter = &source
        .push_downs
        .as_ref()
        .and_then(|push_downs| push_downs.filters.as_ref())
        .expect("expected a Scan pushdown filter")
        .filter;
    let mut timestamps = Vec::new();
    collect_timestamp_constants(filter, &mut timestamps);
    let [statement_now] = timestamps.as_slice() else {
        panic!("expected one timestamp literal in Scan pushdown, got {timestamps:?}")
    };
    Ok((metadata, *statement_now))
}

fn collect_timestamp_constants<Index: ColumnIndex>(
    expr: &RemoteExpr<Index>,
    timestamps: &mut Vec<i64>,
) {
    match expr {
        RemoteExpr::Constant {
            scalar: Scalar::Timestamp(value),
            ..
        } => timestamps.push(*value),
        RemoteExpr::Cast { expr, .. } => collect_timestamp_constants(expr, timestamps),
        RemoteExpr::FunctionCall { args, .. } => {
            for arg in args {
                collect_timestamp_constants(arg, timestamps);
            }
        }
        RemoteExpr::LambdaFunctionCall {
            args, lambda_expr, ..
        } => {
            for arg in args {
                collect_timestamp_constants(arg, timestamps);
            }
            collect_timestamp_constants(lambda_expr, timestamps);
        }
        RemoteExpr::Constant { .. } | RemoteExpr::ColumnRef { .. } => {}
    }
}
