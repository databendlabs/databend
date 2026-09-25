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

use databend_common_catalog::cluster_info::Cluster;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;
use databend_meta_client::types::NodeInfo;
use databend_query::clusters::ClusterHelper;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanBuilder;
use databend_query::physical_plans::Window;
use databend_query::physical_plans::WindowGroup;
use databend_query::physical_plans::WindowPartition;
use databend_query::sessions::TableContextCluster;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::expects_ok;

#[tokio::test(flavor = "multi_thread")]
async fn test_window_inputs_prune_json_after_evaluation() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command("CREATE TABLE window_json_inputs (d VARIANT)")
        .await?;

    let cases = [
        // The original SELECT/QUALIFY shape must reuse the window key columns.
        (
            "SELECT try_cast(d:id AS BIGINT) AS id, try_cast(d:ts AS BIGINT) AS ts \
          FROM window_json_inputs \
          QUALIFY row_number() OVER (PARTITION BY id ORDER BY ts DESC) = 1",
            false,
        ),
        // Reuse also applies to a subexpression and a QUALIFY predicate.
        (
            "SELECT try_cast(d:id AS BIGINT) + 1 FROM window_json_inputs \
          QUALIFY row_number() OVER (PARTITION BY try_cast(d:id AS BIGINT) \
          ORDER BY try_cast(d:ts AS BIGINT)) = 1 AND try_cast(d:id AS BIGINT) > 0",
            false,
        ),
        // Multiple windows canonicalize their partition/order input IDs.
        (
            "SELECT try_cast(d:id AS BIGINT) AS id, \
          row_number() OVER (PARTITION BY id ORDER BY try_cast(d:ts AS BIGINT)) AS rn, \
          rank() OVER (PARTITION BY id ORDER BY try_cast(d:ts AS BIGINT) DESC) AS r \
          FROM window_json_inputs",
            false,
        ),
        // A filter cannot move below the group that produces its reused key.
        (
            "SELECT try_cast(d:id AS BIGINT) AS id, \
          row_number() OVER (PARTITION BY id ORDER BY try_cast(d:ts AS BIGINT)) AS rn, \
          rank() OVER (PARTITION BY id ORDER BY try_cast(d:ts AS BIGINT) DESC) AS r \
          FROM window_json_inputs QUALIFY id > 0",
            false,
        ),
        // RANGE planning must use the evaluated order column's type.
        (
            "SELECT try_cast(d:ts AS BIGINT) AS ts, \
          sum(try_cast(d:id AS BIGINT)) OVER (ORDER BY ts \
          RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM window_json_inputs",
            false,
        ),
        // The parent still needs the original JSON in these cases.
        (
            "SELECT d FROM window_json_inputs QUALIFY row_number() OVER \
          (PARTITION BY try_cast(d:id AS BIGINT) ORDER BY try_cast(d:ts AS BIGINT)) = 1",
            true,
        ),
        (
            "SELECT d:payload FROM window_json_inputs QUALIFY row_number() OVER \
          (PARTITION BY try_cast(d:id AS BIGINT) ORDER BY try_cast(d:ts AS BIGINT)) = 1",
            true,
        ),
    ];

    for nodes in [1, 3] {
        for (sql, keep_json) in cases {
            let ctx = fixture.new_query_ctx().await?;
            ctx.get_settings()
                .set_setting("enable_planner_cache".to_string(), "0".to_string())?;
            if nodes == 3 {
                let members = (0..nodes)
                    .map(|id| {
                        let mut node = NodeInfo::create(
                            id.to_string(),
                            String::new(),
                            String::new(),
                            String::new(),
                            String::new(),
                            String::new(),
                            String::new(),
                        );
                        node.cluster_id = "cluster_id".to_string();
                        node.warehouse_id = "warehouse_id".to_string();
                        Arc::new(node)
                    })
                    .collect();
                ctx.set_cluster(Cluster::create(members, "0".to_string()));
            }
            let (plan, _) = Planner::new(ctx.clone()).plan_sql(sql).await?;
            let Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } = plan
            else {
                panic!("expected query plan");
            };
            let plan = PhysicalPlanBuilder::new(metadata, ctx, false)
                .build(&s_expr, bind_context.column_set())
                .await?;
            assert!(
                check_window_input_json(&plan, keep_json, &format!("{nodes} nodes: {sql}"))? > 0
            );
        }
    }
    Ok(())
}

fn check_window_input_json(plan: &PhysicalPlan, keep_json: bool, sql: &str) -> Result<usize> {
    let is_window = plan.as_any().is::<Window>()
        || plan.as_any().is::<WindowGroup>()
        || plan.as_any().is::<WindowPartition>();
    let mut count = usize::from(is_window);
    for child in plan.children() {
        if is_window {
            let schema = if plan.as_any().is::<WindowPartition>() {
                // WindowPartition projects before its buffering processors.
                plan.output_schema()?
            } else {
                child.output_schema()?
            };
            let has_json = schema
                .fields()
                .iter()
                .any(|field| field.data_type().remove_nullable() == DataType::Variant);
            assert_eq!(has_json, keep_json, "window input schema for {sql}");
        }
        count += check_window_input_json(child, keep_json, sql)?;
    }
    Ok(count)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_window_grouping_over_rollup() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();

    fixture
        .execute_command(&format!("create database {db}"))
        .await?;
    fixture
        .execute_command(&format!(
            "create table {db}.empsalary (depname string, empno bigint, salary int, enroll_date date)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.empsalary values \
            ('develop', 10, 5200, '2007-08-01'), \
            ('sales', 1, 5000, '2006-10-01'), \
            ('personnel', 5, 3500, '2007-12-10'), \
            ('sales', 4, 4800, '2007-08-08'), \
            ('personnel', 2, 3900, '2006-12-23'), \
            ('develop', 7, 4200, '2008-01-01'), \
            ('develop', 9, 4500, '2008-01-01'), \
            ('sales', 3, 4800, '2007-08-01'), \
            ('develop', 8, 6000, '2006-10-01'), \
            ('develop', 11, 5200, '2007-08-15')"
        ))
        .await?;

    expects_ok(
        "window grouping over rollup",
        fixture
            .execute_query(&format!(
                "select grouping(salary), grouping(depname), \
                sum(grouping(salary)) over (partition by grouping(salary) + grouping(depname) \
                order by grouping(depname) desc) \
                from {db}.empsalary group by rollup (depname, salary) order by 1,2,3"
            ))
            .await,
        vec![
            "+----------+----------+----------+",
            "| Column 0 | Column 1 | Column 2 |",
            "+----------+----------+----------+",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 1        | 0        | 3        |",
            "| 1        | 0        | 3        |",
            "| 1        | 0        | 3        |",
            "| 1        | 1        | 1        |",
            "+----------+----------+----------+",
        ],
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unnest_aggregate_argument() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    expects_ok(
        "unnest aggregate argument",
        fixture.execute_query("select unnest(max([11,12]))").await,
        vec![
            "+----------+",
            "| Column 0 |",
            "+----------+",
            "| 11       |",
            "| 12       |",
            "+----------+",
        ],
    )
    .await?;

    Ok(())
}
