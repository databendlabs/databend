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
use databend_query::sessions::TableContextCluster;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_materialized_key_boundary_schemas() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command("CREATE TABLE key_payload (id BIGINT, d VARIANT)")
        .await?;
    fixture
        .execute_command("CREATE TABLE key_dimension (id BIGINT, x BIGINT)")
        .await?;
    fixture.execute_command("INSERT INTO key_payload VALUES (1, '{\"id\":1,\"ts\":10,\"lo\":0,\"hi\":3}'), (2, '{\"id\":2,\"ts\":20,\"lo\":1,\"hi\":4}')").await?;
    fixture
        .execute_command("INSERT INTO key_dimension VALUES (1,1),(2,2),(3,3)")
        .await?;

    let cases = [
        (
            "SELECT try_cast(d:id AS BIGINT)+1, unnest([1,2]) FROM (SELECT d FROM key_payload QUALIFY row_number() OVER(PARTITION BY try_cast(d:id AS BIGINT) ORDER BY try_cast(d:ts AS BIGINT))=1)",
            "Window",
            false,
        ),
        (
            "SELECT try_cast(d:id AS BIGINT) k FROM key_payload QUALIFY row_number() OVER(PARTITION BY k ORDER BY try_cast(d:ts AS BIGINT))=1",
            "Window",
            false,
        ),
        (
            "SELECT try_cast(d:id AS BIGINT) k, row_number() OVER(PARTITION BY k ORDER BY try_cast(d:ts AS BIGINT)), rank() OVER(PARTITION BY k ORDER BY try_cast(d:ts AS BIGINT) DESC) FROM key_payload",
            "WindowGroup",
            false,
        ),
        (
            "SELECT count(*) FROM key_dimension u JOIN key_payload t ON u.id=try_cast(t.d:id AS BIGINT)",
            "HashJoin",
            false,
        ),
        (
            "SELECT try_cast(t.d:id AS BIGINT)+1 FROM key_payload t JOIN key_dimension u ON try_cast(t.d:id AS BIGINT)=u.id",
            "HashJoin",
            false,
        ),
        (
            "SELECT try_cast(d:id AS BIGINT)+1 FROM key_payload QUALIFY row_number() OVER(PARTITION BY try_cast(d:id AS BIGINT) ORDER BY try_cast(d:ts AS BIGINT))=1 AND try_cast(d:id AS BIGINT)>0",
            "Window",
            false,
        ),
        (
            "SELECT t.id FROM key_payload t JOIN key_dimension u ON try_cast(t.d:lo AS BIGINT)<u.x AND try_cast(t.d:hi AS BIGINT)>u.x",
            "RangeJoin",
            false,
        ),
        (
            "SELECT d FROM key_payload QUALIFY row_number() OVER(PARTITION BY try_cast(d:id AS BIGINT) ORDER BY try_cast(d:ts AS BIGINT))=1",
            "Window",
            true,
        ),
        (
            "SELECT t.d FROM key_dimension u JOIN key_payload t ON u.id=try_cast(t.d:id AS BIGINT)",
            "HashJoin",
            true,
        ),
    ];
    for nodes in [1, 3] {
        for (sql, operator, keep_json) in cases {
            let ctx = fixture.new_query_ctx().await?;
            // The same SQL is planned for two different cluster topologies.
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
                metadata,
                s_expr,
                bind_context,
                ..
            } = plan
            else {
                unreachable!()
            };
            let physical = PhysicalPlanBuilder::new(metadata.clone(), ctx, false)
                .build(&s_expr, bind_context.column_set())
                .await?;
            let display = physical
                .format(&metadata.read(), Default::default())?
                .format_pretty()?;
            let (operators, json_inputs) = boundary_inputs(&physical, operator)?;
            assert!(operators > 0, "{sql}: {display}");
            assert_eq!(json_inputs > 0, keep_json, "{sql}: {display}");
            if nodes == 3 && !keep_json {
                let (exchanges, json_inputs) = boundary_inputs(&physical, "Exchange")?;
                assert!(exchanges > 0, "{sql}: {display}");
                assert_eq!(json_inputs, 0, "{sql}: {display}");
            }
        }
    }
    Ok(())
}

fn boundary_inputs(plan: &PhysicalPlan, name: &str) -> Result<(usize, usize)> {
    let boundary = plan.get_name() == name;
    let mut operators = usize::from(boundary);
    let mut json_inputs = 0;
    for child in plan.children() {
        if boundary
            && child
                .output_schema()?
                .fields()
                .iter()
                .any(|field| field.data_type().remove_nullable() == DataType::Variant)
        {
            json_inputs += 1;
        }
        let (child_operators, child_json) = boundary_inputs(child, name)?;
        operators += child_operators;
        json_inputs += child_json;
    }
    Ok((operators, json_inputs))
}
