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

use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;
use databend_query::physical_plans::EvalScalar;
use databend_query::physical_plans::MaterializeCTERef;
use databend_query::physical_plans::MaterializedCTE;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanBuilder;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grouping_sets_project_redefined_keys_once() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    for (union, cascade, skip) in [
        ("0", "0", ""),
        ("1", "0", ""),
        ("1", "1", ""),
        ("1", "1", "HierarchicalGroupingSetsToUnion"),
    ] {
        let ctx = fixture.new_query_ctx().await?;
        for (name, value) in [
            ("grouping_sets_to_union", union),
            ("enable_cascading_grouping_sets", cascade),
            ("optimizer_skip_list", skip),
        ] {
            ctx.get_settings()
                .set_setting(name.to_string(), value.to_string())?;
        }
        let sql = "WITH c AS (
            SELECT a, b, sum(v) AS s
            FROM (VALUES (1, 2, 3), (1, 3, 4)) t(a, b, v)
            GROUP BY ROLLUP(a, b)
        )
        SELECT a, sum(s), grouping(a) FROM c GROUP BY ROLLUP(a)";
        let (plan, _) = Planner::new(ctx.clone()).plan_sql(sql).await?;
        let Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } = plan
        else {
            unreachable!("expected a query plan");
        };
        let plan = PhysicalPlanBuilder::new(metadata, ctx, false)
            .build_query(s_expr.planned()?, bind_context.column_set())
            .await?;
        let mut producer_schemas = HashMap::new();
        collect_producer_schemas(&plan, &mut producer_schemas)?;
        assert_consistent_outputs(&plan, &producer_schemas)?;
    }
    Ok(())
}

fn collect_producer_schemas(
    plan: &PhysicalPlan,
    schemas: &mut HashMap<String, DataSchemaRef>,
) -> Result<()> {
    if let Some(producer) = plan.as_any().downcast_ref::<MaterializedCTE>() {
        schemas.insert(producer.cte_name.clone(), producer.input.output_schema()?);
    }
    for child in plan.children() {
        collect_producer_schemas(child, schemas)?;
    }
    Ok(())
}

fn assert_consistent_outputs(
    plan: &PhysicalPlan,
    producer_schemas: &HashMap<String, DataSchemaRef>,
) -> Result<()> {
    if plan.as_any().is::<EvalScalar>() {
        let schema = plan.output_schema()?;
        let mut names = HashSet::new();
        for field in schema.fields() {
            assert!(
                names.insert(field.name()),
                "EvalScalar must replace an existing symbol, not project it twice: {schema:?}"
            );
        }
    }
    if let Some(consumer) = plan.as_any().downcast_ref::<MaterializeCTERef>() {
        // Grouping-set consumers use identity column mappings. Their schema must
        // reflect the producer's nullable projections, not the base-table metadata.
        let producer = &producer_schemas[&consumer.cte_name];
        for field in consumer.cte_schema.fields() {
            assert_eq!(
                field.data_type(),
                producer.field_with_name(field.name())?.data_type(),
                "CTE consumer type differs from its producer: {}",
                consumer.cte_name
            );
        }
    }
    for child in plan.children() {
        assert_consistent_outputs(child, producer_schemas)?;
    }
    Ok(())
}
