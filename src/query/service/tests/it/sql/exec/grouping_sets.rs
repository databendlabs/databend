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
use databend_common_expression::DataBlock;
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
use databend_query::test_kits::execute_query;
use futures_util::TryStreamExt;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grouping_sets_nested_bound_children() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let cases = [
        (
            "SELECT coalesce(CAST(a + 1 AS BIGINT), 100), coalesce(a + 1, 100), coalesce(a, 100)
             FROM (VALUES (1::INT), (2::INT)) t(a)
             GROUP BY GROUPING SETS ((a), (a + 1), (CAST(a + 1 AS BIGINT)))",
            vec![
                "100 100 1",
                "100 100 2",
                "100 2 100",
                "100 3 100",
                "2 100 100",
                "3 100 100",
            ],
        ),
        (
            "SELECT coalesce(CAST(a IS NULL AS INT), 100)
             FROM (VALUES (1::INT), (2::INT)) t(a) GROUP BY ROLLUP(a IS NULL)",
            vec!["0", "100"],
        ),
    ];
    for (union, cascade, skip) in [
        ("0", "0", ""),
        ("1", "0", ""),
        ("1", "1", ""),
        ("1", "1", "HierarchicalGroupingSetsToUnion"),
    ] {
        for (sql, expected) in &cases {
            let ctx = fixture.new_query_ctx().await?;
            for (name, value) in [
                ("grouping_sets_to_union", union),
                ("enable_cascading_grouping_sets", cascade),
                ("optimizer_skip_list", skip),
            ] {
                ctx.get_settings()
                    .set_setting(name.to_string(), value.to_string())?;
            }
            let blocks = execute_query(ctx, sql)
                .await?
                .try_collect::<Vec<DataBlock>>()
                .await?;
            let mut rows = Vec::new();
            for block in blocks {
                for row in 0..block.num_rows() {
                    rows.push(
                        block
                            .columns()
                            .iter()
                            .map(|column| column.value().index(row).unwrap().to_string())
                            .collect::<Vec<_>>()
                            .join(" "),
                    );
                }
            }
            rows.sort();
            assert_eq!(
                &rows, expected,
                "union={union}, cascade={cascade}, skip={skip}: {sql}"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_ordinary_grouping_key_binding() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let cases = [
        (
            "SELECT a, a + 1 AS k, sum(b) FROM (VALUES (1, 3), (1, 4), (2, 5)) t(a, b) GROUP BY 1, 2",
            vec!["1 2 7", "2 3 5"],
        ),
        (
            "SELECT a + 1 AS k, sum(k) FROM (VALUES (1), (1), (2)) t(a) WHERE k > 0 GROUP BY k",
            vec!["2 4", "3 3"],
        ),
        (
            "SELECT a + 1 AS k, k + 1, sum(b) AS s, s + 1 FROM (VALUES (1, 3), (1, 4), (2, 5)) t(a, b) GROUP BY ALL",
            vec!["2 3 7 8", "3 4 5 6"],
        ),
        (
            "SELECT (count(*) IS NOT NULL)::INT FROM (VALUES (1), (2)) t(a) WHERE false",
            vec!["1"],
        ),
        (
            "SELECT 1 FROM (VALUES (1), (2)) t(a) HAVING count(*) = 2",
            vec!["1"],
        ),
        (
            "SELECT 1 FROM (VALUES (1), (2)) t(a) ORDER BY sum(a)",
            vec!["1"],
        ),
        (
            "SELECT a, unnest([1, 2]) FROM (VALUES (1), (1), (2)) t(a) GROUP BY a",
            vec!["1 1", "1 2", "2 1", "2 2"],
        ),
        (
            "SELECT unnest(array_agg(a)) FROM (VALUES (1), (2)) t(a)",
            vec!["1", "2"],
        ),
        (
            "SELECT sum(unnest([a, a + 1])) FROM (VALUES (1), (2)) t(a)",
            vec!["8"],
        ),
        (
            "SELECT a, sum(unnest([a, a + 1])), unnest([a, a + 1]) FROM (VALUES (1), (1), (2)) t(a) GROUP BY a",
            vec!["1 6 1", "1 6 2", "2 5 2", "2 5 3"],
        ),
        (
            "SELECT a, unnest([a, a + 1]) AS s, count(*) FROM (VALUES (1), (1), (2)) t(a) GROUP BY a, s",
            vec!["1 1 2", "1 2 2", "2 2 1", "2 3 1"],
        ),
        (
            "SELECT a % 2 AS k, row_number() OVER (ORDER BY sum(b)) FROM (VALUES (1, 3), (1, 4), (2, 5)) t(a, b) GROUP BY k",
            vec!["0 1", "1 2"],
        ),
    ];
    for (sql, expected) in cases {
        let blocks = execute_query(fixture.new_query_ctx().await?, sql)
            .await?
            .try_collect::<Vec<DataBlock>>()
            .await?;
        let mut rows = Vec::new();
        for block in blocks {
            for row in 0..block.num_rows() {
                rows.push(
                    block
                        .columns()
                        .iter()
                        .map(|column| column.value().index(row).unwrap().to_string())
                        .collect::<Vec<_>>()
                        .join(" "),
                );
            }
        }
        rows.sort();
        assert_eq!(rows, expected, "{sql}");
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
