// Copyright 2026 Datafuse Labs
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

use databend_common_exception::Result;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnEntry;
use databend_common_sql::Planner;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::Limit;
use databend_common_sql::plans::PhysicalRelOperator;
use databend_common_sql::plans::Plan;
use databend_query::physical_plans::HashJoin;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanBuilder;
use databend_query::physical_plans::UnionAll;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;

// SQL binding supplies real union output symbols/coercions and physical scan
// IDs. Result-only tests cannot establish that both scan targets were restored.
#[tokio::test(flavor = "multi_thread")]
async fn test_union_probe_targets() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    for sql in [
        "CREATE TABLE rf_union_a (a Int64 NOT NULL) ENGINE=FUSE",
        "CREATE TABLE rf_union_b (b Int64 NULL) ENGINE=FUSE",
        "CREATE TABLE rf_union_c (a Int32 NOT NULL) ENGINE=FUSE",
        "CREATE TABLE rf_union_d (k Int64 NULL) ENGINE=FUSE",
        "INSERT INTO rf_union_a VALUES (1), (1), (2), (4)",
        "INSERT INTO rf_union_b VALUES (1), (3), (NULL)",
        "INSERT INTO rf_union_c VALUES (1), (2), (4)",
        "INSERT INTO rf_union_d VALUES (1), (3)",
    ] {
        fixture.execute_command(sql).await?;
    }
    for (name, probe, expected) in [
        ("base regression", "SELECT a AS k FROM rf_union_a", vec![
            "a",
        ]),
        (
            "identity aliases and nullable union",
            "SELECT x AS k FROM (SELECT a AS x FROM rf_union_a) t UNION ALL SELECT b AS k FROM rf_union_b",
            vec!["a", "b"],
        ),
        (
            "limited branch",
            "(SELECT a AS k FROM rf_union_a ORDER BY a LIMIT 1) UNION ALL SELECT b AS k FROM rf_union_b",
            vec!["b"],
        ),
        (
            "window branch",
            "SELECT a AS k FROM (SELECT a, row_number() OVER (ORDER BY a) AS rn FROM rf_union_a) t WHERE rn = 1 UNION ALL SELECT b AS k FROM rf_union_b",
            vec!["b"],
        ),
        (
            "stateful projection above union",
            "SELECT k, running_difference(k) AS d FROM (SELECT a AS k FROM rf_union_a UNION ALL SELECT b AS k FROM rf_union_b) t",
            vec![],
        ),
        (
            "stateful projection in one branch",
            "SELECT a AS k, running_difference(a) AS d FROM rf_union_a UNION ALL SELECT b AS k, 0 AS d FROM rf_union_b",
            vec!["b"],
        ),
        (
            "numeric union coercion",
            "SELECT a AS k FROM rf_union_c UNION ALL SELECT b AS k FROM rf_union_b",
            vec!["b"],
        ),
        (
            "computed branch",
            "SELECT a + 1 AS k FROM rf_union_a UNION ALL SELECT b AS k FROM rf_union_b",
            vec!["b"],
        ),
        (
            "internal shared occurrence",
            "SELECT a AS k FROM rf_union_a UNION ALL SELECT b AS k FROM rf_union_b",
            vec![],
        ),
        (
            "safe root expression",
            "SELECT a AS k FROM rf_union_a UNION ALL SELECT b AS k FROM rf_union_b",
            vec!["a", "b"],
        ),
    ] {
        let ctx = fixture.new_query_ctx().await?;
        // Fix direction only; keep CBO and normal scalar/union rewrites enabled.
        ctx.get_settings()
            .set_setting("disable_join_reorder".to_string(), "1".to_string())?;
        let key = if name == "safe root expression" {
            "u.k + 1"
        } else {
            "u.k"
        };
        // Keep the non-key stateful output live, so column pruning cannot
        // remove the expression whose input rows this test must protect.
        let sql = format!("SELECT u.* FROM ({probe}) u LEFT SEMI JOIN rf_union_d d ON {key} = d.k");
        let (plan, _) = Planner::new(ctx.clone()).plan_sql(&sql).await?;
        let Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } = plan
        else {
            panic!("{name}: expected query");
        };
        let query = s_expr.planned()?;
        let physical = if name == "internal shared occurrence" {
            // This is deliberately NOT a SQL reproduction of shared scan IDs.
            // Reuse the exact bound producer and its symbols in two union
            // consumers; the second has LIMIT. No unrelated symbol collisions.
            let (expr, changed) = share_union_input(query.expr());
            assert!(changed);
            PhysicalPlanBuilder::new(metadata.clone(), ctx, false)
                .build(&expr, bind_context.column_set())
                .await?
        } else {
            PhysicalPlanBuilder::new(metadata.clone(), ctx, false)
                .build_query(query, bind_context.column_set())
                .await?
        };
        let join = find_plan::<HashJoin>(&physical).expect("outer hash join");
        assert_eq!(join.join_type, JoinType::LeftSemi, "{name}");
        if name != "base regression" {
            assert!(
                find_plan::<UnionAll>(&join.probe).is_some(),
                "{name}: test must retain UNION ALL"
            );
        }
        if expected.is_empty() {
            assert!(
                join.runtime_filter.filters.is_empty(),
                "{name}: unsafe shared consumer must block both occurrences"
            );
            continue;
        }
        assert_eq!(join.runtime_filter.filters.len(), 1, "{name}");
        let filter = &join.runtime_filter.filters[0];
        let mut actual = Vec::new();
        for (key, scan_id) in &filter.probe_targets {
            let expr = key.as_expr(&BUILTIN_FUNCTIONS);
            assert_eq!(
                expr.data_type(),
                filter.build_key.as_expr(&BUILTIN_FUNCTIONS).data_type(),
                "{name}: common hash type"
            );
            let refs = expr.column_refs();
            assert_eq!(refs.len(), 1, "{name}");
            let metadata = metadata.read();
            let column = metadata
                .columns()
                .iter()
                .find_map(|entry| {
                    let ColumnEntry::BaseTableColumn(column) = entry else {
                        return None;
                    };
                    (metadata.base_column_scan_id(column.column_index) == Some(*scan_id))
                        .then_some(column.column_name.clone())
                })
                .expect("target must identify a bound base scan");
            assert!(
                refs.contains_key(&column),
                "{name}: physical column, not union alias"
            );
            actual.push(column);
        }
        actual.sort();
        assert_eq!(actual, expected, "{name}");
        if name != "base regression" {
            assert_eq!(
                filter.probe_table_rows, None,
                "{name}: no borrowed root denominator"
            );
        }
    }
    Ok(())
}

fn share_union_input(expr: &PExpr) -> (PExpr, bool) {
    if let PhysicalRelOperator::UnionAll(union) = expr.plan() {
        let mut union = union.clone();
        union.right_outputs = union.left_outputs.clone();
        let producer = expr.left_child_arc();
        let limited = PExpr::create_unary(
            Limit {
                before_exchange: false,
                limit: Some(1),
                offset: 0,
                lazy_columns: Default::default(),
            },
            producer.clone(),
        );
        return (
            expr.replace_plan(PhysicalRelOperator::UnionAll(union))
                .replace_children([producer, Arc::new(limited)]),
            true,
        );
    }
    let mut changed = false;
    let children = expr
        .children()
        .map(|child| {
            let (child, replaced) = if changed {
                (child.clone(), false)
            } else {
                share_union_input(child)
            };
            changed |= replaced;
            Arc::new(child)
        })
        .collect::<Vec<_>>();
    (expr.replace_children(children), changed)
}

fn find_plan<T: 'static>(plan: &PhysicalPlan) -> Option<&T> {
    plan.as_any()
        .downcast_ref::<T>()
        .or_else(|| plan.children().find_map(find_plan::<T>))
}
