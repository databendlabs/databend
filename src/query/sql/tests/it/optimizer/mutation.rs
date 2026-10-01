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

use std::io::Write;

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::binder::MutationStrategy;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::MutationPlan;
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_sql::optimizer::optimize;
use databend_common_sql::plans::Plan;

use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

#[derive(Clone, Copy, Default)]
struct ExpectedMutation {
    direct: bool,
    truncate: bool,
    no_effect: bool,
    empty_target: bool,
    local_retry: bool,
    predicate_column: bool,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_mutation_preparation() -> Result<()> {
    let mut file = open_golden_file("optimizer", "mutation.txt")?;
    let cases = [
        (
            "direct_update",
            "UPDATE target SET v = v + 1 WHERE k > 10",
            ExpectedMutation {
                direct: true,
                predicate_column: true,
                ..Default::default()
            },
        ),
        (
            "direct_update_false",
            "UPDATE target SET v = v + 1 WHERE false",
            ExpectedMutation {
                direct: true,
                no_effect: true,
                ..Default::default()
            },
        ),
        (
            "direct_delete_filter",
            "DELETE FROM target WHERE k > 10",
            ExpectedMutation {
                direct: true,
                ..Default::default()
            },
        ),
        (
            "direct_delete_all",
            "DELETE FROM target",
            ExpectedMutation {
                direct: true,
                truncate: true,
                ..Default::default()
            },
        ),
        (
            "direct_delete_true",
            "DELETE FROM target WHERE true",
            ExpectedMutation {
                direct: true,
                truncate: true,
                ..Default::default()
            },
        ),
        (
            "direct_delete_false",
            "DELETE FROM target WHERE false",
            ExpectedMutation {
                direct: true,
                no_effect: true,
                ..Default::default()
            },
        ),
        (
            "subquery_update",
            "UPDATE target SET v = v + 1 WHERE k IN (SELECT k FROM source)",
            ExpectedMutation::default(),
        ),
        (
            "subquery_delete",
            "DELETE FROM target WHERE k IN (SELECT k FROM source)",
            ExpectedMutation::default(),
        ),
        (
            "matched_merge",
            "MERGE INTO target t USING source s ON t.k = s.k WHEN MATCHED THEN UPDATE SET v = s.v",
            ExpectedMutation::default(),
        ),
        (
            "insert_only_merge",
            "MERGE INTO target t USING source s ON t.k = s.k WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)",
            ExpectedMutation::default(),
        ),
        (
            "mixed_merge",
            "MERGE INTO target t USING source s ON t.k = s.k WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)",
            ExpectedMutation::default(),
        ),
        (
            "aggregate_source_local_retry",
            "MERGE INTO target t USING (SELECT max(k) AS k, max(v) AS v FROM source) s ON t.k = s.k WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)",
            ExpectedMutation {
                local_retry: true,
                ..Default::default()
            },
        ),
        (
            "empty_target_local_retry",
            "MERGE INTO target t USING (SELECT max(k) AS k, max(v) AS v FROM source) s ON t.k = s.k AND t.k > 10 AND t.k < 0 WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)",
            ExpectedMutation {
                empty_target: true,
                local_retry: true,
                ..Default::default()
            },
        ),
        (
            "empty_target_merge",
            "MERGE INTO target t USING source s ON t.k = s.k AND t.k > 10 AND t.k < 0 WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)",
            ExpectedMutation {
                empty_target: true,
                ..Default::default()
            },
        ),
        (
            "empty_source_merge",
            "MERGE INTO target t USING (SELECT * FROM source WHERE false) s ON t.k = s.k WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)",
            ExpectedMutation {
                no_effect: true,
                ..Default::default()
            },
        ),
    ];
    for (name, sql, expected) in cases {
        for distributed in [false, true] {
            let case = SqlTestCase {
                name,
                description: "Mutation logical preparation precedes plan selection; distribution finalization keeps its legacy policy.",
                setup_sqls: &[
                    "CREATE TABLE target(k Int64 NOT NULL, v Int64 NOT NULL)",
                    "CREATE TABLE source(k Int64 NOT NULL, v Int64 NOT NULL)",
                ],
                sql,
            };
            let ctx = LiteTableContext::create().await?;
            ctx.set_cluster_node_num(if distributed { 2 } else { 1 });
            ctx.set_table_warehouse_distribution(distributed);
            for setup in case.setup_sqls {
                ctx.register_setup_sql(setup).await?;
            }
            let raw = ctx.bind_sql(sql).await?;
            let Plan::DataMutation {
                s_expr: bound_expr,
                metadata,
                schema,
            } = &raw
            else {
                unreachable!()
            };
            assert!(matches!(bound_expr.as_ref(), MutationPlan::Logical(_)));
            assert!(
                bound_expr.planned().is_err(),
                "bound mutation must not reach execution"
            );
            raw.capture_bound_query_lineage();
            let bound_lineage = raw.query_lineage()?;
            let bound_udfs = bound_expr
                .input_udfs()?
                .into_iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>();
            let opt_ctx =
                OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?)
                    .with_settings(&ctx.get_settings())?;
            opt_ctx.set_enable_distributed_optimization(distributed);
            let optimized = optimize(opt_ctx, raw.clone()).await?;
            let Plan::DataMutation {
                s_expr,
                schema: optimized_schema,
                ..
            } = &optimized
            else {
                unreachable!()
            };
            assert!(matches!(s_expr.as_ref(), MutationPlan::Planned(_)));
            assert!(
                s_expr.logical().is_err(),
                "planned mutation must not reenter logical preparation"
            );
            assert_eq!(bound_lineage, optimized.query_lineage()?);
            assert_eq!(
                bound_udfs,
                s_expr.input_udfs()?.into_iter().cloned().collect()
            );
            let s_expr = s_expr.planned()?;
            let mutation = s_expr.plan().as_mutation().unwrap();
            assert_eq!(
                schema, optimized_schema,
                "bound result schema changed: {name}"
            );
            assert_eq!(
                mutation.strategy == MutationStrategy::Direct,
                expected.direct,
                "{name}"
            );
            assert_eq!(mutation.truncate_table, expected.truncate, "{name}");
            assert_eq!(mutation.no_effect, expected.no_effect, "{name}");
            assert_eq!(
                mutation.predicate_column_index.is_some(),
                expected.predicate_column,
                "{name}"
            );
            if expected.empty_target {
                assert!(
                    !mutation.no_effect,
                    "unmatched inserts must survive an empty target"
                );
                assert!(
                    mutation
                        .matched_evaluators
                        .iter()
                        .all(|action| action.update.is_none())
                );
                assert!(!mutation.can_try_update_column_only);
            }
            if expected.local_retry && distributed {
                assert!(
                    !mutation.distributed,
                    "an internal Merge requires local retry"
                );
                assert!(!s_expr.child(0)?.has_merge_exchange());
            }
            if expected.direct && !expected.truncate && !expected.no_effect {
                assert!(!mutation.direct_filter.is_empty(), "{name}");
            }
            if let Some(predicate) = mutation.predicate_column_index {
                assert!(mutation.required_columns.contains(&predicate), "{name}");
            }
            s_expr.child(0)?.validate_types(metadata)?;
            s_expr.child(0)?.validate_column_scope(metadata)?;
            if !distributed {
                write_case_header(&mut file, &case)?;
                writeln!(
                    file,
                    "raw_plan:\n{}",
                    raw.format_indent(Default::default(), &StatContext::default())?
                )?;
            }
            writeln!(file, "requested_distributed: {distributed}")?;
            writeln!(
                file,
                "optimized_plan:\n{}",
                optimized.format_indent(Default::default(), &StatContext::default())?
            )?;
            writeln!(
                file,
                "mutation_state: strategy={:?}, distributed={}, row_id_shuffle={}, no_effect={}, truncate={}, predicate_column={:?}, direct_filter_count={}, matched_update_count={}",
                mutation.strategy,
                mutation.distributed,
                mutation.row_id_shuffle,
                mutation.no_effect,
                mutation.truncate_table,
                mutation.predicate_column_index,
                mutation.direct_filter.len(),
                mutation
                    .matched_evaluators
                    .iter()
                    .filter(|action| action.update.is_some())
                    .count()
            )?;
            writeln!(file)?;
        }
    }
    Ok(())
}
