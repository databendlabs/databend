//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! `scan_partitions` / `total_partitions` in `system_history.query_history`
//! and the Prometheus counters come from `QueryContext::get_partition_stats`,
//! a per-query sum of what pruning published; `explain analyze` reads the
//! same publications per plan id.
//!
//! The contract under test: the coordinator publishes a source's
//! `partitions_total` exactly once (`publish_planned_pruning_stats`), each
//! node that prunes publishes what it scanned (`publish_pruned_partitions`),
//! both under the scan's physical plan id. `every_pruning_mode_publishes_under_the_scan_plan_id`
//! checks that for every pruning path; the other tests cover the entry
//! points that path does not go through.

use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_catalog::plan::PartInfoType;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_storage::FaultInjection;
use databend_common_storage::FaultKind;
use databend_common_storage::FaultOp;
use databend_common_storage::FaultRule;
use databend_query::interpreters::InterpreterFactory;
use databend_query::physical_plans::IPhysicalPlan;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanMeta;
use databend_query::physical_plans::TableScan;
use databend_query::physical_plans::publish_planned_pruning_stats;
use databend_query::pipelines::PipelineBuilder;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelinePullingExecutor;
use databend_query::schedulers::build_local_pipeline;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextPartitionStats;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::sql::Planner;
use databend_query::storages::fuse::FuseTable;
use databend_query::stream::PullingExecutorStream;
use databend_query::stream::ReadDataBlockStream;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

use super::data_metrics::execute_like_prepared_query;
use super::data_metrics::init_lazy_delete_fragments;
use super::data_metrics::run_and_observe;
use super::data_metrics::run_and_observe_with;

/// Run `sql` on a fresh `QueryContext` created and executed *without* any
/// tracking payload carrying `io_stats`, the way `FlightSQL` and other
/// handlers that do not install one call the interpreter. The interpreter
/// itself must bind the query's counter so the IO is attributed.
/// Run a statement through the interpreter and return its first column as
/// text; used for `explain` output.
async fn run_for_text(
    fixture: &TestFixture,
    sql: &str,
    configure: impl FnOnce(&Arc<QueryContext>) -> Result<()>,
) -> Result<String> {
    let ctx = fixture.new_query_ctx().await?;
    configure(&ctx)?;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = interpreter.execute(ctx.clone()).await?;
    let blocks = stream.try_collect::<Vec<DataBlock>>().await?;
    let mut lines = Vec::new();
    for block in blocks {
        let col = block.get_by_offset(0).to_column();
        for i in 0..col.len() {
            lines.push(
                col.index(i)
                    .unwrap()
                    .to_string()
                    .trim_matches('\'')
                    .to_string(),
            );
        }
    }
    Ok(lines.join("\n"))
}

fn scan_counts(explain: &str) -> (Option<u64>, Option<u64>, bool) {
    let grab = |key: &str| {
        explain.lines().find_map(|l| {
            l.split_once(key)
                .and_then(|(_, v)| v.trim().parse::<u64>().ok())
        })
    };
    (
        grab("partitions total:"),
        grab("partitions scanned:"),
        explain.contains("pruning stats:"),
    )
}

/// `_segment_name` is applied before any segment is read. On an eager scan
/// that can reject every segment, so the planner has partition counters
/// but no pruning statistics; they must still reach the query.
#[tokio::test(flavor = "multi_thread")]
async fn eager_segment_name_filter_counts_partitions() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.segment_name_eager");

    fixture
        .execute_command(&format!(
            "create table {table} (a int) row_per_block=100 block_per_segment=10"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {table} select number from numbers(300)"
        ))
        .await?;

    let observed = run_and_observe(
        &fixture,
        &format!("select a from {table} where _segment_name = 'no-such-segment'"),
    )
    .await?;
    assert_eq!(observed.partitions_total, 3);
    assert_eq!(observed.partitions_scanned, 0);
    Ok(())
}

/// `ANALYZE TABLE` with window histograms builds one physical plan per
/// eligible column on the same context. Each `PhysicalPlanBuilder::build`
/// clears the per-plan statistics map; the query-level counters must still
/// cover every plan.
#[tokio::test(flavor = "multi_thread")]
async fn analyze_table_counts_every_histogram_scan() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.analyze_histograms");

    // Two histogram-eligible columns, one block.
    fixture
        .execute_command(&format!("create table {table} (a int, b int)"))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {table} select number, number * 2 from numbers(100)"
        ))
        .await?;

    let observed = run_and_observe_with(&fixture, &format!("analyze table {table}"), |ctx| {
        let settings = ctx.get_settings();
        settings.set_setting("enable_analyze_histogram".into(), "1".into())?;
        settings.set_setting("analyze_histogram_algorithm".into(), "window".into())
    })
    .await?;
    // One scan per histogram column (a, b) over the single block.
    assert_eq!(observed.partitions_total, 2);
    assert_eq!(observed.partitions_scanned, 2);
    Ok(())
}

/// Every pruning mode must publish its statistics under the scan node's
/// physical plan id, exactly once per value. The query here has an aggregate
/// above the scan so that the logical `scan_id` (0) and the physical plan id
/// differ; a publisher keyed on the wrong one shows up as a second entry in
/// the per-plan map, and a duplicate publisher as a sum larger than the entry.
#[tokio::test(flavor = "multi_thread")]
async fn every_pruning_mode_publishes_under_the_scan_plan_id() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    // (table, lazy?) — lazy needs segments > nodes (1 here).
    let row = format!("{db}.inv_row");
    let col = format!("{db}.inv_col");
    for (table, extra) in [(&row, ""), (&col, " segment_format='column_oriented'")] {
        fixture
            .execute_command(&format!(
                "create table {table} (a int) row_per_block=100 block_per_segment=10{extra}"
            ))
            .await?;
        for _ in 0..2 {
            fixture
                .execute_command(&format!(
                    "insert into {table} select number from numbers(300)"
                ))
                .await?;
        }
    }
    // Eager: a single segment.
    let eager = format!("{db}.inv_eager");
    fixture
        .execute_command(&format!("create table {eager} (a int) row_per_block=100"))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {eager} select number from numbers(300)"
        ))
        .await?;

    // A third segment on `row` with a disjoint range (3 x 3 blocks = 9):
    // segment-level pruning drops it whole, `_segment_name` skips it before
    // its metadata is read. Both still count towards the total, in blocks.
    fixture
        .execute_command(&format!(
            "insert into {row} select number + 1000 from numbers(300)"
        ))
        .await?;
    let first_segment = fixture
        .execute_query(&format!(
            "select _segment_name from {row} where a = 0 limit 1"
        ))
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?
        .remove(0)
        .get_by_offset(0)
        .index(0)
        .and_then(|s| s.as_string().map(|s| s.to_string()))
        .expect("segment name");

    let aggregate =
        |table: &str| format!("select a % 3, count() from {table} where a >= 200 group by a % 3");

    struct Case {
        name: &'static str,
        sql: String,
        prune_pipeline: bool,
        expect: (u64, u64),
    }
    let cases = [
        Case {
            name: "eager scan",
            sql: aggregate(&eager),
            prune_pipeline: true,
            expect: (3, 1),
        },
        Case {
            name: "lazy row-oriented, prune pipeline",
            sql: aggregate(&row),
            prune_pipeline: true,
            expect: (9, 5),
        },
        Case {
            name: "lazy row-oriented, one-shot",
            sql: aggregate(&row),
            prune_pipeline: false,
            expect: (9, 5),
        },
        Case {
            name: "lazy, whole segment pruned by range",
            sql: format!("select a % 3, count() from {row} where a < 100 group by a % 3"),
            prune_pipeline: true,
            expect: (9, 2),
        },
        Case {
            name: "lazy, _segment_name skips unread segments",
            sql: format!(
                "select a % 3, count() from {row} where _segment_name = '{first_segment}' group by a % 3"
            ),
            prune_pipeline: true,
            expect: (9, 3),
        },
        Case {
            name: "lazy column-oriented, prune pipeline",
            sql: aggregate(&col),
            prune_pipeline: true,
            expect: (6, 2),
        },
        // No "lazy column-oriented, one-shot" case: `prune_snapshot_blocks`
        // reads segments through `RowOrientedSegmentReader` and fails on a
        // column-oriented table with `unsupported encoding`, on `main` too.
        Case {
            name: "eager update",
            sql: format!("update {eager} set a = a where a >= 200"),
            prune_pipeline: true,
            expect: (3, 1),
        },
        Case {
            name: "eager delete",
            sql: format!("delete from {eager} where a >= 200"),
            prune_pipeline: true,
            expect: (3, 1),
        },
    ];

    for case in cases {
        let observed = run_and_observe_with(&fixture, &case.sql, |ctx| {
            ctx.get_settings().set_setting(
                "enable_prune_pipeline".into(),
                if case.prune_pipeline { "1" } else { "0" }.into(),
            )
        })
        .await
        .unwrap_or_else(|e| panic!("{}: {e}", case.name));
        assert_eq!(
            (observed.partitions_total, observed.partitions_scanned),
            case.expect,
            "{}: (total, scanned)",
            case.name
        );
        observed.assert_single_scan_published_once();
    }
    Ok(())
}

/// Plain `explain` of a lazy scan runs whatever prune pipeline
/// `build_prune_pipeline` returns and prints the result. The one-shot
/// mode (`enable_prune_pipeline = 0`) has to be such a pipeline too, or the
/// output shows the planner's total with `scanned: 0` and no pruning stats.
#[tokio::test(flavor = "multi_thread")]
async fn explain_runs_one_shot_pruning_for_lazy_scan() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.explain_sync");
    fixture
        .execute_command(&format!(
            "create table {table} (a int) row_per_block=100 block_per_segment=10"
        ))
        .await?;
    for _ in 0..2 {
        fixture
            .execute_command(&format!(
                "insert into {table} select number from numbers(300)"
            ))
            .await?;
    }

    let out = run_for_text(
        &fixture,
        &format!("explain select a % 3, count() from {table} where a >= 200 group by a % 3"),
        |ctx| {
            ctx.get_settings()
                .set_setting("enable_prune_pipeline".into(), "0".into())
        },
    )
    .await?;
    assert_eq!(scan_counts(&out), (Some(6), Some(2), true), "{out}");
    Ok(())
}

/// `EXPLAIN` drops the prune pipeline's receiver and only looks at whether
/// the pipeline completed. A pruning error in the one-shot mode must
/// therefore fail the processor, not just be sent down the channel, or
/// `explain` prints `scanned: 0` as if pruning had run.
#[tokio::test(flavor = "multi_thread")]
async fn explain_fails_when_one_shot_pruning_fails() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.explain_prune_fault");
    fixture
        .execute_command(&format!(
            "create table {table} (a int) row_per_block=100 block_per_segment=10"
        ))
        .await?;
    for _ in 0..2 {
        fixture
            .execute_command(&format!(
                "insert into {table} select number from numbers(300)"
            ))
            .await?;
    }
    let prefix = {
        let t = fixture
            .new_query_ctx()
            .await?
            .get_table(&fixture.default_catalog_name(), &db, "explain_prune_fault")
            .await?;
        FuseTable::try_from_table(t.as_ref())?
            .meta_location_generator()
            .prefix()
            .to_string()
    };

    // Every segment read fails. The planner does not read segments for a lazy
    // scan, so this only bites the lazy pruning.
    let fault = FaultInjection::install(FaultRule::new(
        FaultOp::Read,
        format!("{prefix}/_sg/"),
        FaultKind::Permanent,
    ));
    let result = run_for_text(
        &fixture,
        &format!("explain select a % 3, count() from {table} where a >= 200 group by a % 3"),
        |ctx| {
            ctx.get_settings()
                .set_setting("enable_prune_pipeline".into(), "0".into())
        },
    )
    .await;
    let hits = fault.hits();
    fault.remove();

    assert!(hits > 0, "the segment-read fault was never exercised");
    let err = result.expect_err("explain must fail when the one-shot pruning fails");
    assert!(
        err.to_string().contains("injected"),
        "unexpected error: {err}"
    );
    Ok(())
}

/// The prune cache (`enable_prune_cache`, on by default) is in-process and
/// keyed by segments + push-downs. Eager pruning stores the real
/// `partitions_total`; lazy pruning stores `0` because the planner
/// publishes the total. The two paths must not share entries: the same
/// query flips between them with `enable_distributed_pruning` or when the
/// node count crosses the segment count.
#[tokio::test(flavor = "multi_thread")]
async fn prune_cache_is_not_shared_between_eager_and_lazy_pruning() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table = format!("{db}.prune_cache_modes");
    fixture
        .execute_command(&format!(
            "create table {table} (a int) row_per_block=100 block_per_segment=10"
        ))
        .await?;
    for _ in 0..2 {
        fixture
            .execute_command(&format!(
                "insert into {table} select number from numbers(300)"
            ))
            .await?;
    }
    // deterministic filter so the result is cacheable; 2 segments x 3 blocks,
    // `a >= 200` keeps one block per segment
    let sql = format!("select count() from {table} where a >= 200");

    // On one node, 2 segments prune lazily iff `enable_distributed_pruning`.
    let with_distributed_pruning = |on: bool| {
        move |ctx: &Arc<QueryContext>| {
            ctx.get_settings().set_setting(
                "enable_distributed_pruning".into(),
                if on { "1" } else { "0" }.into(),
            )
        }
    };
    let eager = with_distributed_pruning(false);
    let lazy = with_distributed_pruning(true);

    // eager first (populates its entry), then lazy (must not hit it)
    let first = run_and_observe_with(&fixture, &sql, eager).await?;
    assert_eq!(
        (first.partitions_total, first.partitions_scanned),
        (6, 2),
        "eager"
    );
    let second = run_and_observe_with(&fixture, &sql, lazy).await?;
    assert_eq!(
        (second.partitions_total, second.partitions_scanned),
        (6, 2),
        "lazy after eager: an eager cache hit would add its total again (12)"
    );

    // lazy again (now its own entry exists), then eager (must not hit it)
    let third = run_and_observe_with(&fixture, &sql, lazy).await?;
    assert_eq!(
        (third.partitions_total, third.partitions_scanned),
        (6, 2),
        "lazy, cached"
    );
    let fourth = run_and_observe_with(&fixture, &sql, eager).await?;
    assert_eq!(
        (fourth.partitions_total, fourth.partitions_scanned),
        (6, 2),
        "eager after lazy: a lazy cache hit would report total 0"
    );

    // The one-shot lazy mode (`enable_prune_pipeline = 0`) goes through
    // `prune_snapshot_blocks` like the eager planner does, so it must opt
    // into the lazy cache scope explicitly. Use a fresh filter so no entry
    // from above is hit.
    let sql = format!("select count() from {table} where a >= 100");
    let lazy_one_shot = |ctx: &Arc<QueryContext>| {
        with_distributed_pruning(true)(ctx)?;
        ctx.get_settings()
            .set_setting("enable_prune_pipeline".into(), "0".into())
    };
    let fifth = run_and_observe_with(&fixture, &sql, lazy_one_shot).await?;
    assert_eq!(
        (fifth.partitions_total, fifth.partitions_scanned),
        (6, 4),
        "lazy one-shot"
    );
    let sixth = run_and_observe_with(&fixture, &sql, eager).await?;
    assert_eq!(
        (sixth.partitions_total, sixth.partitions_scanned),
        (6, 4),
        "eager after lazy one-shot: a shared entry would report total 0"
    );
    Ok(())
}

/// `Table::read_data_block_stream` reads a `DataSourcePlan` without a
/// physical plan. It has to run the prune pipeline for lazy parts the way
/// `TableScan` does, in both lazy modes; otherwise `read_data` has nothing to
/// dispatch and the stream is silently empty.
#[tokio::test(flavor = "multi_thread")]
async fn read_data_block_stream_prunes_lazy_parts() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let table_name = "stream_lazy";
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (a int) row_per_block=100 block_per_segment=10"
        ))
        .await?;
    for _ in 0..2 {
        fixture
            .execute_command(&format!(
                "insert into {db}.{table_name} select number from numbers(300)"
            ))
            .await?;
    }

    for enable_prune_pipeline in ["1", "0"] {
        let ctx = fixture.new_query_ctx().await?;
        ctx.get_settings()
            .set_setting("enable_distributed_pruning".into(), "1".into())?;
        ctx.get_settings()
            .set_setting("enable_prune_pipeline".into(), enable_prune_pipeline.into())?;
        let table = ctx
            .get_table(&fixture.default_catalog_name(), &db, table_name)
            .await?;
        let plan = table
            .read_plan(ctx.clone(), None, None, false, true)
            .await?;
        assert!(
            plan.parts.partitions_type() == PartInfoType::LazyLevel,
            "2 segments with distributed pruning must plan lazily"
        );

        let rows: usize = table
            .read_data_block_stream(ctx.clone(), &plan)
            .await?
            .try_collect::<Vec<DataBlock>>()
            .await?
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(
            rows, 600,
            "enable_prune_pipeline = {enable_prune_pipeline}: lazy parts were not pruned and read"
        );
    }
    Ok(())
}

/// Some code assembles a `TableScan` by hand and runs it through
/// `build_local_pipeline` without `PhysicalPlanBuilder` (`GET_LINEAGE`'s
/// `edge_reader.rs` does this). Planned statistics are published by the
/// pipeline entry points, not the planner, so such scans count too. Covers
/// an eager and a lazy table in both lazy modes.
#[tokio::test(flavor = "multi_thread")]
async fn hand_built_table_scan_publishes_partition_stats() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    // 1 segment -> eager; 2 segments -> lazy once distributed pruning is on
    for (table_name, segments, scanned_expected) in [("hand_eager", 1, 3), ("hand_lazy", 2, 6)] {
        fixture
            .execute_command(&format!(
                "create table {db}.{table_name} (a int) row_per_block=100 block_per_segment=10"
            ))
            .await?;
        for _ in 0..segments {
            fixture
                .execute_command(&format!(
                    "insert into {db}.{table_name} select number from numbers(300)"
                ))
                .await?;
        }
        for enable_prune_pipeline in ["1", "0"] {
            let ctx = fixture.new_query_ctx().await?;
            ctx.get_settings()
                .set_setting("enable_distributed_pruning".into(), "1".into())?;
            ctx.get_settings()
                .set_setting("enable_prune_pipeline".into(), enable_prune_pipeline.into())?;
            let table = ctx
                .get_table(&fixture.default_catalog_name(), &db, table_name)
                .await?;
            let source = table
                .read_plan(ctx.clone(), None, None, false, true)
                .await?;
            let is_lazy = source.parts.partitions_type() == PartInfoType::LazyLevel;
            assert_eq!(
                is_lazy,
                segments > 1,
                "{table_name}: unexpected pruning path"
            );
            let name_mapping = source
                .output_schema
                .fields()
                .iter()
                .map(|f| (f.name().to_string(), f.name().to_string()))
                .collect();
            let plan = PhysicalPlan::new(TableScan {
                meta: PhysicalPlanMeta::new("HandBuiltScan"),
                scan_id: 0,
                name_mapping,
                source: Box::new(source),
                internal_column: None,
                table_index: None,
                stat_info: None,
            });

            let build_res = build_local_pipeline(&ctx, &plan).await?;
            let settings = ExecutorSettings::try_create(ctx.clone())?;
            let executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
            let rows: usize = PullingExecutorStream::create(executor)?
                .try_collect::<Vec<DataBlock>>()
                .await?
                .iter()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(rows, 300 * segments, "{table_name}: all rows read");

            let stats = ctx.get_partition_stats();
            let per_plan = ctx.get_pruned_partitions_stats();
            assert_eq!(
                (stats.total, stats.scanned),
                (scanned_expected, scanned_expected),
                "{table_name}, enable_prune_pipeline = {enable_prune_pipeline}"
            );
            assert_eq!(
                per_plan.keys().copied().collect::<Vec<_>>(),
                vec![plan.get_id()],
                "{table_name}: exactly one per-plan entry, under the scan's id"
            );
        }
    }
    Ok(())
}

/// A lazy `DELETE` prunes on the node that runs the `MutationSource`, either
/// while the fragment is prepared (`enable_prune_pipeline = 0`) or in a prune
/// pipeline at runtime. Either way that node publishes only what it scanned;
/// the whole-table `partitions_total` is published once, by the coordinator.
/// The two segments are split across the two simulated nodes, so this node
/// scans one block; the other node's block would arrive through the
/// statistics exchange. A per-node total would show 4 here, not 2.
#[tokio::test(flavor = "multi_thread")]
async fn lazy_delete_publishes_scanned_per_node_and_total_once() -> Result<()> {
    for enable_prune_pipeline in [false, true] {
        let fixture = TestFixture::setup().await?;
        let (ctx, fragments) = init_lazy_delete_fragments(&fixture, enable_prune_pipeline).await?;

        // The coordinator half: `build_physical_plan` ran on `ctx`, so publish
        // its planned statistics the way `build_distributed_pipeline` does.
        let fragment = fragments
            .fragments
            .iter()
            .find(|fragment| format!("{:?}", fragment.physical_plan).contains("MutationSource"))
            .expect("the lazy mutation source must be prepared on this node");
        publish_planned_pruning_stats(ctx.as_ref(), &fragment.physical_plan);

        if enable_prune_pipeline {
            let mut build_res = PipelineBuilder::create(
                ctx.get_function_context()?,
                ctx.get_settings(),
                ctx.clone(),
            )
            .finalize(&fragment.physical_plan)?;
            build_res.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
            let prune_pipelines = std::mem::take(&mut build_res.sources_pipelines);
            assert!(
                !prune_pipelines.is_empty(),
                "lazy DELETE must come with a prune pipeline"
            );
            execute_like_prepared_query(&ctx, prune_pipelines).await?;
        }

        let stats = ctx.get_partition_stats();
        assert_eq!(
            (stats.total, stats.scanned),
            (2, 1),
            "enable_prune_pipeline = {enable_prune_pipeline}: whole-table total of 2 blocks, \
             this node's share of 1 scanned; a per-node total would show 4"
        );
        let mutation_source_id = fragment
            .physical_plan
            .try_find_mutation_source()
            .expect("fragment holds the mutation source")
            .get_meta()
            .plan_id;
        let per_plan = ctx.get_pruned_partitions_stats();
        assert_eq!(
            per_plan.keys().copied().collect::<Vec<_>>(),
            vec![mutation_source_id],
            "one entry, under the MutationSource's physical id (not the fragment root's)"
        );
    }
    Ok(())
}
