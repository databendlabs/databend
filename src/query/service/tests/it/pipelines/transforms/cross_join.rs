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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use databend_common_exception::Result;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::filters::TransformLimit;
use databend_common_sql::plans::JoinType;
use databend_query::physical_plans::ConstantTableScan;
use databend_query::physical_plans::HashJoin;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanMeta;
use databend_query::pipelines::PipelineBuilder;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelinePullingExecutor;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

fn scan(block: DataBlock, prefix: &str) -> PhysicalPlan {
    let fields = block
        .columns()
        .iter()
        .enumerate()
        .map(|(i, entry)| DataField::new(&format!("{prefix}{i}"), entry.data_type()))
        .collect();
    PhysicalPlan::new(ConstantTableScan {
        meta: PhysicalPlanMeta::new("ConstantTableScan"),
        values: block
            .columns()
            .iter()
            .map(|entry| entry.to_column())
            .collect(),
        num_rows: block.num_rows(),
        output_schema: Arc::new(DataSchema::new(fields)),
    })
}

/// Exercise the executor directly: the planner keeps predicates without equi
/// keys on Inner joins, which pipeline building runs as a filtered Cross join.
fn cross_join(probe: DataBlock, build: DataBlock, predicate: Option<Expr>) -> HashJoin {
    let probe_projections = (0..probe.num_columns()).collect();
    let build_projections = (0..build.num_columns()).collect();
    let probe = scan(probe, "p");
    let build = scan(build, "b");
    let mut fields = probe.output_schema().unwrap().fields().clone();
    fields.extend(build.output_schema().unwrap().fields().iter().cloned());
    HashJoin {
        meta: PhysicalPlanMeta::new("HashJoin"),
        projections: (0..fields.len()).collect(),
        probe_projections,
        build_projections,
        probe,
        build,
        build_keys: vec![],
        probe_keys: vec![],
        is_null_equal: vec![],
        non_equi_conditions: predicate.into_iter().map(|p| p.as_remote_expr()).collect(),
        join_type: JoinType::Cross,
        marker_index: None,
        from_correlated_subquery: false,
        probe_to_build: vec![],
        output_schema: Arc::new(DataSchema::new(fields)),
        stat_info: None,
        single_to_inner: None,
        build_side_cache_info: None,
        runtime_filter: Default::default(),
        broadcast_id: None,
    }
}

async fn run_cross_join(
    fixture: &TestFixture,
    join: HashJoin,
    block_size: usize,
    spill: bool,
    limit: Option<usize>,
) -> anyhow::Result<Vec<DataBlock>> {
    // QueryContext reapplies session changes on get_settings(), so override
    // the fixture's session default rather than a query-local max_threads.
    fixture
        .default_session()
        .get_settings()
        .set_max_threads(2)?;
    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_setting("max_block_size".into(), block_size.to_string())?;
    assert_eq!(ctx.get_settings().get_max_threads()?, 2);
    settings.set_setting("force_join_data_spill".into(), u8::from(spill).to_string())?;
    // Inner joins without equi keys must leave the new join executor too.
    settings.set_setting("enable_experimental_new_join".into(), "1".into())?;
    if spill {
        settings.set_setting(
            "join_spilling_buffer_threshold_per_proc_mb".into(),
            "0".into(),
        )?;
    }
    // A zero-column build is only a row count and never spills, so the
    // probe side does not spill either. The planner, not the pipeline, makes
    // a zero-column side the build, so a zero-column probe still spills here.
    let expect_spill_files = !join.build_projections.is_empty();
    let mut plan = PhysicalPlan::new(join);
    plan.adjust_plan_id(&mut 0);
    let mut pipelines = PipelineBuilder::create(ctx.get_function_context()?, settings, ctx.clone())
        .finalize(&plan)?;
    let pipeline = pipelines.main_pipeline.display_indent().to_string();
    assert!(pipeline.contains("HashJoinProbe"), "{pipeline}");
    assert!(!pipeline.contains("TransformHashJoin"), "{pipeline}");
    if let Some(limit) = limit {
        // Attach LIMIT after physical planning so it cannot be pushed into
        // either input: downstream must close an actually active Cross cursor.
        pipelines.main_pipeline.resize(1, false)?;
        pipelines.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformLimit::try_create(
                Some(limit),
                0,
                input,
                output,
                None,
            )?))
        })?;
    }
    pipelines.set_max_threads(2);
    let mut executor = PipelinePullingExecutor::from_pipelines(
        pipelines,
        ExecutorSettings::try_create(ctx.clone())?,
    )?;
    ctx.set_executor(executor.get_inner())?;
    executor.start();
    let blocks = tokio::time::timeout(Duration::from_secs(60), async {
        let mut blocks = Vec::new();
        while let Some(block) = executor.pull_data().await? {
            assert!(block.num_rows() <= block_size, "oversized Cross output");
            block.check_valid()?;
            blocks.push(block);
        }
        Ok::<_, databend_common_exception::ErrorCode>(blocks)
    })
    .await??;
    if spill {
        let progress = ctx.get_total_spill_progress();
        if expect_spill_files {
            assert!(progress.file_nums > 0, "expected actual Cross spill");
            assert!(progress.bytes > 0, "expected Cross spill bytes");
        } else {
            assert_eq!(progress.file_nums, 0);
            assert_eq!(progress.bytes, 0);
        }
    }
    Ok(blocks)
}

fn int_block(values: Vec<i64>) -> DataBlock {
    DataBlock::new_from_columns(vec![Int64Type::from_data(values)])
}

fn rows(blocks: &[DataBlock]) -> Vec<Vec<Scalar>> {
    let mut result = Vec::new();
    for block in blocks {
        for row in 0..block.num_rows() {
            result.push(
                block
                    .columns()
                    .iter()
                    .map(|col| col.index(row).unwrap().to_owned())
                    .collect(),
            );
        }
    }
    result.sort_by_key(|row| format!("{row:?}"));
    result
}

fn less_than(left: usize, right: usize, data_type: DataType) -> Result<Expr> {
    let column = |id| {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id,
            data_type: data_type.clone(),
            display_name: format!("c{id}"),
        })
    };
    check_function(
        None,
        "lt",
        &[],
        &[column(left), column(right)],
        &BUILTIN_FUNCTIONS,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cross_join_bounded_batches_and_single_row_sides() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    for block_size in [1, 3, 8] {
        for (probe_rows, build_rows) in [(0, 5), (5, 0), (1, 17), (17, 1), (7, 11)] {
            let join = cross_join(
                int_block((0..probe_rows).collect()),
                int_block((0..build_rows).collect()),
                None,
            );
            let actual = run_cross_join(&fixture, join, block_size, false, None).await?;
            let mut expected = (0..probe_rows)
                .flat_map(|p| {
                    (0..build_rows).map(move |b| {
                        vec![
                            Scalar::Number(NumberScalar::Int64(p)),
                            Scalar::Number(NumberScalar::Int64(b)),
                        ]
                    })
                })
                .collect::<Vec<_>>();
            expected.sort_by_key(|row| format!("{row:?}"));
            assert_eq!(rows(&actual), expected);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cross_join_predicate_selection_and_spill_replay() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let data_type = DataType::Number(NumberDataType::Int64);
    for (spill, join_type) in [false, true]
        .into_iter()
        .flat_map(|spill| [(spill, JoinType::Cross), (spill, JoinType::Inner)])
    {
        let mut join = cross_join(
            int_block(vec![0, 1, 1, 2, 100]),
            int_block((0..33).collect()),
            Some(less_than(0, 1, data_type.clone())?),
        );
        join.join_type = join_type;
        let blocks = run_cross_join(&fixture, join, 3, spill, None).await?;
        let mut expected = vec![];
        for p in [0, 1, 1, 2, 100] {
            for b in 0..33 {
                if p < b {
                    expected.push(vec![
                        Scalar::Number(NumberScalar::Int64(p)),
                        Scalar::Number(NumberScalar::Int64(b)),
                    ]);
                }
            }
        }
        expected.sort_by_key(|row| format!("{row:?}"));
        assert_eq!(rows(&blocks), expected, "{join_type}, spill={spill}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cross_join_nullable_predicate_and_projection() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let probe = DataBlock::new_from_columns(vec![
        Int64Type::from_opt_data(vec![None, Some(1), Some(5)]),
        Int64Type::from_data(vec![10, 11, 15]),
    ]);
    let build = DataBlock::new_from_columns(vec![
        Int64Type::from_data(vec![22, 24, 29]),
        Int64Type::from_opt_data(vec![Some(2), Some(4), None]),
    ]);
    // Predicate offsets refer to the projected [probe key, probe payload,
    // build key], not to the original build schema. Only the payload is output.
    let data_type = probe.get_by_offset(0).data_type();
    let mut join = cross_join(probe, build, Some(less_than(0, 2, data_type)?));
    join.build_projections = BTreeSet::from([1]);
    join.projections = BTreeSet::from([1]);
    join.output_schema = Arc::new(DataSchema::new(vec![DataField::new(
        "p1",
        DataType::Number(NumberDataType::Int64),
    )]));
    let blocks = run_cross_join(&fixture, join, 2, false, None).await?;
    assert_eq!(rows(&blocks), vec![
        vec![Scalar::Number(
            NumberScalar::Int64(11)
        )];
        2
    ]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cross_join_active_cursor_early_limit() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    for spill in [false, true] {
        for (probe_rows, block_size) in [(7, 3), (2, 16)] {
            // Exercise both a sliced probe and a partially consumed shared
            // build window when downstream closes the active cursor.
            let join = cross_join(
                int_block((0..probe_rows).collect()),
                int_block((0..257).collect()),
                None,
            );
            let blocks = run_cross_join(&fixture, join, block_size, spill, Some(1)).await?;
            assert_eq!(blocks.iter().map(DataBlock::num_rows).sum::<usize>(), 1);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cross_join_unfiltered_nullable_payload_and_projection() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    for spill in [false, true] {
        let probe = int_block(vec![1, 1]);
        let build = DataBlock::new_from_columns(vec![
            Int64Type::from_data((0..19).collect()),
            Int64Type::from_opt_data(
                (0..19)
                    .map(|v| (v % 2 == 0).then_some(v))
                    .collect::<Vec<_>>(),
            ),
        ]);
        let mut join = cross_join(probe, build, None);
        join.build_projections = BTreeSet::from([1]);
        join.projections = BTreeSet::from([1]);
        join.output_schema = Arc::new(DataSchema::new(vec![DataField::new(
            "b1",
            DataType::Number(NumberDataType::Int64).wrap_nullable(),
        )]));
        let blocks = run_cross_join(&fixture, join, 8, spill, None).await?;
        let expected = DataBlock::new_from_columns(vec![Int64Type::from_opt_data(
            (0..2)
                .flat_map(|_| (0..19).map(|v| (v % 2 == 0).then_some(v)))
                .collect::<Vec<_>>(),
        )]);
        assert_eq!(rows(&blocks), rows(&[expected]));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cross_join_zero_column_sides() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    for spill in [false, true] {
        for zero_is_probe in [false, true] {
            for pruned_input in [false, true] {
                for filtered in [false, true] {
                    let count = if pruned_input {
                        // Unused source columns need not be retained either.
                        int_block(vec![99; 5])
                    } else {
                        DataBlock::new(vec![], 5)
                    };
                    let payload = DataBlock::new_from_columns(vec![
                        Int64Type::from_opt_data(vec![Some(0), Some(1), Some(4), Some(4)]),
                        Int64Type::from_opt_data(vec![Some(10), None, Some(3), Some(20)]),
                    ]);
                    let predicate = if filtered {
                        Some(less_than(
                            0,
                            1,
                            DataType::Number(NumberDataType::Int64).wrap_nullable(),
                        )?)
                    } else {
                        None
                    };
                    let mut join = if zero_is_probe {
                        cross_join(count, payload, predicate)
                    } else {
                        cross_join(payload, count, predicate)
                    };
                    if zero_is_probe {
                        join.probe_projections.clear();
                    } else {
                        join.build_projections.clear();
                    }
                    // The planner keeps predicates without equi keys on Inner.
                    join.join_type = if filtered {
                        JoinType::Inner
                    } else {
                        JoinType::Cross
                    };
                    join.projections = BTreeSet::from([1]);
                    join.output_schema = Arc::new(DataSchema::new(vec![DataField::new(
                        "payload",
                        DataType::Number(NumberDataType::Int64).wrap_nullable(),
                    )]));
                    let actual = run_cross_join(&fixture, join, 3, spill, None).await?;
                    let expected_values = if filtered {
                        vec![Some(10), Some(20)]
                    } else {
                        vec![Some(10), None, Some(3), Some(20)]
                    };
                    let expected = DataBlock::new_from_columns(vec![Int64Type::from_opt_data(
                        (0..5)
                            .flat_map(|_| expected_values.iter().copied())
                            .collect::<Vec<_>>(),
                    )]);
                    assert_eq!(rows(&actual), rows(&[expected]));
                }
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cross_join_zero_column_large_counts_and_early_limit() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    for spill in [false, true] {
        let join = cross_join(
            DataBlock::new(vec![], 100_000),
            DataBlock::new(vec![], 100_000),
            None,
        );
        let blocks = run_cross_join(&fixture, join, 3, spill, Some(1)).await?;
        assert_eq!(blocks.iter().map(DataBlock::num_rows).sum::<usize>(), 1);
        assert!(blocks.iter().all(|block| block.num_columns() == 0));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cross_join_sql_replay_and_early_limit() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    for (threads, buffer_threshold) in [(1, 0), (2, 0), (1, 1)] {
        // Keep this setting on the session: otherwise the fixture's default
        // of eight threads overwrites it and can hide per-worker replay bugs.
        fixture
            .default_session()
            .get_settings()
            .set_max_threads(threads)?;
        for filtered in [false, true] {
            for limit in [false, true] {
                let ctx = fixture.new_query_ctx().await?;
                let settings = ctx.get_settings();
                settings.set_setting("max_block_size".into(), "3".into())?;
                settings.set_setting("disable_join_reorder".into(), "1".into())?;
                settings.set_setting("enable_experimental_new_join".into(), "1".into())?;
                settings.set_setting("force_join_data_spill".into(), "1".into())?;
                settings.set_setting(
                    "join_spilling_buffer_threshold_per_proc_mb".into(),
                    buffer_threshold.to_string(),
                )?;
                // Unlike a ConstantTableScan, these sources emit many input
                // blocks per worker. A zero threshold writes each to its own
                // file; a nonzero threshold exercises buffer-only replay.
                let join = if filtered {
                    "JOIN numbers(37) b ON a.number <> b.number"
                } else {
                    "CROSS JOIN numbers(37) b"
                };
                let limit_clause = if limit { " LIMIT 31" } else { "" };
                let query =
                    format!("SELECT a.number, b.number FROM numbers(31) a {join}{limit_clause}");
                assert_eq!(ctx.get_settings().get_max_threads()?, threads);
                let blocks = tokio::time::timeout(Duration::from_secs(60), async {
                    TestFixture::execute_query_with_ctx(ctx.clone(), &query)
                        .await?
                        .try_collect::<Vec<DataBlock>>()
                        .await
                })
                .await??;
                if limit {
                    assert_eq!(blocks.iter().map(DataBlock::num_rows).sum::<usize>(), 31);
                } else {
                    let pairs = (0..31u64)
                        .flat_map(|p| (0..37u64).map(move |b| (p, b)))
                        .filter(|(p, b)| !filtered || p != b)
                        .collect::<Vec<_>>();
                    let expected = DataBlock::new_from_columns(vec![
                        databend_common_expression::types::UInt64Type::from_data(
                            pairs.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
                        ),
                        databend_common_expression::types::UInt64Type::from_data(
                            pairs.iter().map(|(_, b)| *b).collect::<Vec<_>>(),
                        ),
                    ]);
                    assert_eq!(rows(&blocks), rows(&[expected]));
                }
                assert_eq!(ctx.get_settings().get_max_threads()?, threads);
                let progress = ctx.get_total_spill_progress();
                if buffer_threshold == 0 {
                    assert!(progress.file_nums > 4, "expected many replay files");
                    assert!(progress.bytes > 0);
                } else {
                    assert_eq!(progress.file_nums, 0, "expected buffer-only replay");
                    assert_eq!(progress.bytes, 0);
                }
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cross_join_rejected_batches_and_zero_column_inputs() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    // Reject multiple full candidate batches before reaching a matching pair.
    let data_type = DataType::Number(NumberDataType::Int64);
    let join = cross_join(
        int_block(vec![50, 50, 50]),
        int_block(vec![0, 1, 2, 100]),
        Some(less_than(0, 1, data_type)?),
    );
    let blocks = run_cross_join(&fixture, join, 2, false, None).await?;
    assert_eq!(blocks.iter().map(DataBlock::num_rows).sum::<usize>(), 3);
    for block in &blocks {
        for row in 0..block.num_rows() {
            assert_eq!(
                block.get_by_offset(1).index(row),
                Some(ScalarRef::Number(NumberScalar::Int64(100)))
            );
        }
    }
    for spill in [false, true] {
        for (probe_rows, build_rows) in [(1, 7), (7, 1), (5, 11)] {
            let join = cross_join(
                DataBlock::new(vec![], probe_rows),
                DataBlock::new(vec![], build_rows),
                None,
            );
            let blocks = run_cross_join(&fixture, join, 3, spill, None).await?;
            assert!(blocks.iter().all(|block| block.num_columns() == 0));
            assert_eq!(
                blocks.iter().map(DataBlock::num_rows).sum::<usize>(),
                probe_rows * build_rows
            );
        }
    }
    Ok(())
}
