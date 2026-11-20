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
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::Int64Type;
use databend_common_sql::plans::JoinType;
use databend_query::physical_plans::ConstantTableScan;
use databend_query::physical_plans::HashJoin;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanMeta;
use databend_query::physical_plans::PhysicalRuntimeFilter;
use databend_query::physical_plans::PhysicalRuntimeFilters;
use databend_query::pipelines::PipelineBuilder;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelinePullingExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextRuntimeFilter;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;

/// The scan the runtime filter targets. No such scan exists in the plan: the test reads the
/// filter the join publishes for it from the query context.
const PROBE_SCAN_ID: usize = 4242;

fn int64() -> DataType {
    DataType::Number(NumberDataType::Int64)
}

fn scan(values: Vec<i64>, name: &str) -> PhysicalPlan {
    let num_rows = values.len();
    PhysicalPlan::new(ConstantTableScan {
        meta: PhysicalPlanMeta::new("ConstantTableScan"),
        values: vec![Int64Type::from_data(values)],
        num_rows,
        output_schema: Arc::new(DataSchema::new(vec![DataField::new(name, int64())])),
    })
}

fn key<Index: ColumnIndex>(id: Index, name: &str) -> Expr<Index> {
    Expr::ColumnRef(ColumnRef {
        span: None,
        id,
        data_type: int64(),
        display_name: name.to_string(),
    })
}

/// `probe JOIN build ON probe.p0 = build.b0`, with one runtime filter on the key when
/// `with_runtime_filter` is set.
fn equi_join(
    join_type: JoinType,
    probe: Vec<i64>,
    build: Vec<i64>,
    with_runtime_filter: bool,
) -> HashJoin {
    let probe = scan(probe, "p0");
    let build = scan(build, "b0");
    let mut fields = probe.output_schema().unwrap().fields().clone();
    if matches!(join_type, JoinType::Inner) {
        fields.extend(build.output_schema().unwrap().fields().iter().cloned());
    }
    let filters = match with_runtime_filter {
        false => vec![],
        true => vec![PhysicalRuntimeFilter {
            id: 0,
            build_key: key(0, "b0").as_remote_expr(),
            probe_targets: vec![(key("p0".to_string(), "p0").as_remote_expr(), PROBE_SCAN_ID)],
            build_table_rows: None,
            // Small enough build against this probe table keeps the bloom filter selective.
            probe_table_rows: Some(1_000_000),
            enable_bloom_runtime_filter: true,
            enable_inlist_runtime_filter: true,
            enable_min_max_runtime_filter: true,
        }],
    };
    HashJoin {
        meta: PhysicalPlanMeta::new("HashJoin"),
        projections: (0..fields.len()).collect(),
        probe_projections: [0].into(),
        build_projections: match join_type {
            JoinType::Inner => [0].into(),
            _ => Default::default(),
        },
        probe,
        build,
        build_keys: vec![key(0, "b0").as_remote_expr()],
        probe_keys: vec![key(0, "p0").as_remote_expr()],
        is_null_equal: vec![false],
        non_equi_conditions: vec![],
        join_type,
        marker_index: None,
        from_correlated_subquery: false,
        probe_to_build: vec![],
        output_schema: Arc::new(DataSchema::new(fields)),
        stat_info: None,
        single_to_inner: None,
        build_side_cache_info: None,
        runtime_filter: PhysicalRuntimeFilters { filters },
        broadcast_id: None,
    }
}

async fn run_join(
    fixture: &TestFixture,
    join: HashJoin,
    spill: bool,
) -> anyhow::Result<(Arc<QueryContext>, Vec<i64>)> {
    // QueryContext reapplies session changes on get_settings(), so override
    // the fixture's session default rather than a query-local max_threads.
    fixture
        .default_session()
        .get_settings()
        .set_max_threads(4)?;
    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    settings.set_setting("max_block_size".into(), "16".into())?;
    settings.set_setting("enable_experimental_new_join".into(), "1".into())?;
    settings.set_setting("force_join_data_spill".into(), u8::from(spill).to_string())?;
    if spill {
        settings.set_setting(
            "join_spilling_buffer_threshold_per_proc_mb".into(),
            "0".into(),
        )?;
    }

    let mut plan = PhysicalPlan::new(join);
    plan.adjust_plan_id(&mut 0);
    let mut pipelines = PipelineBuilder::create(ctx.get_function_context()?, settings, ctx.clone())
        .finalize(&plan)?;
    let pipeline = pipelines.main_pipeline.display_indent().to_string();
    assert!(pipeline.contains("TransformHashJoin"), "{pipeline}");
    pipelines.set_max_threads(4);

    let mut executor = PipelinePullingExecutor::from_pipelines(
        pipelines,
        ExecutorSettings::try_create(ctx.clone())?,
    )?;
    ctx.set_executor(executor.get_inner())?;
    executor.start();
    let values = tokio::time::timeout(Duration::from_secs(60), async {
        let mut values = Vec::new();
        while let Some(block) = executor.pull_data().await? {
            block.check_valid()?;
            values.extend(first_column(&block));
        }
        Ok::<_, databend_common_exception::ErrorCode>(values)
    })
    .await??;
    Ok((ctx, values))
}

fn first_column(block: &DataBlock) -> Vec<i64> {
    let column = block.get_by_offset(0).to_column();
    let column = column.as_number().unwrap().as_int64().unwrap();
    column.iter().copied().collect()
}

fn sorted(mut values: Vec<i64>) -> Vec<i64> {
    values.sort();
    values
}

/// A build side that spills must still publish the runtime filter it built from every build
/// row, from all processors of the join. The filter does not depend on whether a processor
/// is in memory or grace mode when it is merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_spilled_build_keeps_runtime_filter() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let build = (100..200).collect::<Vec<_>>();
    let probe = (0..1000).collect::<Vec<_>>();

    for spill in [false, true] {
        let join = equi_join(JoinType::Inner, probe.clone(), build.clone(), true);
        let (ctx, values) = run_join(&fixture, join, spill).await?;
        assert_eq!(sorted(values), build, "spill={spill}");

        if spill {
            let progress = ctx.get_total_spill_progress();
            assert!(progress.file_nums > 0, "expected the build side to spill");
        }

        let filters = ctx.get_runtime_filters(PROBE_SCAN_ID);
        assert_eq!(filters.len(), 1, "spill={spill}");
        let filter = &filters[0];
        assert!(filter.enabled, "spill={spill}");
        assert_eq!(filter.build_rows, build.len(), "spill={spill}");
        assert!(filter.min_max.is_some(), "spill={spill}");
        assert!(filter.inlist.is_some(), "spill={spill}");
        assert_eq!(filter.inlist_value_count, build.len(), "spill={spill}");
        assert!(filter.bloom.is_some(), "spill={spill}");
    }
    Ok(())
}

/// Probe is skipped only when the whole build side is empty. That decision must count every
/// build row, also when no runtime filter is planned and when the build side spills.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_build_side_empty_counts_spilled_rows() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let probe = (0..300).collect::<Vec<_>>();

    for spill in [false, true] {
        for with_runtime_filter in [false, true] {
            for join_type in [JoinType::Inner, JoinType::LeftSemi, JoinType::LeftAnti] {
                // The planner never plans runtime filters for anti joins on the probe side.
                if with_runtime_filter && join_type == JoinType::LeftAnti {
                    continue;
                }
                let case = format!("{join_type}, spill={spill}, rf={with_runtime_filter}");

                let build = (100..150).collect::<Vec<_>>();
                let join = equi_join(join_type, probe.clone(), build, with_runtime_filter);
                let (_, values) = run_join(&fixture, join, spill).await?;
                let expected = match join_type {
                    JoinType::Inner | JoinType::LeftSemi => (100..150).collect::<Vec<_>>(),
                    _ => (0..100).chain(150..300).collect(),
                };
                assert_eq!(sorted(values), expected, "{case}");

                let join = equi_join(join_type, probe.clone(), vec![], with_runtime_filter);
                let (_, values) = run_join(&fixture, join, spill).await?;
                let expected = match join_type {
                    JoinType::Inner | JoinType::LeftSemi => vec![],
                    _ => probe.clone(),
                };
                assert_eq!(sorted(values), expected, "empty build, {case}");
            }
        }
    }
    Ok(())
}
