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

use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::plans::JoinType;
use databend_query::interpreters::InterpreterFactory;
use databend_query::physical_plans::ConstantTableScan;
use databend_query::physical_plans::HashJoin;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanMeta;
use databend_query::physical_plans::PhysicalRuntimeFilters;
use databend_query::pipelines::processors::HashJoinDesc;
use databend_query::pipelines::processors::transforms::HashJoinFactory;
use databend_query::sessions::TableContextSettings;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

fn constant_plan() -> PhysicalPlan {
    PhysicalPlan::new(ConstantTableScan {
        meta: PhysicalPlanMeta::new("ConstantTableScan"),
        values: vec![],
        num_rows: 0,
        output_schema: Arc::new(DataSchema::empty()),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn right_outer_join_skips_probe_keys_when_build_is_empty() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let function_ctx = FunctionContext::default();

    let invalid_probe_key = check_function(
        None,
        "to_int64",
        &[],
        &[Expr::constant(Scalar::String("invalid".to_string()), None)],
        &BUILTIN_FUNCTIONS,
    )?;
    let build_key = Expr::constant(
        Scalar::Number(NumberScalar::Int64(0)),
        Some(DataType::Number(NumberDataType::Int64)),
    );

    let physical_join = HashJoin {
        meta: PhysicalPlanMeta::new("HashJoin"),
        projections: BTreeSet::new(),
        probe_projections: BTreeSet::new(),
        build_projections: BTreeSet::new(),
        build: constant_plan(),
        probe: constant_plan(),
        build_keys: vec![build_key.as_remote_expr()],
        probe_keys: vec![invalid_probe_key.as_remote_expr()],
        is_null_equal: vec![false],
        non_equi_conditions: vec![],
        join_type: JoinType::Right,
        marker_index: None,
        from_correlated_subquery: false,
        probe_to_build: vec![],
        output_schema: Arc::new(DataSchema::empty()),
        need_hold_hash_table: false,
        stat_info: None,
        single_to_inner: None,
        build_side_cache_info: None,
        runtime_filter: PhysicalRuntimeFilters::default(),
        broadcast_id: None,
        nested_loop_filter: None,
    };
    let desc = Arc::new(HashJoinDesc::create(&physical_join)?);
    let method =
        DataBlock::choose_hash_method_with_types(&[DataType::Number(NumberDataType::Int64)])?;
    let factory = HashJoinFactory::create(ctx, function_ctx, method, desc);
    let mut join = factory.create_memory_join(JoinType::Right, 0)?;

    join.add_block(None)?;
    while join.final_build()?.is_some() {}

    // The invalid cast proves that the actual probe-key evaluator is not reached.
    let mut stream = join.probe_block(DataBlock::new(vec![], 1))?;
    assert!(stream.next()?.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn right_outer_join_stops_probe_pipeline_when_build_is_empty() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings()
        .set_setting("enable_experimental_new_join".to_string(), "1".to_string())?;
    ctx.get_settings()
        .set_setting("enable_join_runtime_filter".to_string(), "0".to_string())?;

    // The invalid cast is evaluated by an EvalScalar in the probe pipeline, before the join.
    // An empty preserved/build side must stop that pipeline instead of merely discarding its rows.
    let query = r#"
        SELECT probe.number
        FROM numbers(10) AS probe
        RIGHT JOIN (
            SELECT number
            FROM numbers(1)
            WHERE number > 10
        ) AS build
            ON to_int64(concat('invalid-', to_string(probe.number))) = build.number
    "#;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(query).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let blocks: Vec<DataBlock> = interpreter.execute(ctx).await?.try_collect().await?;

    assert_eq!(blocks.iter().map(DataBlock::num_rows).sum::<usize>(), 0);
    Ok(())
}
