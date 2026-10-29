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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_sql::plans::JoinType;
use databend_query::physical_plans::ConstantTableScan;
use databend_query::physical_plans::HashJoin;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanMeta;
use databend_query::physical_plans::PhysicalRuntimeFilters;
use databend_query::pipelines::processors::HashJoinDesc;
use databend_query::pipelines::processors::transforms::HashJoinFactory;
use databend_query::pipelines::processors::transforms::Join;
use databend_query::test_kits::TestFixture;

fn constant_plan() -> PhysicalPlan {
    PhysicalPlan::new(ConstantTableScan {
        meta: PhysicalPlanMeta::new("ConstantTableScan"),
        values: vec![],
        num_rows: 0,
        output_schema: Arc::new(DataSchema::empty()),
    })
}

fn int64_key(value: i64) -> Expr {
    Expr::constant(
        Scalar::Number(NumberScalar::Int64(value)),
        Some(DataType::Number(NumberDataType::Int64)),
    )
}

fn hash_join_plan(join_type: JoinType) -> HashJoin {
    HashJoin {
        meta: PhysicalPlanMeta::new("HashJoin"),
        projections: BTreeSet::new(),
        probe_projections: BTreeSet::new(),
        build_projections: BTreeSet::new(),
        build: constant_plan(),
        probe: constant_plan(),
        build_keys: vec![int64_key(0).as_remote_expr()],
        probe_keys: vec![int64_key(1).as_remote_expr()],
        is_null_equal: vec![false],
        non_equi_conditions: vec![],
        join_type,
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
    }
}

/// `can_skip_probe` must cover exactly the join types whose output is empty once the build
/// side is empty. A missing type silently scans the whole probe side; an extra type drops
/// preserved probe rows. Pin every type the new hash join dispatches.
#[tokio::test(flavor = "multi_thread")]
async fn can_skip_probe_covers_join_types_emptied_by_empty_build() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let method =
        DataBlock::choose_hash_method_with_types(&[DataType::Number(NumberDataType::Int64)])?;

    // Left and LeftAnti still emit their preserved probe rows, so they must keep probing.
    let cases = [
        (JoinType::Inner, true),
        (JoinType::LeftSemi, true),
        (JoinType::Right, true),
        (JoinType::RightSemi, true),
        (JoinType::RightAnti, true),
        (JoinType::Left, false),
        (JoinType::LeftAnti, false),
    ];

    for (join_type, expected) in cases {
        let desc = Arc::new(HashJoinDesc::create(&hash_join_plan(join_type))?);
        let factory = HashJoinFactory::create(
            ctx.clone(),
            FunctionContext::default(),
            method.clone(),
            desc,
        );
        let join = factory.create_hybrid_join(join_type, factory.create_level_states(0)?)?;
        assert_eq!(
            join.can_skip_probe(),
            expected,
            "unexpected can_skip_probe for {join_type:?}"
        );
    }
    Ok(())
}
