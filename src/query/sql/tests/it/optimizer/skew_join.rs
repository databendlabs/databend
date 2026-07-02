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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::Distribution;
use databend_common_sql::optimizer::ir::PhysicalProperty;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::RequiredProperty;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::plans::SkewHashInfo;
use databend_common_sql::plans::SkewHashRole;
use databend_common_statistics::Datum;
use databend_storages_common_table_meta::meta::ColumnTopN;
use databend_storages_common_table_meta::meta::ColumnTopNEntry;

use crate::framework::LiteTableContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_topn_skew_join_enumerates_skew_hash_required_properties() -> Result<()> {
    let ctx = setup_skew_join_context().await?;
    let plan = optimize_skew_join_query(&ctx).await?;

    let join_expr = find_join(plan_s_expr(&plan)).expect("optimized plan should contain a join");
    let rel_expr = RelExpr::with_s_expr(join_expr);
    let children_required =
        rel_expr.compute_required_prop_children(ctx, &RequiredProperty::default())?;

    assert_required_property_counts(&children_required, 1, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_topn_skew_join_disabled_by_default_uses_normal_hash() -> Result<()> {
    let ctx = setup_skew_join_context_with_default_skew_join_setting().await?;
    let plan = optimize_skew_join_query(&ctx).await?;

    let join_expr = find_join(plan_s_expr(&plan)).expect("optimized plan should contain a join");
    let rel_expr = RelExpr::with_s_expr(join_expr);
    let children_required =
        rel_expr.compute_required_prop_children(ctx, &RequiredProperty::default())?;

    assert_required_property_counts(&children_required, 1, 0);
    assert!(
        find_skew_hash_info(plan_s_expr(&plan), SkewHashRole::Probe).is_none(),
        "skew join should be disabled by default and avoid probe-side skew exchange"
    );
    assert!(
        find_skew_hash_info(plan_s_expr(&plan), SkewHashRole::Build).is_none(),
        "skew join should be disabled by default and avoid build-side skew exchange"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_topn_skew_join_uses_skew_hash_exchange() -> Result<()> {
    let ctx = setup_skew_join_context().await?;
    let plan = optimize_skew_join_query(&ctx).await?;

    let probe_info = find_skew_hash_info(plan_s_expr(&plan), SkewHashRole::Probe)
        .expect("optimized plan should use probe-side skew hash exchange");
    let build_info = find_skew_hash_info(plan_s_expr(&plan), SkewHashRole::Build)
        .expect("optimized plan should use build-side skew hash exchange");

    assert_eq!(probe_info.role, SkewHashRole::Probe);
    assert_eq!(build_info.role, SkewHashRole::Build);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_topn_skew_join_cost_prefers_skew_hash_by_margin() -> Result<()> {
    let ctx = setup_skew_join_context().await?;
    let plan = optimize_skew_join_query(&ctx).await?;
    let probe_info = find_skew_hash_info(plan_s_expr(&plan), SkewHashRole::Probe)
        .expect("optimized plan should contain probe-side skew hash exchange");

    // The common hash-join base cost and the non-duplicated exchange cost are
    // identical for normal hash and skew hash. The discriminating terms are:
    // - normal hash: probe-side straggler penalty;
    // - skew hash: residual straggler penalty plus build-side replication.
    let compute_per_row = 1.0;
    let settings = ctx.get_settings();
    let hash_table_per_row = settings.get_cost_factor_hash_table_per_row()? as f64;
    let network_per_row = settings.get_cost_factor_network_per_row()? as f64;

    let normal_hash_extra_cost = probe_info.normal_skew_penalty_rows as f64 * compute_per_row;
    let skew_hash_extra_cost = probe_info.skew_skew_penalty_rows as f64 * compute_per_row
        + probe_info.extra_build_rows as f64
            * (network_per_row + compute_per_row + hash_table_per_row);

    assert_eq!(probe_info.hot_keys.len(), 1);
    assert_eq!(probe_info.bucket_count, 3);
    assert_eq!(probe_info.normal_skew_penalty_rows, 781_667);
    assert_eq!(probe_info.skew_skew_penalty_rows, 121_667);
    assert_eq!(probe_info.extra_build_rows, 2);
    assert!(
        normal_hash_extra_cost > skew_hash_extra_cost * 5.0,
        "skew hash should be much cheaper for this synthetic hot-key case: \
         normal_extra={normal_hash_extra_cost}, skew_extra={skew_hash_extra_cost}"
    );
    Ok(())
}

#[test]
fn test_skew_hash_distribution_is_not_normal_hash_equivalent() {
    let keys = vec![column_expr(0)];
    let probe_skew_info = test_skew_hash_info(SkewHashRole::Probe);

    let skew_required = RequiredProperty {
        distribution: Distribution::GlobalSkewHash(keys.clone(), probe_skew_info.clone()),
    };
    let normal_physical = PhysicalProperty {
        distribution: Distribution::GlobalHash(keys.clone()),
    };
    assert!(
        !skew_required.satisfied_by(&normal_physical),
        "normal hash shuffle must not satisfy skew hash distribution"
    );

    let normal_required = RequiredProperty {
        distribution: Distribution::GlobalHash(keys.clone()),
    };
    let skew_physical = PhysicalProperty {
        distribution: Distribution::GlobalSkewHash(keys.clone(), probe_skew_info.clone()),
    };
    assert!(
        !normal_required.satisfied_by(&skew_physical),
        "skew hash shuffle must not satisfy normal hash distribution"
    );

    let same_skew_physical = PhysicalProperty {
        distribution: Distribution::GlobalSkewHash(keys.clone(), probe_skew_info.clone()),
    };
    assert!(
        skew_required.satisfied_by(&same_skew_physical),
        "identical skew hash distribution should satisfy the requirement"
    );

    let different_skew_physical = PhysicalProperty {
        distribution: Distribution::GlobalSkewHash(keys, test_skew_hash_info(SkewHashRole::Build)),
    };
    assert!(
        !skew_required.satisfied_by(&different_skew_physical),
        "skew hash distributions with different skew info must not be equivalent"
    );
}

async fn setup_skew_join_context() -> Result<Arc<LiteTableContext>> {
    let ctx = setup_skew_join_context_with_default_skew_join_setting().await?;
    ctx.get_settings()
        .set_setting("enable_experimental_skew_join".to_string(), "1".to_string())?;
    Ok(ctx)
}

async fn setup_skew_join_context_with_default_skew_join_setting() -> Result<Arc<LiteTableContext>> {
    let ctx = LiteTableContext::create().await?;
    ctx.configure_for_optimizer_case(true)?;
    ctx.set_table_warehouse_distribution(true);
    ctx.set_cluster_node_num(3);
    ctx.get_settings().set_max_threads(8)?;
    ctx.get_settings()
        .set_setting("enforce_shuffle_join".to_string(), "1".to_string())?;

    // Keep the probe hot key dominant and the build hot key tiny so the skew
    // alternative wins even after charging build-side replication.
    register_table_with_hot_key(&ctx, "probe", 1_000_000, 990_000)?;
    register_table_with_hot_key(&ctx, "build", 100, 1)?;
    Ok(ctx)
}

async fn optimize_skew_join_query(ctx: &Arc<LiteTableContext>) -> Result<Plan> {
    ctx.optimize_plan(
        ctx.bind_sql("select count(*) from probe p inner join build b on p.k = b.k")
            .await?,
    )
    .await
}

fn register_table_with_hot_key(
    ctx: &Arc<LiteTableContext>,
    table_name: &str,
    rows: u64,
    hot_count: u64,
) -> Result<()> {
    ctx.register_table_with_stats_and_top_n(
        "default",
        table_name,
        vec![TableField::new(
            "k",
            TableDataType::Number(NumberDataType::UInt64),
        )],
        Some(TableStatistics {
            num_rows: Some(rows),
            data_size: Some(rows * 8),
            number_of_blocks: Some(1),
            number_of_segments: Some(1),
            ..Default::default()
        }),
        HashMap::from([("k".to_string(), BasicColumnStatistics {
            min: Some(Datum::UInt(1)),
            max: Some(Datum::UInt(1_000)),
            ndv: Some(1_000),
            null_count: 0,
            in_memory_size: rows * 8,
        })]),
        HashMap::new(),
        HashMap::from([("k".to_string(), ColumnTopN {
            values: vec![ColumnTopNEntry {
                scalar: uint64_scalar(1),
                count: hot_count,
                error: 0,
            }],
            min_index: None,
        })]),
        BTreeMap::new(),
    )
}

fn uint64_scalar(value: u64) -> Scalar {
    Scalar::Number(NumberScalar::UInt64(value))
}

fn column_expr(index: usize) -> ScalarExpr {
    ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            format!("c{index}"),
            Symbol::new(index),
            Box::new(DataType::Number(NumberDataType::UInt64)),
            Visibility::Visible,
        )
        .build(),
    })
}

fn test_skew_hash_info(role: SkewHashRole) -> SkewHashInfo {
    SkewHashInfo {
        role,
        hot_keys: vec![uint64_scalar(1)],
        bucket_count: 2,
        normal_skew_penalty_rows: 100,
        skew_skew_penalty_rows: 10,
        extra_build_rows: 1,
    }
}

fn plan_s_expr(plan: &Plan) -> &SExpr {
    match plan {
        Plan::Query { s_expr, .. } => s_expr,
        _ => unreachable!("test optimizer should return a query plan"),
    }
}

fn find_join(s_expr: &SExpr) -> Option<&SExpr> {
    if matches!(s_expr.plan(), RelOperator::Join(_)) {
        return Some(s_expr);
    }

    for child in s_expr.children() {
        if let Some(join) = find_join(child) {
            return Some(join);
        }
    }
    None
}

fn find_skew_hash_info(s_expr: &SExpr, role: SkewHashRole) -> Option<&SkewHashInfo> {
    if let RelOperator::Exchange(Exchange::GlobalSkewHash(_, skew_info)) = s_expr.plan()
        && skew_info.role == role
    {
        return Some(skew_info);
    }

    for child in s_expr.children() {
        if let Some(skew_info) = find_skew_hash_info(child, role) {
            return Some(skew_info);
        }
    }
    None
}

fn is_normal_hash_required(props: &Vec<RequiredProperty>) -> bool {
    matches!(props.as_slice(), [
        RequiredProperty {
            distribution: Distribution::GlobalHash(_),
        },
        RequiredProperty {
            distribution: Distribution::GlobalHash(_),
        },
    ])
}

fn assert_required_property_counts(
    children_required: &[Vec<RequiredProperty>],
    normal_hash_count: usize,
    skew_hash_count: usize,
) {
    assert_eq!(
        children_required
            .iter()
            .filter(|props| is_normal_hash_required(props))
            .count(),
        normal_hash_count,
        "unexpected normal hash required-property alternative count"
    );
    assert_eq!(
        children_required
            .iter()
            .filter(|props| is_skew_hash_required(props))
            .count(),
        skew_hash_count,
        "unexpected skew hash required-property alternative count"
    );
}

fn is_skew_hash_required(props: &Vec<RequiredProperty>) -> bool {
    matches!(
        props.as_slice(),
        [
            RequiredProperty {
                distribution: Distribution::GlobalSkewHash(_, probe_info),
            },
            RequiredProperty {
                distribution: Distribution::GlobalSkewHash(_, build_info),
            },
        ] if probe_info.role == SkewHashRole::Probe && build_info.role == SkewHashRole::Build
    )
}
