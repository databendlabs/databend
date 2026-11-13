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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_sql::optimizer::ir::Distribution;
use databend_common_sql::optimizer::ir::DistributionEnforcer;
use databend_common_sql::optimizer::ir::Enforcer;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::optimizer::ir::PropertyEnforcer;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::RequiredProperty;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::EvalScalar;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::PhysicalOperator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::plans::Scan;

use crate::framework::LiteTableContext;

fn key(value: i64) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr {
        span: None,
        value: Scalar::Number(value.into()),
    })
}

#[test]
fn test_non_broadcast_distribution_contract() {
    // This required-only property describes replication, not matching hash keys.
    let required = Distribution::NonBroadcast;
    for (delivered, accepted) in [
        (Distribution::Random, true),
        (Distribution::Serial, true),
        (Distribution::NodeToNodeHash(vec![key(2)]), true),
        (Distribution::GlobalHash(vec![key(3)]), true),
        (Distribution::Broadcast, false),
        (Distribution::Any, false),
        (Distribution::NonBroadcast, false),
    ] {
        assert_eq!(required.satisfied_by(&delivered), accepted, "{delivered}");
    }
    assert!(DistributionEnforcer::new(required).enforce().is_err());
    // The new requirement must not change the existing ordinary-join contract.
    assert!(Distribution::GlobalHash(vec![]).satisfied_by(&Distribution::Broadcast));
}

fn mark_join(join_type: JoinType) -> Join {
    Join {
        join_type,
        equi_conditions: vec![JoinEquiCondition::new(key(1), key(2), true)],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_non_broadcast_requirement_scope() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_cluster_node_num(3);
    // Positive SQL plans live in the cluster SLT. These are local preconditions,
    // including shapes that SQL rewrites need not leave intact for this operator.
    for direction in [JoinType::LeftMark, JoinType::RightMark] {
        let mark = mark_join(direction);
        for join in [
            Join {
                join_type: JoinType::Inner,
                ..mark.clone()
            },
            Join {
                equi_conditions: vec![],
                ..mark.clone()
            },
            Join {
                equi_conditions: vec![JoinEquiCondition::new(key(1), key(2), false)],
                ..mark.clone()
            },
            Join {
                equi_conditions: vec![mark.equi_conditions[0].clone(); 2],
                ..mark.clone()
            },
        ] {
            let expr = PExpr::create_binary(
                join.clone(),
                PExpr::create_leaf(Scan::default()),
                PExpr::create_leaf(Scan::default()),
            );
            let rel = RelExpr::with_p_expr(&expr);
            let required = RequiredProperty::default();
            let alternatives = join.compute_required_prop_children(ctx.clone(), &rel, &required)?;
            assert!(
                alternatives
                    .iter()
                    .flatten()
                    .all(|prop| { prop.distribution != Distribution::NonBroadcast })
            );
            for index in 0..2 {
                assert_ne!(
                    join.compute_required_prop_child(ctx.clone(), &rel, index, &required)?
                        .distribution,
                    Distribution::NonBroadcast
                );
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_serial_build_broadcast_keeps_non_broadcast_requirement() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_cluster_node_num(3);
    ctx.get_settings()
        .set_setting("enforce_broadcast_join".to_string(), "1".to_string())?;
    let join = mark_join(JoinType::RightMark);
    let expr = PExpr::create_binary(
        join.clone(),
        PExpr::create_unary(Exchange::Broadcast, PExpr::create_leaf(Scan::default())),
        PExpr::create_unary(Exchange::Merge, PExpr::create_leaf(Scan::default())),
    );
    // This early fallback branch must not use the generic Any requirement, which
    // would accept the replicated probe. It precedes the usual mark-join branch.
    assert_eq!(
        join.compute_required_prop_child(
            ctx,
            &RelExpr::with_p_expr(&expr),
            0,
            &RequiredProperty::default(),
        )?
        .distribution,
        Distribution::NonBroadcast
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_property_enforcer_rejects_inherited_replication() -> Result<()> {
    let enforcer = PropertyEnforcer::new(LiteTableContext::create().await?);
    let input = PExpr::create_leaf(Scan::default());
    let required = Distribution::NonBroadcast;
    assert_eq!(enforcer.enforce_distribution(&required, &input)?, input);

    // Check the physical-plan boundary once, including propagation through a
    // projection. The distribution matrix above covers the other concrete types.
    let replicated = PExpr::create_unary(
        EvalScalar { items: vec![] },
        PExpr::create_unary(Exchange::Broadcast, input),
    );
    let error = enforcer
        .enforce_distribution(&required, &replicated)
        .unwrap_err();
    assert!(error.message().contains("replicated or unknown input"));
    Ok(())
}
