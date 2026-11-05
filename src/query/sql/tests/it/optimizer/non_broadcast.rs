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
use databend_common_sql::optimizer::ir::PhysicalProperty;
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
use databend_common_sql::plans::PhysicalRelOperator;
use databend_common_sql::plans::Plan;
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
fn test_non_broadcast_requirement_cannot_be_enforced() {
    let required = Distribution::NonBroadcast;
    let enforcer = DistributionEnforcer::new(required.clone());
    // Hash keys need not match: this requirement is about replication, not colocation.
    for delivered in [
        Distribution::Random,
        Distribution::Serial,
        Distribution::NodeToNodeHash(vec![key(2)]),
        Distribution::GlobalHash(vec![key(3)]),
    ] {
        assert!(required.satisfied_by(&delivered));
        assert!(!enforcer.check_enforce(&PhysicalProperty {
            distribution: delivered,
        }));
    }
    for delivered in [
        Distribution::Broadcast,
        Distribution::Any,
        Distribution::NonBroadcast,
    ] {
        assert!(!required.satisfied_by(&delivered));
        assert!(enforcer.check_enforce(&PhysicalProperty {
            distribution: delivered,
        }));
    }
    // Neither hash nor merge exchanges can recover the original multiplicity.
    assert!(enforcer.enforce().is_err());
    // Do not change the existing GlobalHash contract for ordinary joins.
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
async fn test_mark_join_child_requirements() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_cluster_node_num(3);
    // The nullable requirement must take priority even when generic broadcast is disabled.
    ctx.get_settings()
        .set_setting("enforce_shuffle_join".to_string(), "1".to_string())?;
    for (direction, expected) in [
        (JoinType::LeftMark, vec![
            Distribution::Broadcast,
            Distribution::NonBroadcast,
        ]),
        (JoinType::RightMark, vec![
            Distribution::NonBroadcast,
            Distribution::Broadcast,
        ]),
    ] {
        let join = mark_join(direction);
        let expr = PExpr::create_binary(
            join.clone(),
            PExpr::create_leaf(Scan::default()),
            PExpr::create_leaf(Scan::default()),
        );
        let rel = RelExpr::with_p_expr(&expr);
        let required: Vec<_> = expected
            .into_iter()
            .map(|distribution| RequiredProperty { distribution })
            .collect();
        assert_eq!(
            join.compute_required_prop_children(ctx.clone(), &rel, &RequiredProperty::default())?,
            vec![required.clone()]
        );
        for (index, required) in required.iter().enumerate() {
            assert_eq!(
                join.compute_required_prop_child(
                    ctx.clone(),
                    &rel,
                    index,
                    &RequiredProperty::default(),
                )?,
                *required
            );
        }

        // The optimization applies only to single nullable mark keys.
        for other in [
            Join {
                join_type: JoinType::Inner,
                ..join.clone()
            },
            Join {
                equi_conditions: vec![JoinEquiCondition::new(key(1), key(2), false)],
                ..join.clone()
            },
            Join {
                equi_conditions: vec![join.equi_conditions[0].clone(); 2],
                ..join.clone()
            },
        ] {
            let alternatives = other.compute_required_prop_children(
                ctx.clone(),
                &rel,
                &RequiredProperty::default(),
            )?;
            assert!(
                alternatives
                    .iter()
                    .flatten()
                    .all(|prop| prop.distribution != Distribution::NonBroadcast)
            );
        }
    }

    // Preserve the main fallback's small Serial-build optimization, but never
    // let it accept a replicated output side through its generic Any requirement.
    ctx.get_settings()
        .set_setting("enforce_shuffle_join".to_string(), "0".to_string())?;
    ctx.get_settings()
        .set_setting("enforce_broadcast_join".to_string(), "1".to_string())?;
    let join = mark_join(JoinType::RightMark);
    let expr = PExpr::create_binary(
        join.clone(),
        PExpr::create_unary(Exchange::Broadcast, PExpr::create_leaf(Scan::default())),
        PExpr::create_unary(Exchange::Merge, PExpr::create_leaf(Scan::default())),
    );
    assert_eq!(
        join.compute_required_prop_child(
            ctx.clone(),
            &RelExpr::with_p_expr(&expr),
            0,
            &RequiredProperty::default(),
        )?
        .distribution,
        Distribution::NonBroadcast
    );

    let enforcer = PropertyEnforcer::new(ctx);
    let required = Distribution::NonBroadcast;
    for exchange in [
        Exchange::Merge,
        Exchange::NodeToNodeHash(vec![key(2)]),
        Exchange::GlobalHash(vec![key(3)]),
    ] {
        let input = PExpr::create_unary(exchange, PExpr::create_leaf(Scan::default()));
        assert_eq!(enforcer.enforce_distribution(&required, &input)?, input);
    }
    let input = PExpr::create_leaf(Scan::default());
    assert_eq!(enforcer.enforce_distribution(&required, &input)?, input);
    let replicated = PExpr::create_unary(Exchange::Broadcast, input);
    let projected = PExpr::create_unary(EvalScalar { items: vec![] }, replicated.clone());
    let replicated_join = PExpr::create_binary(
        Join {
            join_type: JoinType::Inner,
            ..Default::default()
        },
        replicated.clone(),
        replicated.clone(),
    );
    // These are physical property boundary tests, not fabricated rewrite evidence.
    // Reject both direct and inherited replication before creating any new exchange.
    for input in [replicated, projected, replicated_join] {
        assert_eq!(
            RelExpr::with_p_expr(&input)
                .derive_physical_prop()?
                .distribution,
            Distribution::Broadcast
        );
        let error = enforcer
            .enforce_distribution(&required, &input)
            .unwrap_err();
        assert!(error.message().contains("replicated or unknown input"));
    }
    Ok(())
}

fn check_mark_inputs(expr: &PExpr) -> Result<usize> {
    let mut count = 0;
    if let PhysicalRelOperator::Join(join) = expr.plan()
        && join.join_type.is_mark_join()
    {
        let (output, subquery) = match join.join_type {
            JoinType::LeftMark => (1, 0),
            JoinType::RightMark => (0, 1),
            _ => unreachable!(),
        };
        assert_eq!(
            RelExpr::with_p_expr(expr.child(output)?)
                .derive_physical_prop()?
                .distribution,
            Distribution::Random,
            "the table's existing distribution should survive the mark join"
        );
        assert_eq!(
            RelExpr::with_p_expr(expr.child(subquery)?)
                .derive_physical_prop()?
                .distribution,
            Distribution::Broadcast,
            "NULL knowledge must cover the entire subquery"
        );
        count += 1;
    }
    assert!(!matches!(
        RelExpr::with_p_expr(expr)
            .derive_physical_prop()?
            .distribution,
        Distribution::NonBroadcast
    ));
    for child in expr.children() {
        count += check_mark_inputs(child)?;
    }
    Ok(count)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sql_mark_join_retains_table_distribution() -> Result<()> {
    // Exercise both Cascades and the production fallback, starting from bound SQL.
    for cbo in ["1", "0"] {
        let ctx = LiteTableContext::create().await?;
        ctx.set_cluster_node_num(3);
        ctx.set_table_warehouse_distribution(true);
        ctx.get_settings()
            .set_setting("enable_cbo".to_string(), cbo.to_string())?;
        ctx.get_settings()
            .set_setting("disable_join_reorder".to_string(), "1".to_string())?;
        ctx.register_setup_sql("CREATE TABLE mark_rows (k INT)")
            .await?;
        ctx.register_setup_sql("CREATE TABLE mark_values (k INT)")
            .await?;
        let raw = ctx
            .bind_sql("SELECT k, k IN (SELECT k FROM mark_values) AS present FROM mark_rows")
            .await?;
        let optimized = ctx.optimize_plan(raw).await?;
        let Plan::Query {
            s_expr, metadata, ..
        } = &optimized
        else {
            unreachable!()
        };
        let physical = s_expr.planned()?.expr();
        physical.validate_types(metadata)?;
        physical.validate_column_scope(metadata)?;
        assert_eq!(check_mark_inputs(physical)?, 1);
    }
    Ok(())
}
