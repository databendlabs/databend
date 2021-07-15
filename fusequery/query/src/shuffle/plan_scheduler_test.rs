// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataValue;
use common_exception::Result;
use common_flights::Address;
use common_management::cluster::ClusterExecutor;
use common_planners::*;
use common_runtime::tokio;

use crate::sessions::FuseQueryContextRef;
use crate::shuffle::PlanScheduler;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scheduler_plan_without_stage() -> Result<()> {
    let ctx = create_env().await?;
    let scheduled_actions = PlanScheduler::reschedule(
        ctx.clone(),
        HashMap::<String, bool>::new(),
        &PlanNode::Empty(EmptyPlan::create()),
    )
    .await?;

    assert!(scheduled_actions.remote_actions.is_empty());
    assert_eq!(
        scheduled_actions.local_plan,
        PlanNode::Empty(EmptyPlan::create())
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scheduler_plan_with_one_normal_stage() -> Result<()> {
    let ctx = create_env().await?;
    let reschedule_res = PlanScheduler::reschedule(
        ctx.clone(),
        HashMap::<String, bool>::new(),
        &PlanNode::Stage(StagePlan {
            kind: StageKind::Normal,
            scatters_expr: Expression::Literal(DataValue::UInt64(Some(1))),
            input: Arc::new(PlanNode::Empty(EmptyPlan::create())),
        }),
    )
    .await;

    match reschedule_res {
        Ok(_) => assert!(
            false,
            "test_scheduler_plan_with_one_normal_stage must be failure!"
        ),
        Err(error_code) => {
            assert_eq!(error_code.code(), 32);
            assert_eq!(
                error_code.message(),
                "The final stage plan must be convergent"
            );
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scheduler_plan_with_one_expansive_stage() -> Result<()> {
    let ctx = create_env().await?;
    let reschedule_res = PlanScheduler::reschedule(
        ctx.clone(),
        HashMap::<String, bool>::new(),
        &PlanNode::Stage(StagePlan {
            kind: StageKind::Expansive,
            scatters_expr: Expression::Literal(DataValue::UInt64(Some(1))),
            input: Arc::new(PlanNode::Empty(EmptyPlan::create())),
        }),
    )
    .await;

    match reschedule_res {
        Ok(_) => assert!(
            false,
            "test_scheduler_plan_with_one_expansive_stage must be failure!"
        ),
        Err(error_code) => {
            assert_eq!(error_code.code(), 32);
            assert_eq!(
                error_code.message(),
                "The final stage plan must be convergent"
            );
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scheduler_plan_with_one_convergent_stage() -> Result<()> {
    /*
     *  +------------------+
     *  |                  |
     *  |     EmptyPlan    +--------------------------+
     *  |                  |                          |
     *  +------------------+                          |
     *                                       +--------v---------+
     *                                       |                  |
     *                                       |    Remote Plan   |
     *                                       |                  |
     *  +------------------+                 +--------^---------+
     *  |                  |                          |
     *  |     EmptyPlan    +--------------------------+
     *  |                  |
     *  +------------------+
     */
    let ctx = create_env().await?;
    let scheduled_actions = PlanScheduler::reschedule(
        ctx.clone(),
        HashMap::<String, bool>::new(),
        &PlanNode::Stage(StagePlan {
            kind: StageKind::Convergent,
            scatters_expr: Expression::Literal(DataValue::UInt64(Some(0))),
            input: Arc::new(PlanNode::Empty(EmptyPlan::create())),
        }),
    )
    .await?;

    assert_eq!(scheduled_actions.remote_actions.len(), 2);
    assert_eq!(
        scheduled_actions.remote_actions[0].0.name,
        String::from("dummy_local")
    );
    assert_eq!(scheduled_actions.remote_actions[0].1.scatters, vec![
        String::from("dummy_local")
    ]);
    assert_eq!(
        scheduled_actions.remote_actions[0].1.scatters_action,
        Expression::Literal(DataValue::UInt64(Some(0)))
    );
    assert_eq!(
        scheduled_actions.remote_actions[0].1.plan,
        PlanNode::Empty(EmptyPlan::create())
    );

    assert_eq!(
        scheduled_actions.remote_actions[1].0.name,
        String::from("dummy")
    );
    assert_eq!(scheduled_actions.remote_actions[1].1.scatters, vec![
        String::from("dummy_local")
    ]);
    assert_eq!(
        scheduled_actions.remote_actions[1].1.scatters_action,
        Expression::Literal(DataValue::UInt64(Some(0)))
    );
    assert_eq!(
        scheduled_actions.remote_actions[1].1.plan,
        PlanNode::Empty(EmptyPlan::create())
    );

    match scheduled_actions.local_plan {
        PlanNode::Remote(plan) => {
            assert!(plan.fetch_name.ends_with("/dummy_local"));
            assert_eq!(plan.fetch_nodes, ["dummy_local", "dummy"]);
        }
        _ => assert!(
            false,
            "test_scheduler_plan_with_one_convergent_stage must be have Remote plan!"
        ),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scheduler_plan_with_convergent_and_expansive_stage() -> Result<()> {
    /*
     *                  +-----------+       +-----------+
     *        +-------->|RemotePlan +------>|SelectPlan +-----------+
     *        |         +-----------+       +-----------+           |
     *        |                                                     |
     *        |                                                     v
     *   +----+------+                                        +-----------+        +-----------+
     *   | EmptyPlan |                                        |RemotePlan +------->|SelectPlan |
     *   +----+------+                                        +-----------+        +-----------+
     *        |                                                     ^
     *        |         +-----------+       +-----------+           |
     *        +-------->|RemotePlan +------>|SelectPlan +-----------+
     *                  +-----------+       +-----------+
     */
    let ctx = create_env().await?;
    let scheduled_actions = PlanScheduler::reschedule(
        ctx.clone(),
        HashMap::<String, bool>::new(),
        &PlanNode::Select(SelectPlan {
            input: Arc::new(PlanNode::Stage(StagePlan {
                kind: StageKind::Convergent,
                scatters_expr: Expression::Literal(DataValue::UInt64(Some(0))),
                input: Arc::new(PlanNode::Select(SelectPlan {
                    input: Arc::new(PlanNode::Stage(StagePlan {
                        kind: StageKind::Expansive,
                        scatters_expr: Expression::ScalarFunction {
                            op: String::from("blockNumber"),
                            args: vec![],
                        },
                        input: Arc::new(PlanNode::Empty(EmptyPlan::create())),
                    })),
                })),
            })),
        }),
    )
    .await?;

    assert_eq!(scheduled_actions.remote_actions.len(), 3);
    assert_eq!(
        scheduled_actions.remote_actions[0].0.name,
        String::from("dummy_local")
    );
    assert_eq!(scheduled_actions.remote_actions[0].1.scatters, vec![
        String::from("dummy_local"),
        String::from("dummy")
    ]);
    assert_eq!(
        scheduled_actions.remote_actions[0].1.scatters_action,
        Expression::ScalarFunction {
            op: String::from("blockNumber"),
            args: vec![]
        }
    );
    assert_eq!(
        scheduled_actions.remote_actions[0].1.plan,
        PlanNode::Empty(EmptyPlan::create())
    );

    assert_eq!(
        scheduled_actions.remote_actions[1].0.name,
        String::from("dummy_local")
    );
    assert_eq!(scheduled_actions.remote_actions[1].1.scatters, vec![
        String::from("dummy_local")
    ]);
    assert_eq!(
        scheduled_actions.remote_actions[1].1.scatters_action,
        Expression::Literal(DataValue::UInt64(Some(0)))
    );

    assert_eq!(
        scheduled_actions.remote_actions[2].0.name,
        String::from("dummy")
    );
    assert_eq!(scheduled_actions.remote_actions[2].1.scatters, vec![
        String::from("dummy_local")
    ]);
    assert_eq!(
        scheduled_actions.remote_actions[2].1.scatters_action,
        Expression::Literal(DataValue::UInt64(Some(0)))
    );

    // Perform the same plan in different nodes
    match (
        &scheduled_actions.remote_actions[1].1.plan,
        &scheduled_actions.remote_actions[2].1.plan,
        &scheduled_actions.local_plan,
    ) {
        (PlanNode::Select(left), PlanNode::Select(right), PlanNode::Select(finalize)) => {
            match (&*left.input, &*right.input, &*finalize.input) {
                (PlanNode::Remote(left), PlanNode::Remote(right), PlanNode::Remote(finalize)) => {
                    assert!(left.fetch_name.ends_with("/dummy_local"));
                    assert!(right.fetch_name.ends_with("/dummy"));
                    assert_eq!(left.fetch_nodes, ["dummy_local"]);
                    assert_eq!(right.fetch_nodes, ["dummy_local"]);

                    assert!(finalize.fetch_name.ends_with("/dummy_local"));
                    assert_eq!(finalize.fetch_nodes, ["dummy_local", "dummy"]);
                },
                _ => assert!(false, "test_scheduler_plan_with_convergent_and_expansive_stage must be have Remote plan!"),
            }
        }
        _ => assert!(
            false,
            "test_scheduler_plan_with_convergent_and_expansive_stage must be have Select plan!"
        ),
    };

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scheduler_plan_with_convergent_and_normal_stage() -> Result<()> {
    /*
     *   +-----------+      +-----------+       +-----------+
     *   |EmptyStage +----->|RemotePlan +------>|SelectPlan +-----------+
     *   +-------+---+      +-----------+       +-----------+           |
     *           |               ^                                      |
     *           +------------+  |                                      v
     *                        |  |                                +-----------+      +-----------+
     *           +------------+--+                                |RemotePlan +----> |SelectPlan |
     *           |            |                                   +-----------+      +-----------+
     *           |            v                                         ^
     *   +-------+---+      +-----------+       +-----------+           |
     *   |EmptyStage +----->|RemotePlan +------>|SelectPlan +-----------+
     *   +-----------+      +-----------+       +-----------+
     */
    let ctx = create_env().await?;
    let scheduled_actions = PlanScheduler::reschedule(
        ctx.clone(),
        HashMap::<String, bool>::new(),
        &PlanNode::Select(SelectPlan {
            input: Arc::new(PlanNode::Stage(StagePlan {
                kind: StageKind::Convergent,
                scatters_expr: Expression::Literal(DataValue::UInt64(Some(1))),
                input: Arc::new(PlanNode::Select(SelectPlan {
                    input: Arc::new(PlanNode::Stage(StagePlan {
                        kind: StageKind::Normal,
                        scatters_expr: Expression::Literal(DataValue::UInt64(Some(0))),
                        input: Arc::new(PlanNode::Empty(EmptyPlan::create())),
                    })),
                })),
            })),
        }),
    )
    .await?;

    assert_eq!(scheduled_actions.remote_actions.len(), 4);
    assert_eq!(
        scheduled_actions.remote_actions[0].0.name,
        String::from("dummy_local")
    );
    assert_eq!(scheduled_actions.remote_actions[0].1.scatters, vec![
        String::from("dummy_local"),
        String::from("dummy")
    ]);
    assert_eq!(
        scheduled_actions.remote_actions[0].1.scatters_action,
        Expression::Literal(DataValue::UInt64(Some(0)))
    );
    assert_eq!(
        scheduled_actions.remote_actions[0].1.plan,
        PlanNode::Empty(EmptyPlan::create())
    );

    assert_eq!(
        scheduled_actions.remote_actions[2].0.name,
        String::from("dummy")
    );
    assert_eq!(scheduled_actions.remote_actions[2].1.scatters, vec![
        String::from("dummy_local"),
        String::from("dummy")
    ]);
    assert_eq!(
        scheduled_actions.remote_actions[2].1.scatters_action,
        Expression::Literal(DataValue::UInt64(Some(0)))
    );
    assert_eq!(
        scheduled_actions.remote_actions[2].1.plan,
        PlanNode::Empty(EmptyPlan::create())
    );

    assert_eq!(
        scheduled_actions.remote_actions[1].0.name,
        String::from("dummy_local")
    );
    assert_eq!(scheduled_actions.remote_actions[1].1.scatters, vec![
        String::from("dummy_local")
    ]);
    assert_eq!(
        scheduled_actions.remote_actions[1].1.scatters_action,
        Expression::Literal(DataValue::UInt64(Some(1)))
    );

    assert_eq!(
        scheduled_actions.remote_actions[3].0.name,
        String::from("dummy")
    );
    assert_eq!(scheduled_actions.remote_actions[3].1.scatters, vec![
        String::from("dummy_local")
    ]);
    assert_eq!(
        scheduled_actions.remote_actions[3].1.scatters_action,
        Expression::Literal(DataValue::UInt64(Some(1)))
    );

    // Perform the same plan in different nodes
    match (
        &scheduled_actions.remote_actions[1].1.plan,
        &scheduled_actions.remote_actions[3].1.plan,
        &scheduled_actions.local_plan,
    ) {
        (PlanNode::Select(left), PlanNode::Select(right), PlanNode::Select(finalize)) => {
            match (&*left.input, &*right.input, &*finalize.input) {
                (PlanNode::Remote(left), PlanNode::Remote(right), PlanNode::Remote(finalize)) => {
                    assert!(left.fetch_name.ends_with("/dummy_local"));
                    assert!(right.fetch_name.ends_with("/dummy"));
                    assert_eq!(left.fetch_nodes, ["dummy_local", "dummy"]);
                    assert_eq!(right.fetch_nodes, ["dummy_local", "dummy"]);

                    assert!(finalize.fetch_name.ends_with("/dummy_local"));
                    assert_eq!(finalize.fetch_nodes, ["dummy_local", "dummy"]);
                },
                _ => assert!(false, "test_scheduler_plan_with_convergent_and_expansive_stage must be have Remote plan!"),
            }
        }
        _ => assert!(
            false,
            "test_scheduler_plan_with_convergent_and_expansive_stage must be have Select plan!"
        ),
    };

    Ok(())
}

async fn create_env() -> Result<FuseQueryContextRef> {
    let ctx = crate::tests::try_create_context()?;
    let registry = crate::tests::start_cluster_registry().await?;
    let namespace = ctx.get_config().cluster_namespace;
    crate::tests::register_one_executor_to_namespace(
        registry.clone(),
        namespace.clone(),
        &ClusterExecutor {
            name: "dummy_local".to_string(),
            priority: 1,
            address: Address::create("localhost:9090")?,
            local: false,
            sequence: 0,
        },
    )
    .await?;
    crate::tests::register_one_executor_to_namespace(registry, namespace, &ClusterExecutor {
        name: "dummy".to_string(),
        priority: 1,
        address: Address::create("github:9090")?,
        local: false,
        sequence: 0,
    })
    .await?;
    Ok(ctx)
}
