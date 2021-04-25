// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_scheduler_plan_with_one_node() -> anyhow::Result<()> {
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::planners::PlanScheduler;

    let ctx = crate::tests::try_create_context()?;

    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let source = test_source.number_read_source_plan_for_test(100000)?;

    let plan = PlanBuilder::from(&PlanNode::ReadSource(source))
        .filter(col("number").eq(lit(1i64)))?
        .project(vec![col("number")])?
        .build()?;

    let plans = PlanScheduler::reschedule(ctx, &plan)?;
    let expects = vec!["Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 100000, read_bytes: 800000]"];

    for (i, (_, plan)) in plans.iter().enumerate() {
        let actual = format!("{:?}", plan);
        assert_eq!(expects[i], actual);
    }
    Ok(())
}

#[test]
fn test_scheduler_plan_with_more_cpus_1_node() -> anyhow::Result<()> {
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::planners::PlanScheduler;

    let ctx = crate::tests::try_create_context()?;
    let cpus = ctx.get_max_threads()?;

    // For more partitions generation.
    let ctx_more_cpu = crate::tests::try_create_context()?;
    ctx_more_cpu.set_max_threads(cpus * 40)?;
    let test_source = crate::tests::NumberTestData::create(ctx_more_cpu.clone());
    let source = test_source.number_read_source_plan_for_test(100000)?;

    let plan = PlanBuilder::from(&PlanNode::ReadSource(source))
        .filter(col("number").eq(lit(1i64)))?
        .project(vec![col("number")])?
        .build()?;

    let plans = PlanScheduler::reschedule(ctx, &plan)?;
    let expects = vec!["Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan partitions: [320], scan schema: [number:UInt64], statistics: [read_rows: 100000, read_bytes: 800000]"];

    for (i, plan) in plans.iter().enumerate() {
        let actual = format!("{:?}", plan);
        assert_eq!(expects[i], actual);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scheduler_plan_with_3_nodes() -> anyhow::Result<()> {
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::planners::PlanScheduler;

    let ctx = crate::tests::try_create_context_with_nodes(3).await?;
    let cpus = ctx.get_max_threads()?;

    // For more partitions generation.
    let ctx_more_cpu = crate::tests::try_create_context()?;
    ctx_more_cpu.set_max_threads(cpus * 40)?;
    let test_source = crate::tests::NumberTestData::create(ctx_more_cpu.clone());
    let source = test_source.number_read_source_plan_for_test(100000)?;

    let plan = PlanBuilder::from(&PlanNode::ReadSource(source))
        .filter(col("number").eq(lit(1i64)))?
        .project(vec![col("number")])?
        .build()?;

    let plans = PlanScheduler::reschedule(ctx, &plan)?;
    let expects = vec!["Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan partitions: [107], scan schema: [number:UInt64], statistics: [read_rows: 100000, read_bytes: 800000]",
"Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan partitions: [107], scan schema: [number:UInt64], statistics: [read_rows: 100000, read_bytes: 800000]",
"Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan partitions: [106], scan schema: [number:UInt64], statistics: [read_rows: 100000, read_bytes: 800000]",
    ];

    for (i, (_, plan)) in plans.iter().enumerate() {
        let actual = format!("{:?}", plan);
        assert_eq!(expects[i], actual);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scheduler_plan_with_3_nodes_diff_priority() -> anyhow::Result<()> {
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::planners::PlanScheduler;

    let p = vec![2u8, 3u8, 5u8];
    let ctx = crate::tests::try_create_context_with_nodes_and_priority(3, &p).await?;
    let cpus = ctx.get_max_threads()?;

    // For more partitions generation.
    let ctx_more_cpu = crate::tests::try_create_context()?;
    ctx_more_cpu.set_max_threads(cpus * 40)?;
    let test_source = crate::tests::NumberTestData::create(ctx_more_cpu.clone());
    let source = test_source.number_read_source_plan_for_test(100000)?;

    let plan = PlanBuilder::from(&PlanNode::ReadSource(source))
        .filter(col("number").eq(lit(1i64)))?
        .project(vec![col("number")])?
        .build()?;

    let plans = PlanScheduler::reschedule(ctx, &plan)?;
    let expects = vec!["Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan partitions: [161], scan schema: [number:UInt64], statistics: [read_rows: 100000, read_bytes: 800000]",
"Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan partitions: [97], scan schema: [number:UInt64], statistics: [read_rows: 100000, read_bytes: 800000]",
"Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan partitions: [62], scan schema: [number:UInt64], statistics: [read_rows: 100000, read_bytes: 800000]",
    ];

    for (i, (_, plan)) in plans.iter().enumerate() {
        let actual = format!("{:?}", plan);
        assert_eq!(expects[i], actual);
    }
    Ok(())
}
