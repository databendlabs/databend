// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_scheduler_plan_with_one_node() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::planners::*;

    let ctx = crate::tests::try_create_context()?;

    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let source = test_source.number_read_source_plan_for_test(100000)?;

    let plan = PlanBuilder::from(ctx.clone(), &PlanNode::ReadSource(source))
        .filter(field("number").eq(constant(1i64)))?
        .project(vec![field("number")])?
        .build()?;

    let plans = PlanScheduler::schedule(ctx, &plan)?;
    let expects = vec!["Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:100000, Read Bytes:800000)"];

    for (i, plan) in plans.iter().enumerate() {
        let actual = format!("{:?}", plan);
        assert_eq!(expects[i], actual);
    }
    Ok(())
}

#[test]
fn test_scheduler_plan_with_more_cpus_1_node() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::planners::*;

    let ctx = crate::tests::try_create_context()?;
    let cpus = ctx.get_max_threads()?;

    // For more partitions generation.
    let ctx_more_cpu = crate::tests::try_create_context()?;
    ctx_more_cpu.set_max_threads(cpus * 40)?;
    let test_source = crate::tests::NumberTestData::create(ctx_more_cpu.clone());
    let source = test_source.number_read_source_plan_for_test(100000)?;

    let plan = PlanBuilder::from(ctx.clone(), &PlanNode::ReadSource(source))
        .filter(field("number").eq(constant(1i64)))?
        .project(vec![field("number")])?
        .build()?;

    let plans = PlanScheduler::schedule(ctx, &plan)?;
    let expects = vec!["Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan parts [320](Read from system.numbers_mt table, Read Rows:100000, Read Bytes:800000)"];

    for (i, plan) in plans.iter().enumerate() {
        let actual = format!("{:?}", plan);
        assert_eq!(expects[i], actual);
    }
    Ok(())
}

#[test]
fn test_scheduler_plan_with_3_nodes() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::clusters::*;
    use crate::planners::*;

    let ctx = crate::tests::try_create_context()?;
    let cpus = ctx.get_max_threads()?;

    // Add node1 to cluster.
    ctx.try_get_cluster()?.add_node(&Node {
        id: "node1".to_string(),
        cpus: 4,
        address: "127.0.0.1:9001".to_string(),
    })?;

    // Add node2 to cluster.
    ctx.try_get_cluster()?.add_node(&Node {
        id: "node2".to_string(),
        cpus: 4,
        address: "127.0.0.1:9002".to_string(),
    })?;

    // Add node3 to cluster.
    ctx.try_get_cluster()?.add_node(&Node {
        id: "node3".to_string(),
        cpus: 4,
        address: "127.0.0.1:9003".to_string(),
    })?;

    // For more partitions generation.
    let ctx_more_cpu = crate::tests::try_create_context()?;
    ctx_more_cpu.set_max_threads(cpus * 40)?;
    let test_source = crate::tests::NumberTestData::create(ctx_more_cpu.clone());
    let source = test_source.number_read_source_plan_for_test(100000)?;

    let plan = PlanBuilder::from(ctx.clone(), &PlanNode::ReadSource(source))
        .filter(field("number").eq(constant(1i64)))?
        .project(vec![field("number")])?
        .build()?;

    let plans = PlanScheduler::schedule(ctx, &plan)?;
    let expects = vec!["Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan parts [106](Read from system.numbers_mt table, Read Rows:100000, Read Bytes:800000)",
"Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan parts [106](Read from system.numbers_mt table, Read Rows:100000, Read Bytes:800000)",
"Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan parts [106](Read from system.numbers_mt table, Read Rows:100000, Read Bytes:800000)",
"Projection: number:UInt64
  Filter: (number = 1)
    ReadDataSource: scan parts [2](Read from system.numbers_mt table, Read Rows:100000, Read Bytes:800000)",
    ];

    for (i, plan) in plans.iter().enumerate() {
        let actual = format!("{:?}", plan);
        assert_eq!(expects[i], actual);
    }
    Ok(())
}
