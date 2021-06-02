// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::mem::size_of;
use std::sync::Arc;

use common_datavalues::*;
use common_planners::*;
use pretty_assertions::assert_eq;

use crate::optimizers::optimizer_test::*;
use crate::optimizers::*;
use crate::sql::*;

#[test]
fn test_projection_push_down_optimizer_1() -> anyhow::Result<()> {
    let ctx = crate::tests::try_create_context()?;

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Utf8, false),
        DataField::new("b", DataType::Utf8, false),
        DataField::new("c", DataType::Utf8, false),
        DataField::new("d", DataType::Utf8, false),
    ]);

    let output_schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Utf8, false),
        DataField::new("b", DataType::Utf8, false),
        DataField::new("c", DataType::Utf8, false),
    ]);

    let plan = PlanNode::Projection(ProjectionPlan {
        expr: vec![col("a"), col("b"), col("c")],
        schema: output_schema,
        input: Arc::from(PlanBuilder::from(&PlanNode::Empty(EmptyPlan { schema })).build()?),
    });

    let mut projection_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = projection_push_down.optimize(&plan)?;

    let expect = "\
        Projection: a:Utf8, b:Utf8, c:Utf8";

    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}

#[test]
fn test_projection_push_down_optimizer_group_by() -> anyhow::Result<()> {
    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone())
        .build_from_sql("select max(value) as c1, name as c2 from system.settings group by c2")?;

    let mut project_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = project_push_down.optimize(&plan)?;

    let expect = "\
        Projection: max(value) as c1:Utf8, name as c2:Utf8\
        \n  AggregatorFinal: groupBy=[[name]], aggr=[[max(value)]]\
        \n    AggregatorPartial: groupBy=[[name]], aggr=[[max(value)]]\
        \n      ReadDataSource: scan partitions: [1], scan schema: [name:Utf8, value:Utf8], statistics: [read_rows: 0, read_bytes: 0]";

    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);
    Ok(())
}

#[test]
fn test_projection_push_down_optimizer_2() -> anyhow::Result<()> {
    let ctx = crate::tests::try_create_context()?;

    let total = ctx.get_max_block_size()? as u64;
    let statistics = Statistics {
        read_rows: total as usize,
        read_bytes: ((total) * size_of::<u64>() as u64) as usize,
    };
    ctx.try_set_statistics(&statistics)?;
    let source_plan = PlanNode::ReadSource(ReadDataSourcePlan {
        db: "system".to_string(),
        table: "test".to_string(),
        schema: DataSchemaRefExt::create(vec![
            DataField::new("a", DataType::Utf8, false),
            DataField::new("b", DataType::Utf8, false),
            DataField::new("c", DataType::Utf8, false),
        ]),
        partitions: generate_partitions(8, total as u64),
        statistics: statistics.clone(),
        description: format!(
            "(Read from system.{} table, Read Rows:{}, Read Bytes:{})",
            "test".to_string(),
            statistics.read_rows,
            statistics.read_bytes
        ),
        scan_plan: Arc::new(ScanPlan::empty()),
        remote: false,
    });

    let filter_plan = PlanBuilder::from(&source_plan)
        .filter(col("a").gt(lit(6)).and(col("b").lt_eq(lit(10))))?
        .build()?;

    let plan = PlanNode::Projection(ProjectionPlan {
        expr: vec![col("a")],
        schema: DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]),
        input: Arc::from(filter_plan),
    });

    let mut projection_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = projection_push_down.optimize(&plan)?;

    let expect = "\
        Projection: a:Utf8\
        \n  Filter: ((a > 6) and (b <= 10))\
        \n    ReadDataSource: scan partitions: [8], scan schema: [a:Utf8, b:Utf8], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}

#[test]
fn test_projection_push_down_optimizer_3() -> anyhow::Result<()> {
    let ctx = crate::tests::try_create_context()?;

    let total = ctx.get_max_block_size()? as u64;
    let statistics = Statistics {
        read_rows: total as usize,
        read_bytes: ((total) * size_of::<u64>() as u64) as usize,
    };
    ctx.try_set_statistics(&statistics)?;
    let source_plan = PlanNode::ReadSource(ReadDataSourcePlan {
        db: "system".to_string(),
        table: "test".to_string(),
        schema: DataSchemaRefExt::create(vec![
            DataField::new("a", DataType::Utf8, false),
            DataField::new("b", DataType::Utf8, false),
            DataField::new("c", DataType::Utf8, false),
            DataField::new("d", DataType::Utf8, false),
            DataField::new("e", DataType::Utf8, false),
            DataField::new("f", DataType::Utf8, false),
            DataField::new("g", DataType::Utf8, false),
        ]),
        partitions: generate_partitions(8, total as u64),
        statistics: statistics.clone(),
        description: format!(
            "(Read from system.{} table, Read Rows:{}, Read Bytes:{})",
            "test".to_string(),
            statistics.read_rows,
            statistics.read_bytes
        ),
        scan_plan: Arc::new(ScanPlan::empty()),
        remote: false,
    });

    let group_exprs = &[col("a"), col("c")];

    // SELECT a FROM table WHERE b = 10 GROUP BY a, c having a < 10 order by c LIMIT 10;
    let plan = PlanBuilder::from(&source_plan)
        .filter(col("b").eq(lit(10)))?
        .aggregate_partial(&[], group_exprs)?
        .aggregate_final(source_plan.schema(), &[], group_exprs)?
        .having(col("a").lt(lit(10)))?
        .sort(&[col("c")])?
        .limit(10)?
        .project(&[col("a")])?
        .build()?;

    let mut projection_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = projection_push_down.optimize(&plan)?;

    let expect = "\
    Projection: a:Utf8\
    \n  Limit: 10\
    \n    Sort: c:Utf8\
    \n      Having: (a < 10)\
    \n        AggregatorFinal: groupBy=[[a, c]], aggr=[[]]\
    \n          AggregatorPartial: groupBy=[[a, c]], aggr=[[]]\
    \n            Filter: (b = 10)\
    \n              ReadDataSource: scan partitions: [8], scan schema: [a:Utf8, b:Utf8, c:Utf8], statistics: [read_rows: 10000, read_bytes: 80000]";

    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}
