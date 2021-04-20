// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_projection_push_down_optimizer_1() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::optimizers::*;

    let ctx = crate::tests::try_create_context()?;

    let plan = PlanNode::Projection(ProjectionPlan {
        expr: vec![col("a"), col("b"), col("c")],
        schema: Arc::new(DataSchema::new(vec![
            DataField::new("a", DataType::Utf8, false),
            DataField::new("b", DataType::Utf8, false),
            DataField::new("c", DataType::Utf8, false),
        ])),
        input: Arc::from(PlanBuilder::empty().build()?),
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
fn test_projection_push_down_optimizer_2() -> anyhow::Result<()> {
    use std::mem::size_of;
    use std::sync::Arc;

    use common_datavalues::*;
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::optimizers::*;

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
        schema: Arc::new(DataSchema::new(vec![
            DataField::new("a", DataType::Utf8, false),
            DataField::new("b", DataType::Utf8, false),
            DataField::new("c", DataType::Utf8, false),
        ])),
        partitions: Test::generate_partitions(8, total as u64),
        statistics: statistics.clone(),
        description: format!(
            "(Read from system.{} table, Read Rows:{}, Read Bytes:{})",
            "test".to_string(),
            statistics.read_rows,
            statistics.read_bytes
        ),
    });

    let filter_plan = PlanBuilder::from(&source_plan)
        .filter(col("a").gt(lit(6)).and(col("b").lt_eq(lit(10))))?
        .build()?;

    let plan = PlanNode::Projection(ProjectionPlan {
        expr: vec![col("a"), col("c")],
        schema: Arc::new(DataSchema::new(vec![DataField::new(
            "a",
            DataType::Utf8,
            false,
        )])),
        input: Arc::from(filter_plan),
    });

    let mut projection_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = projection_push_down.optimize(&plan)?;

    let expect = "\
    Projection: a:Utf8\
    \n  Filter: ((a > 6) and (b <= 10))\
    \n    ReadDataSource: scan partitions: [8], scan schema: [a:Utf8], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}

#[test]
fn test_projection_push_down_optimizer_3() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::optimizers::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "select (number+1) as c1, number as c2, (number*2) as c3 from numbers_mt(10000) where (c1+c3+1)=1",
    )?;

    let mut projection_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = projection_push_down.optimize(&plan)?;
    let expect = "\
    Projection: (number + 1) as c1:UInt64, number as c2:UInt64, (number * 2) as c3:UInt64\
    \n  Filter: (((c1 + c3) + 1) = 1)\
    \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}
