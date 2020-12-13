// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pipeline_builder() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::contexts::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::testdata;

    let test_source = testdata::NumberTestData::create();
    let ctx = Arc::new(FuseQueryContext::create_ctx(
        0,
        test_source.number_source_for_test()?,
    ));
    let plan = Planner::new().build_from_sql(
        ctx.clone(),
        "select sum(number+1)+2 as sumx from system.numbers_mt where (number+1)=4 limit 1",
    )?;
    let pipeline = PipelineBuilder::create(ctx, plan).build()?;
    let expect = "\
    \n  └─ LimitTransform × 1 processor\
    \n    └─ AggregateFinalTransform × 1 processor\
    \n      └─ Merge (AggregatePartialTransform × 8 processors) to (MergeProcessor × 1)\
    \n        └─ AggregatePartialTransform × 8 processors\
    \n          └─ FilterTransform × 8 processors\
    \n            └─ SourceTransform × 8 processors";
    let actual = format!("{:?}", pipeline);
    assert_eq!(expect, actual);
    Ok(())
}
