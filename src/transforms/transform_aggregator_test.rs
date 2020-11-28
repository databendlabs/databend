// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

//
// 4-ways parallel compute:
// source1 --> count processor -->  \
// source2 --> count processor -->
//                             -->   merge to one processor --> sum processor --> stream
// source3 --> count processor -->
// source4 --> count processor -->  /
//
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_aggregate_sum() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::datavalues::*;
    use crate::functions::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::testdata;
    use crate::transforms::*;

    let test_source = testdata::MemoryTestData::create();
    let mut pipeline = Pipeline::create();

    for i in 0..4 {
        let mut columns = vec![];
        for k in 0..2500000 {
            columns.push(i * 2500000 + k);
        }
        let a = test_source.memory_table_source_transform_for_test(vec![columns])?;
        pipeline.add_source(Arc::new(a))?;
    }

    pipeline.add_simple_transform(|| {
        Ok(Box::new(AggregatorTransform::try_create(
            "sum",
            Arc::new(ExpressionPlan::Field("sum".to_string())),
            Box::new(VariableFunction::try_create("c6")?),
        )?))
    })?;

    pipeline.merge_processor()?;

    pipeline.add_simple_transform(|| {
        Ok(Box::new(AggregatorTransform::try_create(
            "sum",
            Arc::new(ExpressionPlan::Field("sum".to_string())),
            Box::new(VariableFunction::try_create("sum")?),
        )?))
    })?;

    let mut stream = pipeline.execute().await?;
    let v = stream.next().await.unwrap()?;
    let actual = v.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let expect = &Int64Array::from(vec![49999995000000]);
    assert_eq!(expect.clone(), actual.clone());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_aggregate_max() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::datavalues::*;
    use crate::functions::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::testdata;
    use crate::transforms::*;

    let test_source = testdata::MemoryTestData::create();
    let mut pipeline = Pipeline::create();

    for i in 0..4 {
        let mut columns = vec![];
        for k in 0..2500000 {
            columns.push(i * 2500000 + k);
        }
        let a = test_source.memory_table_source_transform_for_test(vec![columns])?;
        pipeline.add_source(Arc::new(a))?;
    }

    pipeline.add_simple_transform(|| {
        Ok(Box::new(AggregatorTransform::try_create(
            "max",
            Arc::new(ExpressionPlan::Field("max".to_string())),
            Box::new(VariableFunction::try_create("c6")?),
        )?))
    })?;

    pipeline.merge_processor()?;

    pipeline.add_simple_transform(|| {
        Ok(Box::new(AggregatorTransform::try_create(
            "max",
            Arc::new(ExpressionPlan::Field("max".to_string())),
            Box::new(VariableFunction::try_create("max")?),
        )?))
    })?;

    println!("{:?}", pipeline);
    let mut stream = pipeline.execute().await?;
    let v = stream.next().await.unwrap().unwrap();
    let actual = v.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let expect = &Int64Array::from(vec![9999999]);
    assert_eq!(expect.clone(), actual.clone());
    Ok(())
}
