// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_pipeline_builder() {
    use std::sync::Arc;

    use crate::datavalues::*;
    use crate::functions::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::transforms::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        result: &'static str,
        pipeline: fn() -> Pipeline,
    }

    let tests = vec![
        Test {
            name: "testdata-simple-transforms-pass",
            pipeline: || {
                let mut pipeline = Pipeline::create();

                let a = crate::testdata::test_data_generate_source(vec![
                    vec![14, 13, 12, 11],
                    vec![11, 12, 13, 14],
                ]);
                pipeline.add_source(Arc::new(a)).unwrap();
                let b = crate::testdata::test_data_generate_source(vec![
                    vec![24, 23, 22, 21],
                    vec![21, 22, 23, 24],
                ]);
                pipeline.add_source(Arc::new(b)).unwrap();

                pipeline
                    .add_simple_transform(|| {
                        Box::new(
                            AggregatorTransform::create(
                                "count",
                                Arc::new(ExpressionPlan::Field("count".to_string())),
                                Arc::new(VariableFunction::create("a").unwrap()),
                                &DataType::UInt64,
                            )
                            .unwrap(),
                        )
                    })
                    .unwrap();
                pipeline.merge_processor().unwrap();
                pipeline
            },
            result: "
  └─ Merge (CountTransform × 2 processors) to (MergeProcessor × 1)
    └─ CountTransform × 2 processors
      └─ SourceTransform × 2 processors",
        },
        Test {
            name: "testdata-transforms-pass",
            pipeline: || {
                let mut pipeline = Pipeline::create();

                let a = crate::testdata::test_data_generate_source(vec![
                    vec![14, 13, 12, 11],
                    vec![11, 12, 13, 14],
                ]);
                pipeline.add_source(Arc::new(a)).unwrap();
                let b = crate::testdata::test_data_generate_source(vec![
                    vec![24, 23, 22, 21],
                    vec![21, 22, 23, 24],
                ]);
                pipeline.add_source(Arc::new(b)).unwrap();

                pipeline.merge_processor().unwrap();
                pipeline.expand_processor(8).unwrap();
                pipeline
                    .add_simple_transform(|| {
                        Box::new(
                            AggregatorTransform::create(
                                "count",
                                Arc::new(ExpressionPlan::Field("count".to_string())),
                                Arc::new(VariableFunction::create("a").unwrap()),
                                &DataType::UInt64,
                            )
                            .unwrap(),
                        )
                    })
                    .unwrap();
                pipeline.merge_processor().unwrap();
                pipeline
            },
            result: "
  └─ Merge (CountTransform × 8 processors) to (MergeProcessor × 1)
    └─ CountTransform × 8 processors
      └─ Expand (MergeProcessor × 1) to (ThroughProcessor × 8)
        └─ Merge (SourceTransform × 2 processors) to (MergeProcessor × 1)
          └─ SourceTransform × 2 processors",
        },
    ];

    for test in tests {
        let pipeline = (test.pipeline)();
        let actual = format!("{:?}", pipeline);
        let expect = test.result;

        println!("testdata name:{}", test.name);
        assert_eq!(expect, actual);
    }
}

//
// 4-ways parallel compute:
// source1 --> count processor -->  \
// source2 --> count processor -->
//                             -->   merge to one processor --> sum processor --> stream
// source3 --> count processor -->
// source4 --> count processor -->  /
//
#[async_std::test]
async fn test_pipeline_executor_sum() -> crate::error::Result<()> {
    use async_std::{stream::StreamExt, sync::Arc};

    use crate::datavalues::*;
    use crate::functions::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::transforms::*;

    let mut pipeline = Pipeline::create();

    for i in 0..4 {
        let mut columns = vec![];
        for k in 0..2500000 {
            columns.push(i * 2500000 + k);
        }
        let a = crate::testdata::test_data_generate_source(vec![columns]);
        pipeline.add_source(Arc::new(a))?;
    }

    pipeline
        .add_simple_transform(|| {
            Box::new(
                AggregatorTransform::create(
                    "sum",
                    Arc::new(ExpressionPlan::Field("sum".to_string())),
                    Arc::new(VariableFunction::create("a").unwrap()),
                    &DataType::Int64,
                )
                .unwrap(),
            )
        })
        .unwrap();

    pipeline.merge_processor().unwrap();

    pipeline.add_simple_transform(|| {
        Box::new(
            AggregatorTransform::create(
                "sum",
                Arc::new(ExpressionPlan::Field("sum".to_string())),
                Arc::new(VariableFunction::create("sum").unwrap()),
                &DataType::Int64,
            )
            .unwrap(),
        )
    })?;

    let mut stream = pipeline.execute().await?;
    let v = stream.next().await.unwrap()?;
    let actual = v.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let expect = &Int64Array::from(vec![49999995000000]);
    assert_eq!(expect.clone(), actual.clone());

    Ok(())
}

#[async_std::test]
async fn test_pipeline_executor_max() -> crate::error::Result<()> {
    use async_std::{stream::StreamExt, sync::Arc};

    use crate::datavalues::*;
    use crate::functions::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::transforms::*;

    let mut pipeline = Pipeline::create();

    for i in 0..4 {
        let mut columns = vec![];
        for k in 0..2500000 {
            columns.push(i * 2500000 + k);
        }
        let a = crate::testdata::test_data_generate_source(vec![columns]);
        pipeline.add_source(Arc::new(a))?;
    }

    pipeline
        .add_simple_transform(|| {
            Box::new(
                AggregatorTransform::create(
                    "max",
                    Arc::new(ExpressionPlan::Field("max".to_string())),
                    Arc::new(VariableFunction::create("a").unwrap()),
                    &DataType::Int64,
                )
                .unwrap(),
            )
        })
        .unwrap();

    pipeline.merge_processor().unwrap();

    pipeline.add_simple_transform(|| {
        Box::new(
            AggregatorTransform::create(
                "max",
                Arc::new(ExpressionPlan::Field("max".to_string())),
                Arc::new(VariableFunction::create("max").unwrap()),
                &DataType::Int64,
            )
            .unwrap(),
        )
    })?;

    println!("{:?}", pipeline);
    let mut stream = pipeline.execute().await?;
    let v = stream.next().await.unwrap()?;
    let actual = v.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let expect = &Int64Array::from(vec![9999999]);
    assert_eq!(expect.clone(), actual.clone());

    Ok(())
}
