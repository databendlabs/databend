// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::processors::Pipeline;

#[allow(dead_code)]
struct Test {
    name: &'static str,
    result: &'static str,
    pipeline: fn() -> Pipeline,
}

#[test]
fn test_pipeline_builder() {
    use std::sync::Arc;

    use crate::datavalues::DataType;
    use crate::functions::VariableFunction;
    use crate::planners::ExpressionPlan;
    use crate::transforms::AggregatorTransform;

    let tests = vec![
        Test {
            name: "test-simple-transforms-pass",
            pipeline: || {
                let mut pipeline = Pipeline::create();

                let a =
                    crate::test::generate_source(vec![vec![14, 13, 12, 11], vec![11, 12, 13, 14]]);
                pipeline.add_source(Arc::new(a)).unwrap();
                let b =
                    crate::test::generate_source(vec![vec![24, 23, 22, 21], vec![21, 22, 23, 24]]);
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
            name: "test-transforms-pass",
            pipeline: || {
                let mut pipeline = Pipeline::create();

                let a =
                    crate::test::generate_source(vec![vec![14, 13, 12, 11], vec![11, 12, 13, 14]]);
                pipeline.add_source(Arc::new(a)).unwrap();
                let b =
                    crate::test::generate_source(vec![vec![24, 23, 22, 21], vec![21, 22, 23, 24]]);
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

        println!("test name:{}", test.name);
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
async fn test_pipeline_executor_sum() {
    use async_std::{stream::StreamExt, sync::Arc};
    use std::time::Instant;

    use crate::datavalues::DataType;
    use crate::functions::VariableFunction;
    use crate::planners::ExpressionPlan;
    use crate::transforms::AggregatorTransform;

    let mut pipeline = Pipeline::create();

    for i in 0..4 {
        let mut columns = vec![];
        for k in 0..2500000 {
            columns.push(i * 2500000 + k);
        }
        let a = crate::test::generate_source(vec![columns]);
        pipeline.add_source(Arc::new(a)).unwrap();
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

    pipeline
        .add_simple_transform(|| {
            Box::new(
                AggregatorTransform::create(
                    "sum",
                    Arc::new(ExpressionPlan::Field("sum".to_string())),
                    Arc::new(VariableFunction::create("sum").unwrap()),
                    &DataType::Int64,
                )
                .unwrap(),
            )
        })
        .unwrap();

    println!("{:?}", pipeline);
    let start = Instant::now();
    let mut stream = pipeline.execute().await.unwrap();
    while let Some(v) = stream.next().await {
        println!("{:?}", v);
    }
    let duration = start.elapsed();
    println!("Time elapsed: {:?}", duration);
}

#[async_std::test]
async fn test_pipeline_executor_max() {
    use async_std::{stream::StreamExt, sync::Arc};
    use std::time::Instant;

    use crate::datavalues::DataType;
    use crate::functions::VariableFunction;
    use crate::planners::ExpressionPlan;
    use crate::transforms::AggregatorTransform;

    let mut pipeline = Pipeline::create();

    for i in 0..4 {
        let mut columns = vec![];
        for k in 0..2500000 {
            columns.push(i * 2500000 + k);
        }
        let a = crate::test::generate_source(vec![columns]);
        pipeline.add_source(Arc::new(a)).unwrap();
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

    pipeline
        .add_simple_transform(|| {
            Box::new(
                AggregatorTransform::create(
                    "max",
                    Arc::new(ExpressionPlan::Field("max".to_string())),
                    Arc::new(VariableFunction::create("max").unwrap()),
                    &DataType::Int64,
                )
                .unwrap(),
            )
        })
        .unwrap();

    println!("{:?}", pipeline);
    let start = Instant::now();
    let mut stream = pipeline.execute().await.unwrap();
    while let Some(v) = stream.next().await {
        println!("{:?}", v);
    }
    let duration = start.elapsed();
    println!("Time elapsed: {:?}", duration);
}
