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

    let tests = vec![
        Test {
            name: "test-simple-transforms-pass",
            pipeline: || {
                use crate::transforms::CountTransform;

                let mut pipeline = Pipeline::create();

                let a =
                    crate::test::generate_source(vec![vec![14, 13, 12, 11], vec![11, 12, 13, 14]]);
                pipeline.add_source_processor(Arc::new(a)).unwrap();
                let b =
                    crate::test::generate_source(vec![vec![24, 23, 22, 21], vec![21, 22, 23, 24]]);
                pipeline.add_source_processor(Arc::new(b)).unwrap();

                pipeline
                    .add_simple_processor(|| Box::new(CountTransform::create()))
                    .unwrap();
                pipeline.merge_processor().unwrap();
                pipeline
            },
            result: "
  └─ Merge (CountTransform × 2) to (MergeTransform × 1)
    └─ CountTransform × 2
      └─ SourceTransform × 2",
        },
        Test {
            name: "test-transforms-pass",
            pipeline: || {
                use crate::transforms::CountTransform;

                let mut pipeline = Pipeline::create();

                let a =
                    crate::test::generate_source(vec![vec![14, 13, 12, 11], vec![11, 12, 13, 14]]);
                pipeline.add_source_processor(Arc::new(a)).unwrap();
                let b =
                    crate::test::generate_source(vec![vec![24, 23, 22, 21], vec![21, 22, 23, 24]]);
                pipeline.add_source_processor(Arc::new(b)).unwrap();

                pipeline.merge_processor().unwrap();
                pipeline.expand_processor(8).unwrap();
                pipeline
                    .add_simple_processor(|| Box::new(CountTransform::create()))
                    .unwrap();
                pipeline.merge_processor().unwrap();
                pipeline
            },
            result: "
  └─ Merge (CountTransform × 8) to (MergeTransform × 1)
    └─ CountTransform × 8
      └─ Expand (MergeTransform × 1) to (ThroughTransform × 8)
        └─ Merge (SourceTransform × 2) to (MergeTransform × 1)
          └─ SourceTransform × 2",
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

#[async_std::test]
async fn test_pipeline_executor() {
    use async_std::{stream::StreamExt, sync::Arc};
    use std::time::Instant;

    use crate::transforms::CountTransform;

    let mut pipeline = Pipeline::create();

    for _i in 0..10 {
        let mut columns = vec![];
        for k in 0..10000 {
            columns.push(k);
        }
        let a = crate::test::generate_source(vec![columns]);
        pipeline.add_source_processor(Arc::new(a)).unwrap();
    }

    pipeline.merge_processor().unwrap();
    pipeline
        .add_simple_processor(|| Box::new(CountTransform::create()))
        .unwrap();
    pipeline.merge_processor().unwrap();

    let start = Instant::now();
    let mut stream = pipeline.execute().await.unwrap();
    while let Some(v) = stream.next().await {
        println!("{:?}", v);
    }
    let duration = start.elapsed();
    println!("Time elapsed: {:?}", duration);
}
