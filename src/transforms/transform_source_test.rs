// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn transform_source_test() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::functions::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::testdata;
    use crate::transforms::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        result: &'static str,
        pipeline: fn() -> crate::error::FuseQueryResult<Pipeline>,
    }

    let tests = vec![
        Test {
            name: "testdata-simple-transforms-pass",
            pipeline: || {
                Ok({
                    let test_source = testdata::MemoryTestData::create();
                    let mut pipeline = Pipeline::create();

                    let a = test_source.memory_table_source_transform_for_test(vec![
                        vec![14, 13, 12, 11],
                        vec![11, 12, 13, 14],
                    ])?;
                    pipeline.add_source(Arc::new(a))?;

                    let b = test_source.memory_table_source_transform_for_test(vec![
                        vec![24, 23, 22, 21],
                        vec![21, 22, 23, 24],
                    ])?;
                    pipeline.add_source(Arc::new(b))?;

                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(AggregatorTransform::try_create(
                            "count",
                            Arc::new(ExpressionPlan::Field("count".to_string())),
                            Box::new(VariableFunction::try_create("a")?),
                        )?))
                    })?;
                    pipeline.merge_processor()?;
                    pipeline
                })
            },
            result: "
  └─ Merge (CountTransform × 2 processors) to (MergeProcessor × 1)
    └─ CountTransform × 2 processors
      └─ SourceTransform × 2 processors",
        },
        Test {
            name: "testdata-transforms-pass",
            pipeline: || {
                Ok({
                    let test_source = testdata::MemoryTestData::create();
                    let mut pipeline = Pipeline::create();

                    let a = test_source.memory_table_source_transform_for_test(vec![
                        vec![14, 13, 12, 11],
                        vec![11, 12, 13, 14],
                    ])?;
                    pipeline.add_source(Arc::new(a))?;

                    let b = test_source.memory_table_source_transform_for_test(vec![
                        vec![24, 23, 22, 21],
                        vec![21, 22, 23, 24],
                    ])?;
                    pipeline.add_source(Arc::new(b))?;

                    pipeline.merge_processor()?;
                    pipeline.expand_processor(8)?;
                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(AggregatorTransform::try_create(
                            "count",
                            Arc::new(ExpressionPlan::Field("count".to_string())),
                            Box::new(VariableFunction::try_create("a")?),
                        )?))
                    })?;
                    pipeline.merge_processor()?;
                    pipeline
                })
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
        let pipeline = (test.pipeline)()?;
        let actual = format!("{:?}", pipeline);
        let expect = test.result;

        println!("testdata name:{}", test.name);
        assert_eq!(expect, actual);
    }
    Ok(())
}
