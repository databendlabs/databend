// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_filter() {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::datavalues::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::testdata;
    use crate::transforms::*;

    let test_source = testdata::MemoryTestData::create();
    let mut pipeline = Pipeline::create();

    let mut columns = vec![];
    for k in 0..2 {
        columns.push(k);
    }
    let a = test_source.memory_table_source_transform_for_test(vec![columns]);
    pipeline.add_source(Arc::new(a)).unwrap();

    pipeline
        .add_simple_transform(|| {
            Box::new(FilterTransform::create(ExpressionPlan::BinaryExpression {
                left: Box::from(ExpressionPlan::Field("c6".to_string())),
                op: "=".to_string(),
                right: Box::from(ExpressionPlan::Constant(DataValue::Int64(Some(1i64)))),
            }))
        })
        .unwrap();

    println!("{:?}", pipeline);
    let mut stream = pipeline.execute().await.unwrap();
    let v = stream.next().await.unwrap().unwrap();
    let actual = v.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let expect = &Int64Array::from(vec![1]);
    assert_eq!(expect.clone(), actual.clone());
}
