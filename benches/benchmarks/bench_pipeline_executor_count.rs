// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::{stream::StreamExt, sync::Arc};
use criterion::{criterion_group, criterion_main, Criterion};

use fuse_engine::datavalues::DataType;
use fuse_engine::functions::VariableFunction;
use fuse_engine::planners::ExpressionPlan;
use fuse_engine::processors::Pipeline;
use fuse_engine::transforms::CountTransform;

async fn pipeline_count_executor(expand: bool, parts: i64) {
    let mut column = "a";
    let mut pipeline = Pipeline::create();

    for i in 0..parts {
        let mut columns = vec![];
        for k in 0..250000 {
            columns.push(i * 250000 + k);
        }
        let a = fuse_engine::test::generate_source(vec![columns]);

        // Add source pipe.
        pipeline.add_source(Arc::new(a)).unwrap();
    }

    // Expand processor.
    if expand {
        pipeline
            .add_simple_transform(|| {
                Box::new(CountTransform::create(
                    Arc::new(ExpressionPlan::Field("count".to_string())),
                    Arc::new(VariableFunction::create("a").unwrap()),
                    &DataType::UInt64,
                ))
            })
            .unwrap();
        column = "count";
    }

    // Merge the processor into one.
    pipeline.merge_processor().unwrap();

    // Add one transform.
    pipeline
        .add_simple_transform(|| {
            Box::new(CountTransform::create(
                Arc::new(ExpressionPlan::Field("count".to_string())),
                Arc::new(VariableFunction::create(column).unwrap()),
                &DataType::UInt64,
            ))
        })
        .unwrap();

    let mut stream = pipeline.execute().await.unwrap();
    while let Some(_v) = stream.next().await {}
}

fn criterion_benchmark_no_expand(c: &mut Criterion) {
    c.bench_function("pipeline count executor bench with 1-processor", |b| {
        b.iter(|| async_std::task::block_on(pipeline_count_executor(false, 4)))
    });
}

fn criterion_benchmark_with_expand(c: &mut Criterion) {
    c.bench_function(
        "pipeline count executor bench with 4-processors expand",
        |b| b.iter(|| async_std::task::block_on(pipeline_count_executor(true, 4))),
    );
}

//
// pipeline count executor bench with 1-processor
//                         time:   [44.490 ms 44.897 ms 45.302 ms]
//                         change: [-32.983% -30.670% -28.360%] (p = 0.00 < 0.05)
//                         Performance has improved.
//
// pipeline count executor bench with 4-processors expand
//                         time:   [46.239 ms 46.428 ms 46.635 ms]
// Found 14 outliers among 100 measurements (14.00%)
//   3 (3.00%) low severe
//   8 (8.00%) high mild
//   3 (3.00%) high severe
criterion_group!(
    benches,
    criterion_benchmark_no_expand,
    criterion_benchmark_with_expand
);
criterion_main!(benches);
