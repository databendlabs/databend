// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::{stream::StreamExt, sync::Arc};
use criterion::{criterion_group, criterion_main, Criterion};

use fuse_engine::datavalues::DataType;
use fuse_engine::functions::VariableFunction;
use fuse_engine::planners::ExpressionPlan;
use fuse_engine::processors::Pipeline;
use fuse_engine::transforms::SumTransform;

async fn pipeline_sum_executor(expand: bool, parts: i64) {
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
                Box::new(SumTransform::create(
                    Arc::new(ExpressionPlan::Field("sum".to_string())),
                    Arc::new(VariableFunction::create("a").unwrap()),
                    &DataType::Int64,
                ))
            })
            .unwrap();
        column = "sum";
    }

    // Merge the processor into one.
    pipeline.merge_processor().unwrap();

    // Add one transform.
    pipeline
        .add_simple_transform(|| {
            Box::new(SumTransform::create(
                Arc::new(ExpressionPlan::Field("sum".to_string())),
                Arc::new(VariableFunction::create(column).unwrap()),
                &DataType::Int64,
            ))
        })
        .unwrap();

    let mut stream = pipeline.execute().await.unwrap();
    while let Some(_v) = stream.next().await {}
}

fn criterion_benchmark_no_expand(c: &mut Criterion) {
    c.bench_function("pipeline sum executor bench with 1-processor", |b| {
        b.iter(|| async_std::task::block_on(pipeline_sum_executor(false, 4)))
    });
}

fn criterion_benchmark_with_expand(c: &mut Criterion) {
    c.bench_function(
        "pipeline sum executor bench with 4-processors expand",
        |b| b.iter(|| async_std::task::block_on(pipeline_sum_executor(true, 4))),
    );
}

//
// pipeline sum executor bench with 1-processor
//                         time:   [47.994 ms 48.370 ms 48.742 ms]
//
// Benchmarking pipeline sum executor bench with 4-processors expand: Warming up for 3.0000 s
// Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 5.4s, or reduce sample count to 90.
// pipeline sum executor bench with 4-processors expand
//                         time:   [51.668 ms 52.535 ms 53.379 ms]
// Found 3 outliers among 100 measurements (3.00%)
//   3 (3.00%) low mild
criterion_group!(
    benches,
    criterion_benchmark_no_expand,
    criterion_benchmark_with_expand
);
criterion_main!(benches);
