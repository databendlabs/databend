// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::{stream::StreamExt, sync::Arc};
use criterion::{criterion_group, criterion_main, Criterion};

use fuse_engine::datavalues::DataType;
use fuse_engine::functions::VariableFunction;
use fuse_engine::planners::ExpressionPlan;
use fuse_engine::processors::Pipeline;
use fuse_engine::transforms::AggregatorTransform;

async fn pipeline_aggregator_executor(transform: &str, parts: i64, expand: bool) {
    let mut column = "a";
    let mut pipeline = Pipeline::create();

    for i in 0..parts {
        let mut columns = vec![];
        for k in 0..1000000 {
            columns.push(i * 1000000 + k);
        }
        let a = fuse_engine::testdata::test_data_generate_source(vec![columns]);

        // Add source pipe.
        pipeline.add_source(Arc::new(a)).unwrap();
    }

    // Expand processor.
    if expand {
        pipeline
            .add_simple_transform(|| {
                Box::new(
                    AggregatorTransform::create(
                        transform,
                        Arc::new(ExpressionPlan::Field(transform.to_string())),
                        Arc::new(VariableFunction::create("a").unwrap()),
                        &DataType::Int64,
                    )
                    .unwrap(),
                )
            })
            .unwrap();
        column = transform;
    }

    // Merge the processor into one.
    pipeline.merge_processor().unwrap();

    // Add one transform.
    pipeline
        .add_simple_transform(|| {
            Box::new(
                AggregatorTransform::create(
                    transform,
                    Arc::new(ExpressionPlan::Field(transform.to_string())),
                    Arc::new(VariableFunction::create(column).unwrap()),
                    &DataType::Int64,
                )
                .unwrap(),
            )
        })
        .unwrap();

    let mut stream = pipeline.execute().await.unwrap();
    while let Some(_v) = stream.next().await {}
}

fn criterion_benchmark_count_no_expand(c: &mut Criterion) {
    c.bench_function("pipeline count executor bench with 1-processor", |b| {
        b.iter(|| async_std::task::block_on(pipeline_aggregator_executor("count", 4, false)))
    });
}

fn criterion_benchmark_count_with_expand(c: &mut Criterion) {
    c.bench_function(
        "pipeline count executor bench with 4-processors expand",
        |b| b.iter(|| async_std::task::block_on(pipeline_aggregator_executor("count", 4, true))),
    );
}

fn criterion_benchmark_min_no_expand(c: &mut Criterion) {
    c.bench_function("pipeline min executor bench with 1-processor", |b| {
        b.iter(|| async_std::task::block_on(pipeline_aggregator_executor("min", 4, false)))
    });
}

fn criterion_benchmark_min_with_expand(c: &mut Criterion) {
    c.bench_function(
        "pipeline min executor bench with 4-processors expand",
        |b| b.iter(|| async_std::task::block_on(pipeline_aggregator_executor("min", 4, true))),
    );
}

fn criterion_benchmark_max_no_expand(c: &mut Criterion) {
    c.bench_function("pipeline max executor bench with 1-processor", |b| {
        b.iter(|| async_std::task::block_on(pipeline_aggregator_executor("max", 4, false)))
    });
}

fn criterion_benchmark_max_with_expand(c: &mut Criterion) {
    c.bench_function(
        "pipeline max executor bench with 4-processors expand",
        |b| b.iter(|| async_std::task::block_on(pipeline_aggregator_executor("max", 4, true))),
    );
}

fn criterion_benchmark_sum_no_expand(c: &mut Criterion) {
    c.bench_function("pipeline sum executor bench with 1-processor", |b| {
        b.iter(|| async_std::task::block_on(pipeline_aggregator_executor("sum", 4, false)))
    });
}

fn criterion_benchmark_sum_with_expand(c: &mut Criterion) {
    c.bench_function(
        "pipeline sum executor bench with 4-processors expand",
        |b| b.iter(|| async_std::task::block_on(pipeline_aggregator_executor("sum", 4, true))),
    );
}

criterion_group!(
    benches,
    criterion_benchmark_count_no_expand,
    criterion_benchmark_count_with_expand,
    criterion_benchmark_min_no_expand,
    criterion_benchmark_min_with_expand,
    criterion_benchmark_max_no_expand,
    criterion_benchmark_max_with_expand,
    criterion_benchmark_sum_no_expand,
    criterion_benchmark_sum_with_expand,
);
criterion_main!(benches);
