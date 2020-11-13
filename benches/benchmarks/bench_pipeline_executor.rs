// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::{stream::StreamExt, sync::Arc};
use criterion::{criterion_group, criterion_main, Criterion};

use fuse_engine::processors::Pipeline;
use fuse_engine::transforms::CountTransform;

async fn pipeline_count_executor() {
    let mut pipeline = Pipeline::create();

    for _i in 0..10 {
        let mut columns = vec![];
        for k in 0..100 {
            columns.push(k);
        }
        let a = fuse_engine::test::generate_source(vec![columns]);
        pipeline.add_source(Arc::new(a)).unwrap();
    }

    pipeline.merge_processor().unwrap();
    pipeline
        .add_simple_transform(|| Box::new(CountTransform::create()))
        .unwrap();
    pipeline.merge_processor().unwrap();

    let mut stream = pipeline.execute().await.unwrap();
    while let Some(_v) = stream.next().await {}
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("pipeline count executor bench", |b| {
        b.iter(|| async_std::task::block_on(pipeline_count_executor()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
