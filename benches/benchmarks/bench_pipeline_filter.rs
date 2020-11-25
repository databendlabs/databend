// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use tokio::stream::StreamExt;

use fuse_query::datavalues::DataValue;
use fuse_query::error::FuseQueryResult;
use fuse_query::planners::ExpressionPlan;
use fuse_query::processors::Pipeline;
use fuse_query::transforms::FilterTransform;

#[derive(Clone)]
enum TableType {
    Memory,
    Csv,
}

impl std::fmt::Debug for TableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableType::Memory => write!(f, "Memory"),
            TableType::Csv => write!(f, "Csv"),
        }
    }
}

async fn pipeline_filter_executor(typ: TableType, parts: i64) -> FuseQueryResult<()> {
    let mut pipeline = Pipeline::create();

    match typ {
        TableType::Memory => {
            let rows = 1000000;
            let step = rows / parts;
            let memory_source = fuse_query::testdata::MemoryTestData::create();
            for i in 0..parts {
                let mut columns = Vec::with_capacity(step as usize);
                for k in 0..step {
                    columns.push(i * step + k);
                }
                let source = memory_source.memory_table_source_transform_for_test(vec![columns]);
                pipeline.add_source(Arc::new(source))?;
            }
        }
        TableType::Csv => {
            let csv_source = fuse_query::testdata::CsvTestData::create();
            let source = csv_source.csv_table_source_transform_for_test();
            pipeline.add_source(Arc::new(source))?;
        }
    }

    // Add one transform.
    pipeline.add_simple_transform(|| {
        Box::new(FilterTransform::create(ExpressionPlan::BinaryExpression {
            left: Box::from(ExpressionPlan::Field("c6".to_string())),
            op: "=".to_string(),
            right: Box::from(ExpressionPlan::Constant(DataValue::Int64(Some(10000i64)))),
        }))
    })?;

    pipeline.merge_processor()?;
    let mut stream = pipeline.execute().await?;
    while let Some(_v) = stream.next().await {}
    Ok(())
}

fn criterion_benchmark_suite(c: &mut Criterion, typ: TableType, parts: usize) {
    c.bench_function(
        format!(
            "Pipeline executor bench with {}-processor, {:?} Table",
            parts, typ,
        )
        .as_str(),
        |b| {
            b.iter(|| {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(pipeline_filter_executor(typ.clone(), parts as i64))
            })
        },
    );
}

fn criterion_benchmark_memory_table_processor(c: &mut Criterion) {
    criterion_benchmark_suite(c, TableType::Memory, 1);
    criterion_benchmark_suite(c, TableType::Memory, 4);
}

fn criterion_benchmark_csv_table_processor(c: &mut Criterion) {
    criterion_benchmark_suite(c, TableType::Csv, 1);
    criterion_benchmark_suite(c, TableType::Csv, 4);
}

criterion_group!(
    benches,
    criterion_benchmark_csv_table_processor,
    criterion_benchmark_memory_table_processor,
);
criterion_main!(benches);
