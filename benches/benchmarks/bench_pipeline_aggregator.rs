// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use tokio::stream::StreamExt;

use fuse_query::datavalues::DataType;
use fuse_query::error::FuseQueryResult;
use fuse_query::functions::VariableFunction;
use fuse_query::planners::ExpressionPlan;
use fuse_query::processors::Pipeline;
use fuse_query::transforms::AggregatorTransform;

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

async fn pipeline_aggregator_executor(
    typ: TableType,
    transform: &str,
    parts: i64,
    expand: bool,
) -> FuseQueryResult<()> {
    let mut column = "c6";
    let mut pipeline = Pipeline::create();

    match typ {
        TableType::Memory => {
            let rows = 10000000;
            let step = rows / parts;
            let memory_source = fuse_query::testdata::MemoryTestData::create();
            for i in 0..parts {
                let mut columns = vec![];
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

    // Expand processor.
    if expand {
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
        column = transform;
    }

    // Merge the processor into one.
    pipeline.merge_processor()?;

    // Add one transform.
    pipeline.add_simple_transform(|| {
        Box::new(
            AggregatorTransform::create(
                transform,
                Arc::new(ExpressionPlan::Field(transform.to_string())),
                Arc::new(VariableFunction::create(column).unwrap()),
                &DataType::Int64,
            )
            .unwrap(),
        )
    })?;

    let mut stream = pipeline.execute().await?;
    while let Some(_v) = stream.next().await {}
    Ok(())
}

fn criterion_benchmark_suite(
    c: &mut Criterion,
    typ: TableType,
    suite: &str,
    parts: usize,
    expand: bool,
) {
    c.bench_function(
        format!(
            "Pipeline {} executor bench with {}-processor{}, {:?} Table",
            suite,
            parts,
            if expand {
                format!(" (with {}-expand)", parts)
            } else {
                "".to_string()
            },
            typ,
        )
        .as_str(),
        |b| {
            b.iter(|| {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(pipeline_aggregator_executor(
                        typ.clone(),
                        suite,
                        parts as i64,
                        expand,
                    ))
            })
        },
    );
}

fn criterion_benchmark_memory_table_processor(c: &mut Criterion) {
    criterion_benchmark_suite(c, TableType::Memory, "count", 1, false);
    criterion_benchmark_suite(c, TableType::Memory, "count", 4, false);
    criterion_benchmark_suite(c, TableType::Memory, "count", 4, true);

    criterion_benchmark_suite(c, TableType::Memory, "max", 1, false);
    criterion_benchmark_suite(c, TableType::Memory, "max", 4, false);
    criterion_benchmark_suite(c, TableType::Memory, "max", 4, true);
    criterion_benchmark_suite(c, TableType::Memory, "sum", 1, false);
    criterion_benchmark_suite(c, TableType::Memory, "sum", 4, false);
    criterion_benchmark_suite(c, TableType::Memory, "sum", 4, true);
}

fn criterion_benchmark_csv_table_processor(c: &mut Criterion) {
    criterion_benchmark_suite(c, TableType::Csv, "count", 1, false);
    criterion_benchmark_suite(c, TableType::Csv, "count", 4, false);
    criterion_benchmark_suite(c, TableType::Csv, "count", 4, true);

    criterion_benchmark_suite(c, TableType::Csv, "max", 1, false);
    criterion_benchmark_suite(c, TableType::Csv, "max", 4, false);
    criterion_benchmark_suite(c, TableType::Csv, "max", 4, true);
    criterion_benchmark_suite(c, TableType::Csv, "sum", 1, false);
    criterion_benchmark_suite(c, TableType::Csv, "sum", 4, false);
    criterion_benchmark_suite(c, TableType::Csv, "sum", 4, true);
}

criterion_group!(
    benches,
    criterion_benchmark_csv_table_processor,
    criterion_benchmark_memory_table_processor,
);
criterion_main!(benches);
