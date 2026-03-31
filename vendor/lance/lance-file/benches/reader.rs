// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
use std::sync::{Arc, Mutex};

use arrow_array::{cast::AsArray, types::Int32Type, UInt32Array};
use arrow_schema::DataType;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures::{FutureExt, StreamExt};
use lance_datagen::ArrayGeneratorExt;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::{
    reader::{FileReader, FileReaderOptions},
    testing::test_cache,
    version::LanceFileVersion,
    writer::{FileWriter, FileWriterOptions},
};
use lance_io::{
    object_store::ObjectStore,
    scheduler::{ScanScheduler, SchedulerConfig},
    utils::CachedFileSize,
};
use rand::seq::SliceRandom;

fn bench_reader(c: &mut Criterion) {
    for version in [LanceFileVersion::V2_0, LanceFileVersion::V2_1] {
        let mut group = c.benchmark_group(format!("reader_{}", version));
        let data = lance_datagen::gen_batch()
            .anon_col(lance_datagen::array::rand_type(&DataType::Int32))
            .into_batch_rows(lance_datagen::RowCount::from(2 * 1024 * 1024))
            .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let test_path = lance_core::utils::tempfile::TempStdFile::default();
        let (object_store, base_path) = rt
            .block_on(ObjectStore::from_uri(
                test_path.as_os_str().to_str().unwrap(),
            ))
            .unwrap();

        let file_path = base_path.child("foo.lance");
        let object_writer = rt.block_on(object_store.create(&file_path)).unwrap();

        let mut writer = FileWriter::try_new(
            object_writer,
            data.schema().as_ref().try_into().unwrap(),
            FileWriterOptions {
                format_version: Some(version),
                ..Default::default()
            },
        )
        .unwrap();
        rt.block_on(writer.write_batch(&data)).unwrap();
        rt.block_on(writer.finish()).unwrap();
        group.throughput(criterion::Throughput::Bytes(
            data.get_array_memory_size() as u64
        ));
        group.bench_function("decode", |b| {
            b.iter(|| {
                let object_store = &object_store;
                let file_path = &file_path;
                let data = &data;
                rt.block_on(async move {
                    let store_scheduler = ScanScheduler::new(
                        object_store.clone(),
                        SchedulerConfig::default_for_testing(),
                    );
                    let scheduler = store_scheduler
                        .open_file(file_path, &CachedFileSize::unknown())
                        .await
                        .unwrap();
                    let reader = FileReader::try_open(
                        scheduler.clone(),
                        None,
                        Arc::<DecoderPlugins>::default(),
                        &test_cache(),
                        FileReaderOptions::default(),
                    )
                    .await
                    .unwrap();
                    let stream = reader
                        .read_tasks(
                            lance_io::ReadBatchParams::RangeFull,
                            16 * 1024,
                            None,
                            FilterExpression::no_filter(),
                        )
                        .unwrap();
                    let stats = Arc::new(Mutex::new((0, 0)));
                    let mut stream = stream
                        .map(|batch_task| {
                            let stats = stats.clone();
                            async move {
                                let batch = batch_task.task.await.unwrap();
                                let row_count = batch.num_rows();
                                let sum = batch
                                    .column(0)
                                    .as_primitive::<Int32Type>()
                                    .values()
                                    .iter()
                                    .map(|v| *v as i64)
                                    .sum::<i64>();
                                let mut stats = stats.lock().unwrap();
                                stats.0 += row_count;
                                stats.1 += sum;
                            }
                            .boxed()
                        })
                        .buffer_unordered(16);
                    while (stream.next().await).is_some() {}
                    let stats = stats.lock().unwrap();
                    let row_count = stats.0;
                    let sum = stats.1;
                    assert_eq!(data.num_rows(), row_count);
                    black_box(sum);
                });
            })
        });
    }
}

fn bench_random_access(c: &mut Criterion) {
    const TAKE_SIZE: usize = 100;
    for version in [LanceFileVersion::V2_0, LanceFileVersion::V2_1] {
        let mut group = c.benchmark_group(format!("reader_{}", version));
        let data = lance_datagen::gen_batch()
            .anon_col(lance_datagen::array::rand_type(&DataType::Int32).with_random_nulls(0.1))
            .into_batch_rows(lance_datagen::RowCount::from(2 * 1024 * 1024))
            .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let test_path = lance_core::utils::tempfile::TempStdFile::default();
        let (object_store, base_path) = rt
            .block_on(ObjectStore::from_uri(
                test_path.as_os_str().to_str().unwrap(),
            ))
            .unwrap();
        let file_path = base_path.child("foo.lance");
        let object_writer = rt.block_on(object_store.create(&file_path)).unwrap();

        let mut writer = FileWriter::try_new(
            object_writer,
            data.schema().as_ref().try_into().unwrap(),
            FileWriterOptions {
                format_version: Some(version),
                ..Default::default()
            },
        )
        .unwrap();
        rt.block_on(writer.write_batch(&data)).unwrap();
        rt.block_on(writer.finish()).unwrap();

        let mut indices = (0..data.num_rows() as u32).collect::<Vec<_>>();
        indices.partial_shuffle(&mut rand::rng(), TAKE_SIZE);
        indices.truncate(TAKE_SIZE);
        let indices: UInt32Array = indices.into();

        let object_store = &object_store;
        let file_path = &file_path;
        let reader = rt.block_on(async move {
            let store_scheduler =
                ScanScheduler::new(object_store.clone(), SchedulerConfig::default_for_testing());
            let scheduler = store_scheduler
                .open_file(file_path, &CachedFileSize::unknown())
                .await
                .unwrap();
            Arc::new(
                FileReader::try_open(
                    scheduler.clone(),
                    None,
                    Arc::<DecoderPlugins>::default(),
                    &test_cache(),
                    FileReaderOptions::default(),
                )
                .await
                .unwrap(),
            )
        });

        group.throughput(criterion::Throughput::Elements(TAKE_SIZE as u64));
        group.bench_function("take", |b| {
            let reader = reader.clone();
            let indices = indices.clone();
            b.iter(|| {
                let reader = reader.clone();
                let indices = indices.clone();
                rt.block_on(async move {
                    let stream = reader
                        .read_tasks(
                            lance_io::ReadBatchParams::Indices(indices),
                            TAKE_SIZE as u32,
                            None,
                            FilterExpression::no_filter(),
                        )
                        .unwrap();
                    let stats = Arc::new(Mutex::new((0, 0)));
                    let mut stream = stream
                        .map(|batch_task| {
                            let stats = stats.clone();
                            async move {
                                let batch = batch_task.task.await.unwrap();
                                let row_count = batch.num_rows();
                                let sum = batch
                                    .column(0)
                                    .as_primitive::<Int32Type>()
                                    .values()
                                    .iter()
                                    .map(|v| *v as i64)
                                    .sum::<i64>();
                                let mut stats = stats.lock().unwrap();
                                stats.0 += row_count;
                                stats.1 += sum;
                            }
                            .boxed()
                        })
                        .buffer_unordered(16);
                    while (stream.next().await).is_some() {}
                    let stats = stats.lock().unwrap();
                    let row_count = stats.0;
                    let sum = stats.1;
                    assert_eq!(TAKE_SIZE, row_count);
                    black_box(sum);
                });
            })
        });
    }
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(pprof::criterion::PProfProfiler::new(100, pprof::criterion::Output::Flamegraph(None)));
    targets = bench_reader, bench_random_access);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_reader, bench_random_access);
criterion_main!(benches);
