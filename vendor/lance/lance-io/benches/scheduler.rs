// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
#![allow(clippy::print_stdout)]

use bytes::Bytes;
use lance_core::Result;
use lance_io::{
    object_store::ObjectStore,
    scheduler::{ScanScheduler, SchedulerConfig},
    utils::CachedFileSize,
};
use object_store::path::Path;
use rand::{seq::SliceRandom, RngCore};
use std::{fmt::Display, process::Command, sync::Arc};
use tokio::{runtime::Runtime, sync::mpsc};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
#[cfg(target_os = "linux")]
use pprof::criterion::{Output, PProfProfiler};

#[derive(Clone, Copy, Debug)]
struct FullReadParams {
    io_parallelism: u32,
    page_size: u64,
}

impl Display for FullReadParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "full_read,parallel={},read_size={}",
            self.io_parallelism, self.page_size
        )
    }
}

async fn create_data(num_bytes: u64) -> (Arc<ObjectStore>, Path) {
    let store_uri = std::env::var("STORE_URI").unwrap_or("/tmp/lance_bench".to_string());
    println!(
        "Reading from {}, use the environment variable STORE_URI to change this",
        store_uri
    );
    let (obj_store, path) = ObjectStore::from_uri(&store_uri).await.unwrap();
    let tmp_file = path.child("foo.file");

    let mut some_data = vec![0; num_bytes as usize];
    rand::rng().fill_bytes(&mut some_data);
    obj_store.put(&tmp_file, &some_data).await.unwrap();

    (obj_store, tmp_file)
}

const DATA_SIZE: u64 = 128 * 1024 * 1024;

async fn drain_task<F: std::future::Future<Output = Result<Vec<Bytes>>>>(
    mut rx: tokio::sync::mpsc::Receiver<F>,
) -> u64 {
    let mut bytes_received = 0;
    while let Some(fut) = rx.recv().await {
        let loaded = fut.await.unwrap();
        bytes_received += loaded.iter().map(|bytes| bytes.len() as u64).sum::<u64>();
    }
    bytes_received
}

/// This benchmark creates a file with DATA_SIZE bytes and then reads from it sequentially
/// with the given parallelism and read size
fn bench_full_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("from_elem");

    group.throughput(criterion::Throughput::Bytes(DATA_SIZE));

    let runtime = Runtime::new().unwrap();
    let (obj_store, tmp_file) = runtime.block_on(create_data(DATA_SIZE));

    for io_parallelism in [1, 16, 32, 64] {
        for page_size in [4096, 16 * 1024, 1024 * 1024] {
            let params = FullReadParams {
                io_parallelism,
                page_size,
            };
            group.bench_with_input(BenchmarkId::from_parameter(params), &params, |b, params| {
                b.iter(|| {
                    let obj_store = obj_store.clone();
                    if obj_store.is_local() {
                        let path_str = format!("/{}", tmp_file);
                        Command::new("dd")
                            .arg(format!("of={}", path_str))
                            .arg("oflag=nocache")
                            .arg("conv=notrunc,fdatasync")
                            .arg("count=0")
                            .output()
                            .unwrap();
                    }
                    std::env::set_var("IO_THREADS", io_parallelism.to_string());
                    runtime.block_on(async {
                        let scheduler =
                            ScanScheduler::new(obj_store, SchedulerConfig::default_for_testing());
                        let file_scheduler = scheduler
                            .open_file(&tmp_file, &CachedFileSize::unknown())
                            .await
                            .unwrap();

                        let (tx, rx) = mpsc::channel(1024);
                        let drainer = tokio::spawn(drain_task(rx));
                        let mut offset = 0;
                        while offset < DATA_SIZE {
                            #[allow(clippy::single_range_in_vec_init)]
                            let req = vec![offset..(offset + params.page_size)];
                            let req = file_scheduler.submit_request(req, 0);
                            tx.send(req).await.unwrap();
                            offset += params.page_size;
                        }
                        drop(tx);
                        let bytes_received = drainer.await.unwrap();
                        assert_eq!(bytes_received, DATA_SIZE);
                    });
                });
            });
        }
    }
}

const INDICES_PER_ITER: usize = 1000;
const INDICES_PER_BATCH: u32 = 10;

#[derive(Clone, Debug)]
struct RandomReadParams {
    io_parallelism: u32,
    item_size: u32,
    indices: Arc<Vec<u32>>,
}

impl Display for RandomReadParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "random_read,parallel={},item_size={}",
            self.io_parallelism, self.item_size
        )
    }
}

/// This benchmark creates a file with DATA_SIZE bytes which is then treated as
/// a contiguous array of items with width `item_size`.  We read a random selection
/// of INDICES_PER_ITER items from the array.  The selection is chosen randomly but
/// sorted before reading (to mimic actual use and make coalescing easier)
fn bench_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("from_elem");

    group.throughput(criterion::Throughput::Elements(INDICES_PER_ITER as u64));

    let runtime = Runtime::new().unwrap();
    let (obj_store, tmp_file) = runtime.block_on(create_data(DATA_SIZE));

    for io_parallelism in [1, 16, 32, 64] {
        for item_size in [8, 1024, 4096] {
            let num_indices = DATA_SIZE as u32 / item_size;
            let mut rng = rand::rng();
            let mut indices = (0..num_indices).collect::<Vec<_>>();
            let (shuffled, _) = indices.partial_shuffle(&mut rng, INDICES_PER_ITER);
            let mut indices = shuffled.to_vec();
            indices.sort_unstable();

            let params = RandomReadParams {
                io_parallelism,
                item_size,
                indices: Arc::new(indices),
            };
            group.bench_with_input(
                BenchmarkId::from_parameter(&params),
                &params,
                |b, params| {
                    b.iter(|| {
                        let obj_store = obj_store.clone();
                        if obj_store.is_local() {
                            let path_str = format!("/{}", tmp_file);
                            Command::new("dd")
                                .arg(format!("of={}", path_str))
                                .arg("oflag=nocache")
                                .arg("conv=notrunc,fdatasync")
                                .arg("count=0")
                                .output()
                                .unwrap();
                        }
                        std::env::set_var("IO_THREADS", params.io_parallelism.to_string());
                        runtime.block_on(async {
                            let scheduler = ScanScheduler::new(
                                obj_store,
                                SchedulerConfig::default_for_testing(),
                            );
                            let file_scheduler = scheduler
                                .open_file(&tmp_file, &CachedFileSize::unknown())
                                .await
                                .unwrap();

                            let (tx, rx) = mpsc::channel(1024);
                            let drainer = tokio::spawn(drain_task(rx));
                            let mut idx = 0;
                            while idx < params.indices.len() {
                                let iops = (idx..(idx + INDICES_PER_BATCH as usize))
                                    .map(|idx| {
                                        let start = idx as u64 * params.item_size as u64;
                                        let end = start + params.item_size as u64;
                                        start..end
                                    })
                                    .collect::<Vec<_>>();
                                idx += INDICES_PER_BATCH as usize;
                                let req = file_scheduler.submit_request(iops, 0);
                                tx.send(req).await.unwrap();
                            }
                            drop(tx);
                            let bytes_received = drainer.await.unwrap();
                            assert_eq!(bytes_received, INDICES_PER_ITER as u64 * item_size as u64);
                        });
                    });
                },
            );
        }
    }
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets =bench_full_read, bench_random_read);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_full_read, bench_random_read);

criterion_main!(benches);
