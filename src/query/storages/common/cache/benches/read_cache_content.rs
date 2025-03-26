// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use databend_storages_common_cache::read_cache_content;
use tempfile::TempDir;

fn create_test_file(dir: &TempDir, name: &str, size: usize) -> PathBuf {
    let path = dir.path().join(name);
    let mut file = File::create(&path).unwrap();
    let content = vec![42u8; size];
    file.write_all(&content).unwrap();
    path
}

fn bench_read_cache_content(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    let small_file_path = create_test_file(&temp_dir, "small.bin", 4 * 1024); // 4KB
    let medium_file_path = create_test_file(&temp_dir, "medium.bin", 64 * 1024); // 64KB
    let large_file_path = create_test_file(&temp_dir, "large.bin", 1024 * 1024); // 1MB

    // Benchmark Unix specific implementation (open with O_NOATIME, read with libc::read without stat / seek)
    #[cfg(target_os = "linux")]
    {
        let mut group = c.benchmark_group("read_cache_content_linux_specific_impl");

        group.bench_function("small_file_4KB", |b| {
            b.iter(|| {
                let path = small_file_path.clone();
                let result = read_cache_content(path, 4 * 1024);
                black_box(result)
            })
        });

        group.bench_function("medium_file_64KB", |b| {
            b.iter(|| {
                let path = medium_file_path.clone();
                let result = read_cache_content(path, 64 * 1024);
                black_box(result)
            })
        });

        group.bench_function("large_file_1MB", |b| {
            b.iter(|| {
                let path = large_file_path.clone();
                let result = read_cache_content(path, 1024 * 1024);
                black_box(result)
            })
        });

        group.finish();
    }

    // Benchmark using read_to_end from std lib
    let mut group = c.benchmark_group("read_cache_std_lib_read_to_end");

    group.bench_function("small_file_4KB", |b| {
        b.iter(|| {
            let path = small_file_path.clone();
            let result = fallback_std_lib_read_to_end(path, 4 * 1024);
            black_box(result)
        })
    });

    group.bench_function("medium_file_64KB", |b| {
        b.iter(|| {
            let path = medium_file_path.clone();
            let result = fallback_std_lib_read_to_end(path, 64 * 1024);
            black_box(result)
        })
    });

    group.bench_function("large_file_1MB", |b| {
        b.iter(|| {
            let path = large_file_path.clone();
            let result = fallback_std_lib_read_to_end(path, 1024 * 1024);
            black_box(result)
        })
    });

    group.finish();
}

fn fallback_std_lib_read_to_end(path: PathBuf, size: usize) -> std::io::Result<Vec<u8>> {
    use std::fs::File;
    use std::io::Read;

    let mut v = Vec::with_capacity(size);
    let mut file = File::open(path)?;
    file.read_to_end(&mut v)?;
    Ok(v)
}

criterion_group!(benches, bench_read_cache_content);
criterion_main!(benches);
