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

fn main() {
    divan::main()
}

// read_cache_content                          fastest       │ slowest       │ median        │ mean          │ samples │ iters
// ╰─ dummy                                                  │               │               │               │         │
//    ├─ bench_read_cache_content                            │               │               │               │         │
//    │  ├─ 4                                  2.58 µs       │ 7.169 µs      │ 2.663 µs      │ 2.751 µs      │ 100     │ 100
//    │  ├─ 64                                 7.18 µs       │ 9.665 µs      │ 7.346 µs      │ 7.385 µs      │ 100     │ 100
//    │  ╰─ 1024                               52.27 µs      │ 132.4 µs      │ 91.4 µs       │ 78.31 µs      │ 100     │ 100
//    ╰─ bench_read_cache_std_lib_read_to_end                │               │               │               │         │
//       ├─ 4                                  1.224 µs      │ 4.253 µs      │ 1.255 µs      │ 1.292 µs      │ 100     │ 100
//       ├─ 64                                 3.087 µs      │ 3.53 µs       │ 3.151 µs      │ 3.166 µs      │ 100     │ 100
//       ╰─ 1024                               51.45 µs      │ 328.9 µs      │ 53.24 µs      │ 61.26 µs      │ 100     │ 100
#[cfg(target_os = "linux")]
#[divan::bench_group(max_time = 0.5)]
mod dummy {
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;

    #[cfg(target_os = "linux")]
    use databend_storages_common_cache::read_cache_content;
    use divan::black_box;
    use tempfile::TempDir;

    fn create_test_file(dir: &TempDir, name: &str, size: usize) -> PathBuf {
        let path = dir.path().join(name);
        let mut file = File::create(&path).unwrap();
        let content = vec![42u8; size];
        file.write_all(&content).unwrap();
        path
    }

    fn input_file(name: &str, size: usize) -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let fp = create_test_file(&temp_dir, name, size);
        (temp_dir, fp)
    }

    #[cfg(target_os = "linux")]
    #[divan::bench(args = [4 , 64, 1024])]
    fn bench_read_cache_content(bencher: divan::Bencher, ksize: usize) {
        let name = format!("{ksize}k.bin");
        bencher
            .with_inputs(|| input_file(&name, ksize * 1024))
            .bench_refs(|(_d, fp): &mut (TempDir, PathBuf)| {
                let path = fp.to_owned();
                let result = read_cache_content(path, ksize * 1024);
                black_box(result)
            });
    }

    #[cfg(target_os = "linux")]
    #[divan::bench(args = [4 , 64, 1024])]
    fn bench_read_cache_std_lib_read_to_end(bencher: divan::Bencher, ksize: usize) {
        let name = format!("{ksize}k.bin");
        bencher
            .with_inputs(|| input_file(&name, ksize * 1024))
            .bench_refs(|(_d, fp): &mut (TempDir, PathBuf)| {
                let path = fp.clone();
                let result = fallback_std_lib_read_to_end(path, 4 * 1024);
                black_box(result)
            });
    }
    fn fallback_std_lib_read_to_end(path: PathBuf, size: usize) -> std::io::Result<Vec<u8>> {
        use std::fs::File;
        use std::io::Read;

        let mut v = Vec::with_capacity(size);
        let mut file = File::open(path)?;
        file.read_to_end(&mut v)?;
        Ok(v)
    }
}
