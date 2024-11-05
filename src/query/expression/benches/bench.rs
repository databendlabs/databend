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

#[macro_use]
extern crate criterion;

use criterion::Criterion;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::StringType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_kernels");

    let mut rng = StdRng::seed_from_u64(0);
    // concats
    {
        for length in [12, 20, 500] {
            let (s, b) = generate_random_string_data(&mut rng, length);
            let bin_col = (0..5).map(|_| BinaryType::from_data(b.clone()));
            let str_col = (0..5).map(|_| StringType::from_data(s.clone()));

            group.bench_function(format!("concat_string_offset/{length}"), |b| {
                b.iter(|| Column::concat_columns(bin_col.clone()).unwrap())
            });

            group.bench_function(format!("concat_string_view/{length}"), |b| {
                b.iter(|| Column::concat_columns(str_col.clone()).unwrap())
            });
        }
    }

    // take compact
    {
        for length in [12, 20, 500] {
            let (s, b) = generate_random_string_data(&mut rng, length);
            let block_bin = DataBlock::new_from_columns(vec![BinaryType::from_data(b.clone())]);
            let block_view = DataBlock::new_from_columns(vec![StringType::from_data(s.clone())]);

            let indices: Vec<(u32, u32)> = (0..s.len())
                .filter(|x| x % 10 == 0)
                .map(|x| (x as u32, 1000))
                .collect();
            let num_rows = indices.len() * 1000;

            group.bench_function(format!("take_compact_string_offset/{length}"), |b| {
                b.iter(|| {
                    block_bin
                        .take_compacted_indices(&indices, num_rows)
                        .unwrap()
                })
            });

            group.bench_function(format!("take_compact_string_view/{length}"), |b| {
                b.iter(|| {
                    block_view
                        .take_compacted_indices(&indices, num_rows)
                        .unwrap()
                })
            });
        }
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);

fn generate_random_string_data(rng: &mut StdRng, length: usize) -> (Vec<String>, Vec<Vec<u8>>) {
    let iter_str: Vec<_> = (0..10000)
        .map(|_| {
            let random_string: String = (0..length)
                .map(|_| {
                    // Generate a random character (ASCII printable characters)
                    let random_char = rng.gen_range(32..=126) as u8 as char;
                    random_char
                })
                .collect();
            random_string
        })
        .collect();

    let iter_binary: Vec<_> = iter_str
        .iter()
        .map(|x| x.clone().as_bytes().to_vec())
        .collect();

    (iter_str, iter_binary)
}
