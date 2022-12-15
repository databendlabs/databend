// Copyright 2021 Datafuse Labs.
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

use common_expression::types::number::NumberColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::Column;
use common_storages_index::filters::FilterBuilder;
use common_storages_index::filters::Xor8Builder;
use criterion::Criterion;
use rand::prelude::random;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

/// Benchmark building BlockFilter from DataBlock.
///
/// Benchmark and optimization log:
///
/// - 2022-10-14:
///   Reproduce the building process same as databend-query does: collect keys with `column.to_values()`:
///   i64:                       210ns/key
///   string of length 16 to 32: 240ns/key
///
/// - 2022-12-7:
///   Platform: MacBook Pro M1 MAX
///   i64:                       122ns/key
///   string of length 16 to 32: 123ns/key

fn bench_u64(c: &mut Criterion) {
    let block = rand_i64_block(1_000_000);
    let column = block.try_column_by_name("a").unwrap();

    let mut builder = Xor8Builder::create();
    (0..column.len()).for_each(|i| builder.add_key(&column.get(i)));
    let filter = builder.build().unwrap();

    for i in 0..column.len() {
        let key = column.get(i);
        assert!(filter.contains(&key), "key {} present", key);
    }

    c.bench_function("xor8_filter_u64_1m_rows_build_from_column_to_values", |b| {
        b.iter(|| {
            let mut builder = Xor8Builder::create();
            (0..column.len()).for_each(|i| builder.add_key(&column.get(i)));
            let _filter = criterion::black_box(builder.build().unwrap());
        })
    });
}

fn bench_string(c: &mut Criterion) {
    let block = rand_str_block(1_000_000, 32);
    let column = block.try_column_by_name("a").unwrap();

    let mut builder = Xor8Builder::create();
    (0..column.len()).for_each(|i| builder.add_key(&column.get(i)));
    let filter = builder.build().unwrap();

    for i in 0..column.len() {
        let key = column.get(i);
        assert!(filter.contains(&key), "key {} present", key);
    }

    c.bench_function(
        "xor8_filter_string16to32_1m_rows_build_from_column_to_values",
        |b| {
            b.iter(|| {
                let mut builder = Xor8Builder::create();
                (0..column.len()).for_each(|i| builder.add_key(&column.get(i)));
                let _filter = criterion::black_box(builder.build().unwrap());
            })
        },
    );
}

fn rand_i64_column(n: i32) -> Column {
    let seed: u64 = random();

    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<i64> = (0..n).map(|_| rng.gen::<i64>()).collect();

    Column::Number(NumberColumn::Int64(keys.into()))
}

fn rand_str_column(n: i32, len: i32) -> Column {
    let seed: u64 = random();

    let mut rng = StdRng::seed_from_u64(seed);
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789)(*&^%$#@!~";

    let mut builder = StringColumnBuilder::with_capacity(n as usize, 0);
    for _ in 0..n {
        for _ in (len / 2)..len {
            let idx = rng.gen_range(0..CHARSET.len());
            builder.put_char(CHARSET[idx] as char);
        }
        builder.commit_row();
    }

    Column::String(builder.build())
}

criterion_group!(benches, bench_u64, bench_string);
criterion_main!(benches);
