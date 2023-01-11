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

use std::ops::Deref;

use common_arrow::arrow::buffer::Buffer;
use common_expression::types::number::NumberColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::UInt64Type;
use common_expression::types::ValueType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::FunctionContext;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_sql::executor::PhysicalScalar;
use criterion::Criterion;
use rand::prelude::random;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use storages_common_index::filters::Filter;
use storages_common_index::filters::FilterBuilder;
use storages_common_index::filters::Xor8Builder;

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
    let column = rand_i64_column(1_000_000);

    let mut builder = Xor8Builder::create();
    (0..column.len()).for_each(|i| builder.add_key(unsafe { &column.index_unchecked(i) }));
    let filter = builder.build().unwrap();

    for i in 0..column.len() {
        let key = unsafe { column.index_unchecked(i) };
        assert!(filter.contains(&key), "key {} present", key);
    }

    c.bench_function("xor8_filter_u64_1m_rows_build_from_column_to_values", |b| {
        b.iter(|| {
            let mut builder = Xor8Builder::create();
            (0..column.len()).for_each(|i| builder.add_key(unsafe { &column.index_unchecked(i) }));
            let _filter = criterion::black_box(builder.build().unwrap());
        })
    });
}

fn bench_string(c: &mut Criterion) {
    let column = rand_str_column(1_000_000, 32);

    let mut builder = Xor8Builder::create();
    (0..column.len()).for_each(|i| builder.add_key(unsafe { &column.index_unchecked(i) }));
    let filter = builder.build().unwrap();

    for i in 0..column.len() {
        let key = unsafe { column.index_unchecked(i) };
        assert!(filter.contains(&key), "key {} present", key);
    }

    c.bench_function(
        "xor8_filter_string16to32_1m_rows_build_from_column_to_values",
        |b| {
            b.iter(|| {
                let mut builder = Xor8Builder::create();
                (0..column.len())
                    .for_each(|i| builder.add_key(unsafe { &column.index_unchecked(i) }));
                let _filter = criterion::black_box(builder.build().unwrap());
            })
        },
    );
}

fn bench_u64_using_digests(c: &mut Criterion) {
    let column = rand_i64_column(1_000_000);

    let mut builder = Xor8Builder::create();
    let digests = calculate_digests(&column, &DataType::Number(NumberDataType::Int64));
    builder.add_digests(digests.deref());
    let filter = builder.build().unwrap();

    for i in 0..digests.len() {
        let digest = unsafe { digests.get_unchecked(i) };
        assert!(filter.contains_digest(*digest), "digest {} present", digest);
    }

    c.bench_function(
        "xor8_filter_u64_1m_rows_build_from_column_to_digests",
        |b| {
            b.iter(|| {
                let mut builder = Xor8Builder::create();
                let digests = calculate_digests(&column, &DataType::Number(NumberDataType::Int64));
                builder.add_digests(digests.deref());
                let _filter = criterion::black_box(builder.build().unwrap());
            })
        },
    );
}

fn bench_string_using_digests(c: &mut Criterion) {
    let column = rand_str_column(1_000_000, 32);

    let mut builder = Xor8Builder::create();
    let digests = calculate_digests(&column, &DataType::String);
    builder.add_digests(digests.deref());
    let filter = builder.build().unwrap();

    for i in 0..digests.len() {
        let digest = unsafe { digests.get_unchecked(i) };
        assert!(filter.contains_digest(*digest), "digest {} present", digest);
    }

    c.bench_function(
        "xor8_filter_string16to32_1m_rows_build_from_column_to_digests",
        |b| {
            b.iter(|| {
                let mut builder = Xor8Builder::create();
                let digests = calculate_digests(&column, &DataType::String);
                builder.add_digests(digests.deref());
                let _filter = criterion::black_box(builder.build().unwrap());
            })
        },
    );
}

fn calculate_digests(column: &Column, data_type: &DataType) -> Buffer<u64> {
    let arg = PhysicalScalar::IndexedVariable {
        index: 0,
        data_type: data_type.clone(),
        display_name: "a".to_string(),
    };
    let siphash_func = PhysicalScalar::Function {
        name: "siphash".to_string(),
        params: vec![],
        args: vec![arg],
        return_type: DataType::Boolean,
    };
    let expr = siphash_func.as_expr().unwrap();

    let func_ctx = FunctionContext::default();
    let data_block = DataBlock::new_from_columns(vec![column.clone()]);
    let evaluator = Evaluator::new(&data_block, func_ctx, &BUILTIN_FUNCTIONS);
    let value = evaluator.run(&expr).unwrap();
    let col = value.convert_to_full_column(data_type, data_block.num_rows());
    UInt64Type::try_downcast_column(&col).unwrap()
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

criterion_group!(
    benches,
    bench_u64,
    bench_u64_using_digests,
    bench_string,
    bench_string_using_digests
);
criterion_main!(benches);
