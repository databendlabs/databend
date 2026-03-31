// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
use std::{collections::HashMap, sync::Arc};

use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_select::take::take;
use criterion::{criterion_group, criterion_main, Criterion};
use lance_core::cache::LanceCache;
use lance_datagen::ArrayGeneratorExt;
use lance_encoding::{
    decoder::{DecoderPlugins, FilterExpression},
    encoder::{default_encoding_strategy, encode_batch, EncodingOptions},
    version::LanceFileVersion,
};

use rand::Rng;

const PRIMITIVE_TYPES: &[DataType] = &[
    DataType::Date32,
    DataType::Date64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Float16,
    DataType::Float32,
    DataType::Float64,
    DataType::Decimal128(10, 10),
    DataType::Decimal256(10, 10),
    DataType::Timestamp(TimeUnit::Nanosecond, None),
    DataType::Time32(TimeUnit::Second),
    DataType::Time64(TimeUnit::Nanosecond),
    DataType::Duration(TimeUnit::Second),
    // The Interval type is supported by the reader but the writer works with Lance schema
    // at the moment and Lance schema can't parse interval
    // DataType::Interval(IntervalUnit::DayTime),
];

// Some types are supported by the encoder/decoder but Lance
// schema doesn't yet parse them in the context of a fixed size list.
const PRIMITIVE_TYPES_FOR_FSL: &[DataType] = &[DataType::Int8, DataType::Float32];

const ENCODING_OPTIONS: EncodingOptions = EncodingOptions {
    cache_bytes_per_column: 8 * 1024 * 1024,
    max_page_bytes: 32 * 1024 * 1024,
    keep_original_array: true,
    buffer_alignment: 64,
};

fn bench_decode(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_primitive");
    const NUM_BYTES: u64 = 1024 * 1024 * 128;
    group.throughput(criterion::Throughput::Bytes(NUM_BYTES));
    for data_type in PRIMITIVE_TYPES {
        let func_name = format!("{:?}", data_type).to_lowercase();
        let num_rows = NUM_BYTES / data_type.primitive_width().unwrap() as u64;
        group.bench_function(func_name, |b| {
            let data = lance_datagen::gen_batch()
                .anon_col(lance_datagen::array::rand_type(data_type))
                .into_batch_rows(lance_datagen::RowCount::from(num_rows))
                .unwrap();
            let lance_schema =
                Arc::new(lance_core::datatypes::Schema::try_from(data.schema().as_ref()).unwrap());
            let encoding_strategy = default_encoding_strategy(LanceFileVersion::default());
            let encoded = rt
                .block_on(encode_batch(
                    &data,
                    lance_schema,
                    encoding_strategy.as_ref(),
                    &ENCODING_OPTIONS,
                ))
                .unwrap();

            b.iter(|| {
                let batch = rt
                    .block_on(lance_encoding::decoder::decode_batch(
                        &encoded,
                        &FilterExpression::no_filter(),
                        Arc::<DecoderPlugins>::default(),
                        false,
                        LanceFileVersion::default(),
                        Some(Arc::new(LanceCache::no_cache())),
                    ))
                    .unwrap();
                assert_eq!(data.num_rows(), batch.num_rows());
            })
        });
    }
}

fn bench_decode_fsl(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_fsl");
    const NUM_BYTES: u64 = 1024 * 1024 * 128;
    for version in [LanceFileVersion::V2_0, LanceFileVersion::V2_1] {
        for data_type in PRIMITIVE_TYPES_FOR_FSL {
            for dimension in [4, 16, 32, 64, 128] {
                let nullable_choices: &[bool] = if version == LanceFileVersion::V2_0 {
                    &[false]
                } else {
                    &[false, true]
                };
                for nullable in nullable_choices {
                    let func_name = format!(
                        "{:?}_{}_v{}_null{}",
                        data_type, dimension, version, nullable
                    )
                    .to_lowercase();
                    group.throughput(criterion::Throughput::Bytes(NUM_BYTES));
                    group.bench_function(func_name, |b| {
                        let num_rows =
                            NUM_BYTES / (dimension * data_type.primitive_width().unwrap() as u64);
                        let mut arraygen =
                            lance_datagen::array::rand_type(&DataType::FixedSizeList(
                                Arc::new(Field::new("item", data_type.clone(), true)),
                                dimension as i32,
                            ));
                        if *nullable {
                            arraygen = arraygen.with_random_nulls(0.5);
                        }
                        let data = lance_datagen::gen_batch()
                            .anon_col(arraygen)
                            .into_batch_rows(lance_datagen::RowCount::from(num_rows))
                            .unwrap();
                        let lance_schema = Arc::new(
                            lance_core::datatypes::Schema::try_from(data.schema().as_ref())
                                .unwrap(),
                        );
                        let encoding_strategy = default_encoding_strategy(version);
                        let encoded = rt
                            .block_on(encode_batch(
                                &data,
                                lance_schema,
                                encoding_strategy.as_ref(),
                                &ENCODING_OPTIONS,
                            ))
                            .unwrap();
                        b.iter(|| {
                            let batch = rt
                                .block_on(lance_encoding::decoder::decode_batch(
                                    &encoded,
                                    &FilterExpression::no_filter(),
                                    Arc::<DecoderPlugins>::default(),
                                    false,
                                    version,
                                    Some(Arc::new(LanceCache::no_cache())),
                                ))
                                .unwrap();
                            assert_eq!(data.num_rows(), batch.num_rows());
                        })
                    });
                }
            }
        }
    }
}

fn bench_decode_str_with_dict_encoding(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_primitive");
    const NUM_ROWS: u64 = 100000;

    let data_type = DataType::Utf8;
    // generate string column with 20 rows
    let string_data = lance_datagen::gen_batch()
        .anon_col(lance_datagen::array::rand_type(&DataType::Utf8))
        .into_batch_rows(lance_datagen::RowCount::from(20))
        .unwrap();

    group.throughput(criterion::Throughput::Bytes(
        NUM_ROWS * std::mem::size_of::<u32>() as u64 + string_data.get_array_memory_size() as u64,
    ));

    let func_name = format!("{:?}", data_type).to_lowercase();
    group.bench_function(func_name, |b| {
        let string_array = string_data.column(0);

        // generate random int column with 100000 rows
        let mut rng = rand::rng();
        let integer_arr: Vec<u32> = (0..100_000).map(|_| rng.random_range(0..20)).collect();
        let integer_array = UInt32Array::from(integer_arr);

        let mapped_strings = take(string_array, &integer_array, None).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "string",
            DataType::Utf8,
            false,
        )]));

        let data = RecordBatch::try_new(schema, vec![Arc::new(mapped_strings)]).unwrap();

        let lance_schema =
            Arc::new(lance_core::datatypes::Schema::try_from(data.schema().as_ref()).unwrap());
        let encoding_strategy = default_encoding_strategy(LanceFileVersion::default());
        let encoded = rt
            .block_on(encode_batch(
                &data,
                lance_schema,
                encoding_strategy.as_ref(),
                &ENCODING_OPTIONS,
            ))
            .unwrap();
        b.iter(|| {
            let batch = rt
                .block_on(lance_encoding::decoder::decode_batch(
                    &encoded,
                    &FilterExpression::no_filter(),
                    Arc::<DecoderPlugins>::default(),
                    false,
                    LanceFileVersion::default(),
                    Some(Arc::new(LanceCache::no_cache())),
                ))
                .unwrap();
            assert_eq!(data.num_rows(), batch.num_rows());
        })
    });
}

fn bench_decode_packed_struct(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_primitive");

    const NUM_ROWS: u64 = 10000;
    let size_bytes =
        ((6 * std::mem::size_of::<i32>() as u64) + std::mem::size_of::<f32>() as u64) * NUM_ROWS;
    group.throughput(criterion::Throughput::Bytes(size_bytes));

    let func_name = "struct";
    group.bench_function(func_name, |b| {
        let fields = vec![
            Arc::new(Field::new("int_field", DataType::Int32, false)),
            Arc::new(Field::new("float_field", DataType::Float32, false)),
            Arc::new(Field::new(
                "fsl_field",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 5),
                false,
            )),
        ]
        .into();

        // generate struct column with 1M rows
        let data = lance_datagen::gen_batch()
            .anon_col(lance_datagen::array::rand_type(&DataType::Struct(fields)))
            .into_batch_rows(lance_datagen::RowCount::from(NUM_ROWS))
            .unwrap();

        let schema = data.schema();
        let new_fields: Vec<Arc<Field>> = schema
            .fields()
            .iter()
            .map(|field| {
                if matches!(field.data_type(), &DataType::Struct(_)) {
                    let mut metadata = HashMap::new();
                    metadata.insert("packed".to_string(), "true".to_string());
                    let field =
                        Field::new(field.name(), field.data_type().clone(), field.is_nullable());
                    Arc::new(field.with_metadata(metadata))
                } else {
                    field.clone()
                }
            })
            .collect();

        let new_schema = Schema::new(new_fields);
        let data =
            RecordBatch::try_new(Arc::new(new_schema.clone()), data.columns().to_vec()).unwrap();

        let lance_schema = Arc::new(lance_core::datatypes::Schema::try_from(&new_schema).unwrap());
        let encoding_strategy = default_encoding_strategy(LanceFileVersion::default());
        let encoded = rt
            .block_on(encode_batch(
                &data,
                lance_schema,
                encoding_strategy.as_ref(),
                &ENCODING_OPTIONS,
            ))
            .unwrap();

        b.iter(|| {
            let batch = rt
                .block_on(lance_encoding::decoder::decode_batch(
                    &encoded,
                    &FilterExpression::no_filter(),
                    Arc::<DecoderPlugins>::default(),
                    false,
                    LanceFileVersion::default(),
                    Some(Arc::new(LanceCache::no_cache())),
                ))
                .unwrap();
            assert_eq!(data.num_rows(), batch.num_rows());
        })
    });
}

#[allow(dead_code)]
fn bench_decode_str_with_fixed_size_binary_encoding(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_primitive");

    const NUM_ROWS: u64 = 10000;
    // Randomly generated strings are always 12 characters (at the moment)
    // Plus we need 4 bytes for the offset
    const NUM_BYTES: u64 = NUM_ROWS * 16;
    group.throughput(criterion::Throughput::Bytes(NUM_BYTES));

    let func_name = "fixed-utf8".to_string();
    group.bench_function(func_name, |b| {
        // generate string column with 10k rows
        // Currently the generator generates fixed size strings by default
        // This function will need to be updated once that changes.
        let string_data = lance_datagen::gen_batch()
            .anon_col(lance_datagen::array::rand_type(&DataType::Utf8))
            .into_batch_rows(lance_datagen::RowCount::from(10000))
            .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "string",
            DataType::Utf8,
            false,
        )]));

        let data = RecordBatch::try_new(schema, string_data.columns().to_vec()).unwrap();

        let lance_schema =
            Arc::new(lance_core::datatypes::Schema::try_from(data.schema().as_ref()).unwrap());
        let encoding_strategy = default_encoding_strategy(LanceFileVersion::default());
        let encoded = rt
            .block_on(encode_batch(
                &data,
                lance_schema,
                encoding_strategy.as_ref(),
                &ENCODING_OPTIONS,
            ))
            .unwrap();
        b.iter(|| {
            let batch = rt
                .block_on(lance_encoding::decoder::decode_batch(
                    &encoded,
                    &FilterExpression::no_filter(),
                    Arc::<DecoderPlugins>::default(),
                    false,
                    LanceFileVersion::default(),
                    Some(Arc::new(LanceCache::no_cache())),
                ))
                .unwrap();
            assert_eq!(data.num_rows(), batch.num_rows());
        })
    });
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(pprof::criterion::PProfProfiler::new(100, pprof::criterion::Output::Flamegraph(None)));
    targets = bench_decode, bench_decode_fsl, bench_decode_str_with_dict_encoding, bench_decode_packed_struct,
                bench_decode_str_with_fixed_size_binary_encoding);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_decode, bench_decode_fsl, bench_decode_str_with_dict_encoding, bench_decode_packed_struct);
criterion_main!(benches);
