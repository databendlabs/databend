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

#![feature(portable_simd)]

fn main() {
    divan::main()
}

// Timer precision: 10 ns
// bench                fastest       │ slowest       │ median        │ mean          │ samples │ iters
// ╰─ dummy                           │               │               │               │         │
//    ├─ native_deser                 │               │               │               │         │
//    │  ├─ LZ4         588.9 ms      │ 588.9 ms      │ 588.9 ms      │ 588.9 ms      │ 1       │ 1
//    │  │              3.873 GB/s    │ 3.873 GB/s    │ 3.873 GB/s    │ 3.873 GB/s    │         │
//    │  ╰─ Zstd        832.1 ms      │ 832.1 ms      │ 832.1 ms      │ 832.1 ms      │ 1       │ 1
//    │                 1.942 GB/s    │ 1.942 GB/s    │ 1.942 GB/s    │ 1.942 GB/s    │         │
//    ╰─ parquet_deser                │               │               │               │         │
//       ├─ LZ4         807.5 ms      │ 807.5 ms      │ 807.5 ms      │ 807.5 ms      │ 1       │ 1
//       │              3.176 GB/s    │ 3.176 GB/s    │ 3.176 GB/s    │ 3.176 GB/s    │         │
//       ╰─ Zstd        1.009 s       │ 1.009 s       │ 1.009 s       │ 1.009 s       │ 1       │ 1
//                      1.425 GB/s    │ 1.425 GB/s    │ 1.425 GB/s    │ 1.425 GB/s    │         │
#[divan::bench_group(max_time = 3)]
mod dummy {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use bytes::Bytes;
    use databend_common_expression::DataBlock;
    use databend_common_expression::DataSchema;
    use databend_common_expression::TableSchema;
    use databend_common_expression::TableSchemaRef;
    use databend_common_storages_fuse::FuseStorageFormat;
    use databend_common_storages_fuse::index::BloomIndexType;
    use databend_common_storages_fuse::io::WriteSettings;
    use databend_common_storages_fuse::io::serialize_block;
    use databend_storages_common_table_meta::table::TableCompression;
    use divan::counter::BytesCount;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    const NUM_ROWS: usize = 6001215;

    fn read_parquet_file() -> (DataBlock, TableSchema) {
        // 246M    /tmp/tpch_1/lineitem.parquet/
        // generate by duckdb:
        // CALL dbgen(sf=1)
        // EXPORT DATABASE '/tmp/tpch_1/' (FORMAT PARQUET)
        let file = "/tmp/tpch_1/lineitem.parquet";
        let file = std::fs::File::open(file).unwrap();

        // Create a sync parquet reader with batch_size.
        // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
        let mut parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(usize::MAX)
            .build()
            .unwrap();

        let batch = parquet_reader.next().unwrap();
        let batch = batch.unwrap();
        let schema: TableSchema = batch.schema().as_ref().try_into().unwrap();
        let data_schema = DataSchema::from(&schema);
        let block = DataBlock::from_record_batch(&data_schema, &batch).unwrap();
        (block, schema)
    }

    #[divan::bench(args = [TableCompression::LZ4, TableCompression::Zstd])]
    fn parquet_deser(bencher: divan::Bencher, compression: TableCompression) {
        // write the block into temp memory buffers
        // prepare the metas
        // use deserialize_chunk to read back into block
        bencher
            .with_inputs(|| prepare_format_file(FuseStorageFormat::Parquet, compression))
            .input_counter(|(a, _)| {
                // Changes based on input.
                BytesCount::usize(a.len())
            })
            .bench_refs(|(a, _)| {
                let reader = ParquetRecordBatchReaderBuilder::try_new(a.clone())
                    .unwrap()
                    .with_batch_size(8192)
                    .build()
                    .unwrap();
                let batch: Vec<Result<RecordBatch, arrow_schema::ArrowError>> = reader.collect();
                let batch = batch.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>();
                let num_rows: usize = batch.iter().map(|b| b.num_rows()).sum();
                assert_eq!(num_rows, NUM_ROWS);
            });
    }

    fn prepare_format_file(
        storage_format: FuseStorageFormat,
        compression: TableCompression,
    ) -> (Bytes, TableSchemaRef) {
        let (datablock, schema) = read_parquet_file();
        // write the block into temp memory buffers
        let block_per_seg = 1000;

        let enable_parquet_dictionary = false;
        let write_settings = WriteSettings {
            storage_format,
            table_compression: compression,
            bloom_index_type: BloomIndexType::default(),
            block_per_seg,
            enable_parquet_dictionary,
            data_page_rows: None,
            data_page_bytes: None,
            index_granularity: None,
            col_stats_truncate_lens: std::collections::BTreeMap::new(),
        };
        let schema = Arc::new(schema);
        let (_, buffer) = serialize_block(&write_settings, &schema, datablock).unwrap();

        (buffer.to_bytes(), schema)
    }
}

// Run with the deployment baseline (AVX2 remains runtime-dispatched):
// RUSTFLAGS='-C target-feature=+sse4.2' \
//   cargo bench -p databend-common-storages-fuse --bench bench runtime_bloom
#[divan::bench_group(max_time = 2)]
mod runtime_bloom {
    use core::simd::Simd;
    use core::simd::cmp::SimdPartialEq;
    use std::sync::Arc;

    use bytes::Bytes;
    use databend_common_catalog::sbbf::Sbbf;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::DataSchema;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::hash_util::hash_by_method_for_bloom;
    use databend_common_expression::types::MutableBitmap;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::NumberColumn;
    use databend_common_storages_fuse::FuseStorageFormat;
    use databend_common_storages_fuse::index::BloomIndexType;
    use databend_common_storages_fuse::io::WriteSettings;
    use databend_common_storages_fuse::io::serialize_block;
    use databend_common_storages_fuse::pruning::ExprBloomFilter;
    use databend_storages_common_table_meta::table::TableCompression;
    use divan::black_box;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    const REPRESENTATIVE: Scenario = Scenario {
        rows: 1_048_576,
        ndv: 65_536,
        hit_stride: Some(100),
    };
    const SALT: [u32; 8] = [
        0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947,
        0x5c6bfb31,
    ];

    type U32x8 = Simd<u32, 8>;

    #[derive(Clone, Copy, Debug)]
    enum NumberKind {
        Int64,
        UInt64,
    }

    #[derive(Clone, Copy, Debug)]
    struct Scenario {
        rows: usize,
        ndv: usize,
        /// One true match every `hit_stride` rows. `None` means no true matches.
        hit_stride: Option<usize>,
    }

    struct LookupInput {
        filter: Sbbf,
        portable_filter: PortableSbbf,
        hashes: Vec<u64>,
    }

    struct ExprInput {
        filter: Sbbf,
        portable_filter: PortableSbbf,
        column: Column,
    }

    struct ParquetInput {
        filter: Sbbf,
        portable_filter: PortableSbbf,
        parquet: Bytes,
        data_schema: DataSchema,
    }

    /// The portable-SIMD lookup implementation used before the optimized batch path.
    struct PortableSbbf(Vec<[u32; 8]>);

    impl PortableSbbf {
        fn new(num_blocks: usize, hashes: &[u64]) -> Self {
            let mut filter = Self(vec![[0; 8]; num_blocks]);
            for &hash in hashes {
                let block_index = filter.block_index(hash);
                let mask = Self::mask(hash as u32).to_array();
                for (slot, bit) in filter.0[block_index].iter_mut().zip(mask) {
                    *slot |= bit;
                }
            }
            filter
        }

        #[inline]
        fn check_hash(&self, hash: u64) -> bool {
            let mask = Self::mask(hash as u32);
            let block = U32x8::from_array(self.0[self.block_index(hash)]);
            (block & mask).simd_ne(U32x8::splat(0)).all()
        }

        #[inline]
        fn block_index(&self, hash: u64) -> usize {
            (((hash >> 32) * self.0.len() as u64) >> 32) as usize
        }

        #[inline]
        fn mask(hash: u32) -> U32x8 {
            let hash = U32x8::splat(hash);
            let bit_index = (hash * U32x8::from_array(SALT)) >> U32x8::splat(27);
            U32x8::splat(1) << bit_index
        }
    }

    #[divan::bench(args = [NumberKind::Int64, NumberKind::UInt64])]
    fn lookup_optimized(bencher: divan::Bencher, kind: NumberKind) {
        bencher
            .with_inputs(|| prepare_lookup(kind, REPRESENTATIVE))
            .bench_refs(|input| {
                let mut matches = 0;
                input
                    .filter
                    .check_hash_batch(&input.hashes, |_| matches += 1);
                black_box(matches)
            });
    }

    #[divan::bench(args = [NumberKind::Int64, NumberKind::UInt64])]
    fn lookup_portable_simd(bencher: divan::Bencher, kind: NumberKind) {
        bencher
            .with_inputs(|| prepare_lookup(kind, REPRESENTATIVE))
            .bench_refs(|input| {
                black_box(
                    input
                        .hashes
                        .iter()
                        .filter(|&&hash| input.portable_filter.check_hash(hash))
                        .count(),
                )
            });
    }

    #[divan::bench(args = [NumberKind::Int64, NumberKind::UInt64])]
    fn hash_and_bloom_optimized(bencher: divan::Bencher, kind: NumberKind) {
        bencher
            .with_inputs(|| prepare_expr(kind, REPRESENTATIVE))
            .bench_refs(|input| {
                black_box(
                    ExprBloomFilter::new(&input.filter)
                        .apply(input.column.clone())
                        .unwrap(),
                )
            });
    }

    #[divan::bench(args = [NumberKind::Int64, NumberKind::UInt64])]
    fn hash_and_bloom_portable_simd(bencher: divan::Bencher, kind: NumberKind) {
        bencher
            .with_inputs(|| prepare_expr(kind, REPRESENTATIVE))
            .bench_refs(|input| {
                black_box(apply_portable(&input.portable_filter, input.column.clone()))
            });
    }

    #[divan::bench(args = [NumberKind::Int64, NumberKind::UInt64])]
    fn parquet_zstd_and_bloom_optimized(bencher: divan::Bencher, kind: NumberKind) {
        bencher
            .with_inputs(|| prepare_parquet(kind, REPRESENTATIVE))
            .bench_refs(|input| {
                let reader = ParquetRecordBatchReaderBuilder::try_new(input.parquet.clone())
                    .unwrap()
                    .with_batch_size(8_192)
                    .build()
                    .unwrap();
                let mut matches = 0;
                for batch in reader {
                    let block =
                        DataBlock::from_record_batch(&input.data_schema, &batch.unwrap()).unwrap();
                    let bitmap = ExprBloomFilter::new(&input.filter)
                        .apply(block.get_by_offset(0).to_column())
                        .unwrap();
                    matches += bitmap.iter().filter(|matched| *matched).count();
                }
                black_box(matches)
            });
    }

    #[divan::bench(args = [NumberKind::Int64, NumberKind::UInt64])]
    fn parquet_zstd_and_bloom_portable_simd(bencher: divan::Bencher, kind: NumberKind) {
        bencher
            .with_inputs(|| prepare_parquet(kind, REPRESENTATIVE))
            .bench_refs(|input| {
                let reader = ParquetRecordBatchReaderBuilder::try_new(input.parquet.clone())
                    .unwrap()
                    .with_batch_size(8_192)
                    .build()
                    .unwrap();
                let mut matches = 0;
                for batch in reader {
                    let block =
                        DataBlock::from_record_batch(&input.data_schema, &batch.unwrap()).unwrap();
                    let bitmap =
                        apply_portable(&input.portable_filter, block.get_by_offset(0).to_column());
                    matches += bitmap.iter().filter(|matched| *matched).count();
                }
                black_box(matches)
            });
    }

    #[divan::bench(args = [
        Scenario { rows: 262_144, ndv: 1_024, hit_stride: None },
        Scenario { rows: 524_288, ndv: 1_048_576, hit_stride: Some(1_000) },
        Scenario { rows: 1_048_576, ndv: 65_536, hit_stride: Some(10) },
        Scenario { rows: 1_048_576, ndv: 65_536, hit_stride: Some(1) },
    ])]
    fn lookup_scenarios_optimized(bencher: divan::Bencher, scenario: Scenario) {
        bencher
            .with_inputs(|| prepare_lookup(NumberKind::Int64, scenario))
            .bench_refs(|input| {
                let mut matches = 0;
                input
                    .filter
                    .check_hash_batch(&input.hashes, |_| matches += 1);
                black_box(matches)
            });
    }

    #[divan::bench(args = [
        Scenario { rows: 262_144, ndv: 1_024, hit_stride: None },
        Scenario { rows: 524_288, ndv: 1_048_576, hit_stride: Some(1_000) },
        Scenario { rows: 1_048_576, ndv: 65_536, hit_stride: Some(10) },
        Scenario { rows: 1_048_576, ndv: 65_536, hit_stride: Some(1) },
    ])]
    fn lookup_scenarios_portable_simd(bencher: divan::Bencher, scenario: Scenario) {
        bencher
            .with_inputs(|| prepare_lookup(NumberKind::Int64, scenario))
            .bench_refs(|input| {
                black_box(
                    input
                        .hashes
                        .iter()
                        .filter(|&&hash| input.portable_filter.check_hash(hash))
                        .count(),
                )
            });
    }

    fn prepare_lookup(kind: NumberKind, scenario: Scenario) -> LookupInput {
        let (filter, portable_filter) = make_filters(kind, scenario.ndv);
        let column = make_column(kind, &probe_values(scenario));
        let hashes = make_hashes(column);
        assert_filters_match(&filter, &portable_filter, &hashes);
        LookupInput {
            filter,
            portable_filter,
            hashes,
        }
    }

    fn prepare_expr(kind: NumberKind, scenario: Scenario) -> ExprInput {
        let (filter, portable_filter) = make_filters(kind, scenario.ndv);
        ExprInput {
            filter,
            portable_filter,
            column: make_column(kind, &probe_values(scenario)),
        }
    }

    fn prepare_parquet(kind: NumberKind, scenario: Scenario) -> ParquetInput {
        let (filter, portable_filter) = make_filters(kind, scenario.ndv);
        let block = DataBlock::new_from_columns(vec![make_column(kind, &probe_values(scenario))]);
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "value",
            match kind {
                NumberKind::Int64 => TableDataType::Number(NumberDataType::Int64),
                NumberKind::UInt64 => TableDataType::Number(NumberDataType::UInt64),
            },
        )]));
        let data_schema = DataSchema::from(schema.as_ref());
        let settings = WriteSettings {
            storage_format: FuseStorageFormat::Parquet,
            table_compression: TableCompression::Zstd,
            bloom_index_type: BloomIndexType::default(),
            block_per_seg: 1_000,
            enable_parquet_dictionary: false,
            data_page_rows: None,
            data_page_bytes: None,
            col_stats_truncate_lens: std::collections::BTreeMap::new(),
        };
        let (_, buffer) = serialize_block(&settings, &schema, block).unwrap();
        ParquetInput {
            filter,
            portable_filter,
            parquet: buffer.to_bytes(),
            data_schema,
        }
    }

    fn make_filters(kind: NumberKind, ndv: usize) -> (Sbbf, PortableSbbf) {
        let inserted = (0..ndv)
            .map(|index| mix_hash(index as u64))
            .collect::<Vec<_>>();
        let hashes = make_hashes(make_column(kind, &inserted));
        let mut filter = Sbbf::new_with_ndv_fpp(ndv as u64, 0.01).unwrap();
        filter.insert_hash_batch(&hashes);
        let num_blocks = filter.estimated_memory_size() / std::mem::size_of::<[u32; 8]>();
        let portable_filter = PortableSbbf::new(num_blocks, &hashes);
        (filter, portable_filter)
    }

    fn apply_portable(filter: &PortableSbbf, column: Column) -> MutableBitmap {
        let hashes = make_hashes(column);
        let iter = hashes.iter().map(|&hash| filter.check_hash(hash));
        // SAFETY: iter length equals hashes.len().
        unsafe { MutableBitmap::from_trusted_len_iter_unchecked(iter) }
    }

    fn assert_filters_match(filter: &Sbbf, portable_filter: &PortableSbbf, hashes: &[u64]) {
        let step = (hashes.len() / 1_024).max(1);
        for &hash in hashes.iter().step_by(step) {
            assert_eq!(filter.check_hash(hash), portable_filter.check_hash(hash));
        }
    }

    fn make_hashes(column: Column) -> Vec<u64> {
        let num_rows = column.len();
        let method = DataBlock::choose_hash_method_with_types(&[column.data_type()]).unwrap();
        let entries = &[column.into()];
        let group_columns = entries.into();
        let mut hashes = Vec::with_capacity(num_rows);
        hash_by_method_for_bloom(&method, group_columns, num_rows, &mut hashes).unwrap();
        hashes
    }

    fn make_column(kind: NumberKind, values: &[u64]) -> Column {
        match kind {
            NumberKind::Int64 => Column::Number(NumberColumn::Int64(
                values
                    .iter()
                    .map(|value| *value as i64)
                    .collect::<Vec<_>>()
                    .into(),
            )),
            NumberKind::UInt64 => Column::Number(NumberColumn::UInt64(values.to_vec().into())),
        }
    }

    fn probe_values(scenario: Scenario) -> Vec<u64> {
        (0..scenario.rows)
            .map(|index| {
                if scenario
                    .hit_stride
                    .is_some_and(|stride| index % stride == 0)
                {
                    mix_hash((index % scenario.ndv) as u64)
                } else {
                    mix_hash((index + scenario.ndv + scenario.rows) as u64)
                }
            })
            .collect()
    }

    fn mix_hash(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}
