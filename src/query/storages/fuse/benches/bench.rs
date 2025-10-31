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
    use databend_common_native::read::NativeColumnsReader;
    use databend_common_storages_fuse::io::serialize_block;
    use databend_common_storages_fuse::io::WriteSettings;
    use databend_common_storages_fuse::FuseStorageFormat;
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

    #[divan::bench(args = [TableCompression::LZ4, TableCompression::Zstd])]
    fn native_deser(bencher: divan::Bencher, compression: TableCompression) {
        // write the block into temp memory buffers
        // prepare the metas
        // use deserialize_chunk to read back into block
        bencher
            .with_inputs(|| prepare_format_file(FuseStorageFormat::Native, compression))
            .input_counter(|(a, _)| {
                // Changes based on input.
                BytesCount::usize(a.len())
            })
            .bench_refs(|(a, schema)| {
                let mut seek_a = std::io::Cursor::new(a.clone());
                let metas = databend_common_native::read::reader::read_meta(&mut seek_a).unwrap();

                let reader = NativeColumnsReader::new().unwrap();
                let mut columns = Vec::with_capacity(schema.fields().len());

                for (meta, f) in metas.iter().zip(schema.fields().iter()) {
                    let bs =
                        a.slice(meta.offset as usize..(meta.offset + meta.total_len()) as usize);
                    let col = reader
                        .batch_read_column(vec![bs.as_ref()], f.data_type.clone(), vec![meta
                            .pages
                            .clone()])
                        .unwrap();

                    columns.push(col);
                }
                let datablock = DataBlock::new_from_columns(columns);
                assert_eq!(datablock.num_rows(), NUM_ROWS);
                divan::black_box(datablock);
            });
    }

    fn prepare_format_file(
        storage_format: FuseStorageFormat,
        compression: TableCompression,
    ) -> (Bytes, TableSchemaRef) {
        let (datablock, schema) = read_parquet_file();
        // write the block into temp memory buffers
        let max_page_size = 8192;
        let block_per_seg = 1000;

        let write_settings = WriteSettings {
            storage_format,
            table_compression: compression,
            max_page_size,
            block_per_seg,
        };
        let schema = Arc::new(schema);
        let mut buffer = Vec::new();
        let _ = serialize_block(&write_settings, &schema, datablock, &mut buffer).unwrap();

        (buffer.into(), schema)
    }
}
