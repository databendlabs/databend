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

use std::io::BufRead;
use std::io::BufReader;

use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::*;
use databend_common_native::ColumnMeta;
use databend_common_native::CommonCompression;
use databend_common_native::PageMeta;
use databend_common_native::n_columns;
use databend_common_native::read::batch_read::batch_read_column;
use databend_common_native::read::deserialize::column_iters;
use databend_common_native::read::reader::NativeReader;
use databend_common_native::write::NativeWriter;
use databend_common_native::write::WriteOptions;

pub const WRITE_PAGE: usize = 2048;
pub const SMALL_WRITE_PAGE: usize = 2;

pub fn new_test_column() -> Vec<Column> {
    vec![
        UInt8Type::from_data(vec![1, 2, 3, 4, 5, 6]),
        BooleanType::from_data(vec![true, true, true, false, false, false]),
        UInt16Type::from_data(vec![1, 2, 3, 4, 5, 6]),
        UInt32Type::from_data(vec![1, 2, 3, 4, 5, 6]),
        UInt64Type::from_data(vec![1, 2, 3, 4, 5, 6]),
        Int8Type::from_data(vec![1, 2, 3, 4, 5, 6]),
        Int16Type::from_data(vec![1, 2, 3, 4, 5, 6]),
        Int32Type::from_data(vec![1, 2, 3, 4, 5, 6]),
        Int64Type::from_data(vec![1, 2, 3, 4, 5, 6]),
        Float32Type::from_data(vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6]),
        Float64Type::from_data(vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6]),
        Column::String(StringColumn::from_iter(
            ["abcdefg", "mn", "11", "", "3456", "xyz"].iter(),
        )),
        Column::Binary(BinaryColumn::from_iter(
            ["abcdefg", "mn", "11", "", "3456", "xyz"].iter(),
        )),
        // use binary jsonb format values to test
        Column::Variant(BinaryColumn::from_iter(
            [
                // null
                vec![0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
                // "abc"
                vec![
                    0x20, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x03, 0x61, 0x62, 0x63,
                ],
                // 123
                vec![0x20, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x02, 0x50, 0x7B],
                // []
                vec![0x80, 0x00, 0x00, 0x00],
                // [1,2]
                vec![
                    0x80, 0x00, 0x00, 0x02, 0x20, 0x00, 0x00, 0x02, 0x20, 0x00, 0x00, 0x02, 0x50,
                    0x01, 0x50, 0x02,
                ],
                // {"k":"v"}
                vec![
                    0x40, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00, 0x01, 0x6B,
                    0x76,
                ],
            ]
            .iter(),
        )),
    ]
}

#[test]
fn test_basic() {
    test_write_read(new_test_column());
}

#[test]
fn test_random() {
    let size = 1000;
    let chunk = rand_columns_for_all_types(size);
    test_write_read(chunk);
}

#[test]
fn test_freq() {
    let size = WRITE_PAGE * 5;
    let mut values: Vec<u32> = Vec::with_capacity(size);
    for _ in 0..5 {
        values.extend_from_slice(&vec![20; WRITE_PAGE - 3]);
        values.push(10000);
        values.push(10000);
        values.push(10000);
    }

    let chunk = vec![UInt32Type::from_data(values)];
    test_write_read(chunk);
}

#[test]
fn test_dict_string() {
    let size = WRITE_PAGE * 5;

    let values = vec![
        (0..size)
            .map(|s| format!("stringxxxxxxxx{}", s % 4))
            .collect::<Vec<_>>(),
        // (0..size)
        //     .map(|s| format!("abc{}", s % 4))
        //     .collect::<Vec<_>>(),
    ];
    for v in values {
        let col = StringColumn::from_iter(v.iter());
        let chunk = vec![Column::String(col)];
        test_write_read(chunk);
    }
}

#[test]
fn test_bitpacking() {
    let size = WRITE_PAGE * 5;
    let chunk = vec![
        Column::random(&DataType::Number(NumberDataType::Int32), size, None),
        Column::random(&DataType::Number(NumberDataType::Int32), size, None),
    ];
    test_write_read(chunk);
}

#[test]
fn test_deleta_bitpacking() {
    let size = WRITE_PAGE * 5;
    let chunk = vec![
        UInt32Type::from_data((0..size as u32).collect()),
        Int32Type::from_data((0..size as i32).collect()),
    ];
    test_write_read(chunk);
}

#[test]
fn test_onevalue() {
    let size = 10000;
    let chunk = vec![
        BooleanType::from_data((0..size).map(|_| true).collect()),
        BooleanType::from_data((0..size).map(|_| false).collect()),
        UInt32Type::from_data(vec![3; size]),
    ];
    test_write_read(chunk);
}

fn test_write_read(chunk: Vec<Column>) {
    let _ = env_logger::try_init();

    let compressions = vec![
        CommonCompression::Lz4,
        CommonCompression::Zstd,
        CommonCompression::Snappy,
        CommonCompression::None,
    ];
    let page_sizes = vec![WRITE_PAGE, SMALL_WRITE_PAGE];

    for compression in compressions {
        for page_size in &page_sizes {
            test_write_read_with_options(chunk.clone(), WriteOptions {
                default_compression: compression,
                max_page_size: Some(*page_size),
                default_compress_ratio: Some(2.0f64),
                forbidden_compressions: vec![],
            });
        }
    }
}

fn test_write_read_with_options(chunk: Vec<Column>, options: WriteOptions) {
    let mut bytes = Vec::new();
    let fields: Vec<TableField> = chunk
        .iter()
        .map(|col| TableField::new("name", infer_schema_type(&col.data_type()).unwrap()))
        .collect();

    let schema = TableSchema::new(fields);
    let mut writer = NativeWriter::new(&mut bytes, schema.clone(), options).unwrap();

    writer.start().unwrap();
    writer.write(&chunk).unwrap();
    writer.finish().unwrap();

    log::info!("write finished, start to read");

    let mut batch_metas = writer.metas.clone();

    let mut metas = writer.metas.clone();

    for (field, want) in schema.fields.iter().zip(chunk.iter()) {
        let n = n_columns(&field.data_type);

        let curr_metas: Vec<ColumnMeta> = metas.drain(..n).collect();

        let mut native_readers = Vec::with_capacity(n);
        for curr_meta in curr_metas.iter() {
            let mut range_bytes = std::io::Cursor::new(bytes.clone());
            range_bytes.consume(curr_meta.offset as usize);

            let native_reader = NativeReader::new(range_bytes, curr_meta.pages.clone(), vec![]);
            native_readers.push(native_reader);
        }

        let mut col_iter = column_iters(native_readers, field.clone(), vec![]).unwrap();

        let mut cols = vec![];
        for col in col_iter.by_ref() {
            cols.push(col.unwrap());
        }
        let result = Column::concat_columns(cols.into_iter()).unwrap();
        assert_eq!(want, &result);
    }

    // test batch read
    for (field, want) in schema.fields.iter().zip(chunk.iter()) {
        let n = n_columns(&field.data_type);

        let curr_metas: Vec<ColumnMeta> = batch_metas.drain(..n).collect();

        let mut pages: Vec<Vec<PageMeta>> = Vec::with_capacity(n);
        let mut readers = Vec::with_capacity(n);
        for curr_meta in curr_metas.iter() {
            pages.push(curr_meta.pages.clone());
            let mut reader = std::io::Cursor::new(bytes.clone());
            reader.consume(curr_meta.offset as usize);

            let buffer_size = curr_meta.total_len().min(8192) as usize;
            let reader = BufReader::with_capacity(buffer_size, reader);

            readers.push(reader);
        }
        let batch_result = batch_read_column(readers, field.data_type().clone(), pages).unwrap();
        assert_eq!(want, &batch_result);
    }
}

fn get_all_test_data_types() -> Vec<DataType> {
    vec![
        DataType::Boolean,
        DataType::Binary,
        DataType::String,
        DataType::Bitmap,
        DataType::Variant,
        DataType::Timestamp,
        DataType::Date,
        DataType::Number(NumberDataType::UInt8),
        DataType::Number(NumberDataType::UInt16),
        DataType::Number(NumberDataType::UInt32),
        DataType::Number(NumberDataType::UInt64),
        DataType::Number(NumberDataType::Int8),
        DataType::Number(NumberDataType::Int16),
        DataType::Number(NumberDataType::Int32),
        DataType::Number(NumberDataType::Int64),
        DataType::Number(NumberDataType::Float32),
        DataType::Number(NumberDataType::Float64),
        DataType::Decimal(DecimalSize::new_unchecked(10, 2)),
        DataType::Decimal(DecimalSize::new_unchecked(35, 3)),
        DataType::Decimal(DecimalSize::new_unchecked(55, 3)),
        DataType::Nullable(Box::new(DataType::Geography)),
        DataType::Nullable(Box::new(DataType::Geometry)),
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Nullable(Box::new(DataType::String)),
        DataType::Nullable(Box::new(DataType::Variant)),
        DataType::Nullable(Box::new(DataType::Binary)),
        DataType::Array(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Array(Box::new(
            DataType::Number(NumberDataType::UInt32).wrap_nullable(),
        )),
        DataType::Nullable(Box::new(DataType::Array(Box::new(DataType::Number(
            NumberDataType::UInt32,
        ))))),
        DataType::Map(Box::new(DataType::Tuple(vec![
            DataType::Number(NumberDataType::UInt64),
            DataType::String,
        ]))),
        DataType::Array(Box::new(DataType::Array(Box::new(DataType::Number(
            NumberDataType::UInt32,
        ))))),
        DataType::Array(Box::new(DataType::Map(Box::new(DataType::Tuple(vec![
            DataType::Number(NumberDataType::UInt64),
            DataType::String,
        ]))))),
        DataType::Tuple(vec![
            DataType::Tuple(vec![
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Float64),
            ])
            .wrap_nullable(),
            DataType::Tuple(vec![
                DataType::Number(NumberDataType::Int64),
                DataType::Number(NumberDataType::Int64),
            ])
            .wrap_nullable(),
        ]),
    ]
}

fn rand_columns_for_all_types(num_rows: usize) -> Vec<Column> {
    let types = get_all_test_data_types();
    let mut columns = Vec::with_capacity(types.len());
    for data_type in types.iter() {
        columns.push(Column::random(data_type, num_rows, None));
    }

    columns
}
