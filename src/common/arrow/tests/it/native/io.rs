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

use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::array::BinaryArray;
use databend_common_arrow::arrow::array::BooleanArray;
use databend_common_arrow::arrow::array::Float32Array;
use databend_common_arrow::arrow::array::Float64Array;
use databend_common_arrow::arrow::array::Int16Array;
use databend_common_arrow::arrow::array::Int32Array;
use databend_common_arrow::arrow::array::Int64Array;
use databend_common_arrow::arrow::array::Int8Array;
use databend_common_arrow::arrow::array::ListArray;
use databend_common_arrow::arrow::array::MapArray;
use databend_common_arrow::arrow::array::PrimitiveArray;
use databend_common_arrow::arrow::array::StructArray;
use databend_common_arrow::arrow::array::UInt16Array;
use databend_common_arrow::arrow::array::UInt32Array;
use databend_common_arrow::arrow::array::UInt64Array;
use databend_common_arrow::arrow::array::UInt8Array;
use databend_common_arrow::arrow::array::Utf8Array;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_arrow::arrow::chunk::Chunk;
use databend_common_arrow::arrow::compute;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::arrow::datatypes::Schema;
use databend_common_arrow::arrow::io::parquet::read::n_columns;
use databend_common_arrow::arrow::io::parquet::read::ColumnDescriptor;
use databend_common_arrow::arrow::io::parquet::write::to_parquet_schema;
use databend_common_arrow::arrow::offset::OffsetsBuffer;
use databend_common_arrow::native::read::batch_read::batch_read_array;
use databend_common_arrow::native::read::deserialize::column_iter_to_arrays;
use databend_common_arrow::native::read::reader::is_primitive;
use databend_common_arrow::native::read::reader::NativeReader;
use databend_common_arrow::native::write::NativeWriter;
use databend_common_arrow::native::write::WriteOptions;
use databend_common_arrow::native::ColumnMeta;
use databend_common_arrow::native::CommonCompression;
use databend_common_arrow::native::PageMeta;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

pub const WRITE_PAGE: usize = 2048;
pub const SMALL_WRITE_PAGE: usize = 2;

pub fn new_test_chunk() -> Chunk<Box<dyn Array>> {
    Chunk::new(vec![
        Box::new(BooleanArray::from_slice([
            true, true, true, false, false, false,
        ])) as _,
        Box::new(UInt8Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(UInt16Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(UInt32Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(UInt64Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(Int8Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(Int16Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(Int32Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(Int64Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(Float32Array::from_vec(vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6])) as _,
        Box::new(Float64Array::from_vec(vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6])) as _,
        Box::new(Utf8Array::<i32>::from_iter_values(
            ["abcdefg", "mn", "11", "", "3456", "xyz"].iter(),
        )) as _,
        Box::new(BinaryArray::<i64>::from_iter_values(
            ["abcdefg", "mn", "11", "", "3456", "xyz"].iter(),
        )) as _,
    ])
}

#[test]
fn test_basic() {
    test_write_read(new_test_chunk());
}

#[test]
fn test_random_nonull() {
    let size: usize = 10000;
    let chunk = Chunk::new(vec![
        Box::new(create_random_bool(size, 0.0)) as _,
        Box::new(create_random_index(size, 0.0, size)) as _,
        Box::new(create_random_double(size, 0.0, size)) as _,
        Box::new(create_random_string(size, 0.0, size)) as _,
    ]);
    test_write_read(chunk);
}

#[test]
fn test_random() {
    let size = 10000;
    let chunk = Chunk::new(vec![
        Box::new(create_random_bool(size, 0.1)) as _,
        Box::new(create_random_index(size, 0.1, size)) as _,
        Box::new(create_random_index(size, 0.2, size)) as _,
        Box::new(create_random_index(size, 0.3, size)) as _,
        Box::new(create_random_index(size, 0.4, size)) as _,
        Box::new(create_random_double(size, 0.5, size)) as _,
        Box::new(create_random_string(size, 0.4, size)) as _,
    ]);
    test_write_read(chunk);
}

#[test]
fn test_dict() {
    let size = 10000;
    let chunk = Chunk::new(vec![
        Box::new(create_random_bool(size, 0.1)) as _,
        Box::new(create_random_index(size, 0.1, 8)) as _,
        Box::new(create_random_index(size, 0.2, 8)) as _,
        Box::new(create_random_index(size, 0.3, 8)) as _,
        Box::new(create_random_index(size, 0.4, 8)) as _,
        Box::new(create_random_double(size, 0.5, 8)) as _,
        Box::new(create_random_string(size, 0.4, 8)) as _,
    ]);
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

    let chunk = Chunk::new(vec![Box::new(UInt32Array::from_vec(values)) as _]);
    test_write_read(chunk);
}

#[test]
fn test_bitpacking() {
    let size = WRITE_PAGE * 5;
    let chunk = Chunk::new(vec![
        Box::new(create_random_index(size, 0.1, 8)) as _,
        Box::new(create_random_index(size, 0.5, 8)) as _,
    ]);
    test_write_read(chunk);
}

#[test]
fn test_deleta_bitpacking() {
    let size = WRITE_PAGE * 5;
    let chunk = Chunk::new(vec![
        Box::new(UInt32Array::from_vec((0..size as u32).collect())) as _,
        Box::new(Int32Array::from_vec((0..size as i32).collect())) as _,
    ]);
    test_write_read(chunk);
}

#[test]
fn test_onevalue() {
    let size = 10000;
    let chunk = Chunk::new(vec![
        Box::new(BooleanArray::from_iter((0..size).map(|_| Some(true)))) as _,
        Box::new(BooleanArray::from_iter((0..size).map(|_| Some(false)))) as _,
        Box::new(UInt32Array::from_vec(vec![3; size])) as _,
        Box::new(create_random_index(size, 0.3, 1)) as _,
        Box::new(create_random_string(size, 0.4, 1)) as _,
    ]);
    test_write_read(chunk);
}

#[test]
fn test_struct() {
    let struct_array = create_struct(1000, 0.2, 1000);
    let chunk = Chunk::new(vec![Box::new(struct_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_float() {
    let size = 1000;
    let chunk = Chunk::new(vec![Box::new(create_random_double(size, 0.5, size)) as _]);
    test_write_read(chunk);
}

#[test]
fn test_list() {
    let list_array = create_list(1000, 0.2);
    let chunk = Chunk::new(vec![Box::new(list_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_map() {
    let map_array = create_map(1000, 0.2);
    let chunk = Chunk::new(vec![Box::new(map_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_list_list() {
    let l1 = create_list(2000, 0.2);

    let mut offsets = vec![];
    for i in (0..=1000).step_by(2) {
        offsets.push(i);
    }
    let list_array = ListArray::try_new(
        DataType::List(Box::new(Field::new("item", l1.data_type().clone(), true))),
        OffsetsBuffer::try_from(offsets).unwrap(),
        l1.boxed(),
        None,
    )
    .unwrap();

    let chunk = Chunk::new(vec![Box::new(list_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_list_struct() {
    let s1 = create_struct(2000, 0.2, 2000);

    let mut offsets = vec![];
    for i in (0..=1000).step_by(2) {
        offsets.push(i);
    }
    let list_array = ListArray::try_new(
        DataType::List(Box::new(Field::new("item", s1.data_type().clone(), true))),
        OffsetsBuffer::try_from(offsets).unwrap(),
        s1.boxed(),
        None,
    )
    .unwrap();

    let chunk = Chunk::new(vec![Box::new(list_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_list_map() {
    let m1 = create_map(2000, 0.2);

    let mut offsets = vec![];
    for i in (0..=1000).step_by(2) {
        offsets.push(i);
    }
    let list_array = ListArray::try_new(
        DataType::List(Box::new(Field::new("item", m1.data_type().clone(), true))),
        OffsetsBuffer::try_from(offsets).unwrap(),
        m1.boxed(),
        None,
    )
    .unwrap();

    let chunk = Chunk::new(vec![Box::new(list_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_struct_list() {
    let size = 10000;
    let null_density = 0.2;
    let dt = DataType::Struct(vec![
        Field::new("name", DataType::LargeBinary, true),
        Field::new(
            "age",
            DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
    ]);
    let struct_array = StructArray::try_new(
        dt,
        vec![
            Box::new(create_random_string(size, null_density, size)) as _,
            Box::new(create_list(size, null_density)) as _,
        ],
        None,
    )
    .unwrap();
    let chunk = Chunk::new(vec![Box::new(struct_array) as _]);
    test_write_read(chunk);
}

fn create_list(size: usize, null_density: f32) -> ListArray<i32> {
    let (offsets, bitmap) = create_random_offsets(size, 0.1);
    let length = *offsets.last().unwrap() as usize;
    let l1 = create_random_index(length, null_density, length);

    ListArray::try_new(
        DataType::List(Box::new(Field::new("item", l1.data_type().clone(), true))),
        OffsetsBuffer::try_from(offsets).unwrap(),
        l1.boxed(),
        bitmap,
    )
    .unwrap()
}

fn create_map(size: usize, null_density: f32) -> MapArray {
    let (offsets, bitmap) = create_random_offsets(size, 0.1);
    let length = *offsets.last().unwrap() as usize;
    let dt = DataType::Struct(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", DataType::LargeBinary, true),
    ]);
    let struct_array = StructArray::try_new(
        dt,
        vec![
            Box::new(create_random_index(length, 0.0, length)) as _,
            Box::new(create_random_string(length, null_density, length)) as _,
        ],
        None,
    )
    .unwrap();

    MapArray::try_new(
        DataType::Map(
            Box::new(Field::new(
                "entries",
                struct_array.data_type().clone(),
                false,
            )),
            false,
        ),
        OffsetsBuffer::try_from(offsets).unwrap(),
        struct_array.boxed(),
        bitmap,
    )
    .unwrap()
}

fn create_struct(size: usize, null_density: f32, uniq: usize) -> StructArray {
    let dt = DataType::Struct(vec![
        Field::new("name", DataType::LargeBinary, true),
        Field::new("age", DataType::Int32, true),
    ]);
    StructArray::try_new(
        dt,
        vec![
            Box::new(create_random_string(size, null_density, uniq)) as _,
            Box::new(create_random_index(size, null_density, uniq)) as _,
        ],
        None,
    )
    .unwrap()
}

fn create_random_bool(size: usize, null_density: f32) -> BooleanArray {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() > null_density {
                let value = rng.gen::<bool>();
                Some(value)
            } else {
                None
            }
        })
        .collect::<BooleanArray>()
}

fn create_random_index(size: usize, null_density: f32, uniq: usize) -> PrimitiveArray<i32> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() > null_density {
                let value = rng.gen_range::<i32, _>(0i32..uniq as i32);
                Some(value)
            } else {
                None
            }
        })
        .collect::<PrimitiveArray<i32>>()
}

fn create_random_double(size: usize, null_density: f32, uniq: usize) -> PrimitiveArray<f64> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() > null_density {
                let value = rng.gen_range::<i32, _>(0i32..uniq as i32);
                Some(value as f64)
            } else {
                None
            }
        })
        .collect::<PrimitiveArray<f64>>()
}

fn create_random_string(size: usize, null_density: f32, uniq: usize) -> BinaryArray<i64> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() > null_density {
                let value = rng.gen_range::<i32, _>(0i32..uniq as i32);
                Some(format!("{value}"))
            } else {
                None
            }
        })
        .collect::<BinaryArray<i64>>()
}

fn create_random_offsets(size: usize, null_density: f32) -> (Vec<i32>, Option<Bitmap>) {
    let mut offsets = Vec::with_capacity(size + 1);
    offsets.push(0i32);
    let mut builder = MutableBitmap::with_capacity(size);
    let mut rng = StdRng::seed_from_u64(42);
    for _ in 0..size {
        if rng.gen::<f32>() > null_density {
            let offset = rng.gen_range::<i32, _>(0i32..3i32);
            offsets.push(*offsets.last().unwrap() + offset);
            builder.push(true);
        } else {
            offsets.push(*offsets.last().unwrap());
            builder.push(false);
        }
    }
    (offsets, builder.into())
}

fn test_write_read(chunk: Chunk<Box<dyn Array>>) {
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

fn test_write_read_with_options(chunk: Chunk<Box<dyn Array>>, options: WriteOptions) {
    let mut bytes = Vec::new();
    let fields: Vec<Field> = chunk
        .iter()
        .map(|array| {
            Field::new(
                "name",
                array.data_type().clone(),
                array.validity().is_some(),
            )
        })
        .collect();

    let schema = Schema::from(fields);
    let mut writer = NativeWriter::new(&mut bytes, schema.clone(), options);

    writer.start().unwrap();
    writer.write(&chunk).unwrap();
    writer.finish().unwrap();

    log::info!("write finished, start to read");

    let mut batch_metas = writer.metas.clone();
    let mut metas = writer.metas.clone();
    let schema_descriptor = to_parquet_schema(&schema).unwrap();
    let mut leaves = schema_descriptor.columns().to_vec();
    let mut results = Vec::with_capacity(schema.fields.len());
    for field in schema.fields.iter() {
        let n = n_columns(&field.data_type);

        let curr_metas: Vec<ColumnMeta> = metas.drain(..n).collect();
        let curr_leaves: Vec<ColumnDescriptor> = leaves.drain(..n).collect();

        let mut native_readers = Vec::with_capacity(n);
        for curr_meta in curr_metas.iter() {
            let mut range_bytes = std::io::Cursor::new(bytes.clone());
            range_bytes.consume(curr_meta.offset as usize);

            let native_reader = NativeReader::new(range_bytes, curr_meta.pages.clone(), vec![]);
            native_readers.push(native_reader);
        }
        let is_nested = !is_primitive(field.data_type());

        let mut array_iter =
            column_iter_to_arrays(native_readers, curr_leaves, field.clone(), is_nested).unwrap();

        let mut arrays = vec![];
        for array in array_iter.by_ref() {
            arrays.push(array.unwrap().to_boxed());
        }
        let arrays: Vec<&dyn Array> = arrays.iter().map(|v| v.as_ref()).collect();
        let result = compute::concatenate::concatenate(&arrays).unwrap();
        results.push(result);
    }
    let result_chunk = Chunk::new(results);

    assert_eq!(chunk, result_chunk);

    // test batch read
    let schema_descriptor = to_parquet_schema(&schema).unwrap();
    let mut leaves = schema_descriptor.columns().to_vec();
    let mut batch_results = Vec::with_capacity(schema.fields.len());
    for field in schema.fields.iter() {
        let n = n_columns(&field.data_type);

        let curr_metas: Vec<ColumnMeta> = batch_metas.drain(..n).collect();
        let curr_leaves: Vec<ColumnDescriptor> = leaves.drain(..n).collect();

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
        let is_nested = !is_primitive(field.data_type());
        let batch_result =
            batch_read_array(readers, curr_leaves, field.clone(), is_nested, pages).unwrap();
        batch_results.push(batch_result);
    }
    let batch_result_chunk = Chunk::new(batch_results);

    assert_eq!(chunk, batch_result_chunk);
}
