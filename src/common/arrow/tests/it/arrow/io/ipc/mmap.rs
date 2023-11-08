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

use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::error::Result;
use common_arrow::arrow::io::ipc::read::read_file_metadata;

use super::write::file::write;

fn round_trip(array: Box<dyn Array>) -> Result<()> {
    let schema = Schema::from(vec![Field::new("a", array.data_type().clone(), true)]);
    let columns = Chunk::try_new(vec![array.clone()])?;

    let data = Arc::new(write(&[columns], &schema, None, None)?);

    let metadata = read_file_metadata(&mut std::io::Cursor::new(data.as_ref()))?;

    let dictionaries =
        unsafe { arrow2::mmap::mmap_dictionaries_unchecked(&metadata, data.clone())? };

    let new_array = unsafe { arrow2::mmap::mmap_unchecked(&metadata, &dictionaries, data, 0)? };
    assert_eq!(new_array.into_arrays()[0], array);
    Ok(())
}

#[test]
fn utf8() -> Result<()> {
    let array = Utf8Array::<i32>::from([None, None, Some("bb")])
        .sliced(1, 2)
        .boxed();
    round_trip(array)
}

#[test]
fn fixed_size_binary() -> Result<()> {
    let array = FixedSizeBinaryArray::from([None, None, Some([1, 2])])
        .boxed()
        .sliced(1, 2);
    round_trip(array)?;

    let array = FixedSizeBinaryArray::new_empty(DataType::FixedSizeBinary(20)).boxed();
    round_trip(array)
}

#[test]
fn primitive() -> Result<()> {
    let array = PrimitiveArray::<i32>::from([None, None, Some(3)])
        .boxed()
        .sliced(1, 2);
    round_trip(array)
}

#[test]
fn boolean() -> Result<()> {
    let array = BooleanArray::from([None, None, Some(true)])
        .boxed()
        .sliced(1, 2);
    round_trip(array)
}

#[test]
fn null() -> Result<()> {
    let array = NullArray::new(DataType::Null, 10).boxed();
    round_trip(array)
}

#[test]
fn fixed_size_list() -> Result<()> {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableFixedSizeListArray::new(MutablePrimitiveArray::<i32>::new(), 3);
    array.try_extend(data)?;

    let array: FixedSizeListArray = array.into();
    round_trip(array.sliced(1, 2).boxed())
}

#[test]
fn list() -> Result<()> {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data).unwrap();
    let array = array.into_box().sliced(1, 2);
    round_trip(array)
}

#[test]
fn struct_() -> Result<()> {
    let array = PrimitiveArray::<i32>::from([None, None, None, Some(3), Some(4)]).boxed();

    let array = StructArray::new(
        DataType::Struct(vec![Field::new("f1", array.data_type().clone(), true)]),
        vec![array],
        Some([true, true, false, true, false].into()),
    )
    .boxed()
    .sliced(1, 4);

    round_trip(array)
}

#[test]
fn dict() -> Result<()> {
    let keys = PrimitiveArray::<i32>::from([None, None, None, Some(3), Some(4)]);

    let values = PrimitiveArray::<i32>::from_slice([0, 1, 2, 3, 4, 5]).boxed();

    let array = DictionaryArray::try_from_keys(keys, values)?
        .boxed()
        .sliced(1, 4);

    round_trip(array)
}
