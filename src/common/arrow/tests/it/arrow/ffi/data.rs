// Copyright 2020-2022 Jorge C. Leitão
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

use std::collections::BTreeMap;

use databend_common_arrow::arrow::array::*;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::arrow::datatypes::TimeUnit;
use databend_common_arrow::arrow::error::Result;
use databend_common_arrow::arrow::ffi;

fn _test_round_trip(array: Box<dyn Array>, expected: Box<dyn Array>) -> Result<()> {
    let field = Field::new("a", array.data_type().clone(), true);

    // export array and corresponding data_type
    let array_ffi = ffi::export_array_to_c(array);
    let schema_ffi = ffi::export_field_to_c(&field);

    // import references
    let result_field = unsafe { ffi::import_field_from_c(&schema_ffi)? };
    let result_array =
        unsafe { ffi::import_array_from_c(array_ffi, result_field.data_type.clone())? };

    assert_eq!(&result_array, &expected);
    assert_eq!(result_field, field);
    Ok(())
}

fn test_round_trip(expected: impl Array + Clone + 'static) -> Result<()> {
    let array: Box<dyn Array> = Box::new(expected.clone());
    let expected = Box::new(expected) as Box<dyn Array>;
    _test_round_trip(array.clone(), clone(expected.as_ref()))?;

    // sliced
    _test_round_trip(array.sliced(1, 2), expected.sliced(1, 2))
}

fn test_round_trip_schema(field: Field) -> Result<()> {
    let schema_ffi = ffi::export_field_to_c(&field);

    let result = unsafe { ffi::import_field_from_c(&schema_ffi)? };

    assert_eq!(result, field);
    Ok(())
}

#[test]
fn bool_nullable() -> Result<()> {
    let data = BooleanArray::from(&[Some(true), None, Some(false), None]);
    test_round_trip(data)
}

#[test]
fn bool() -> Result<()> {
    let data = BooleanArray::from_slice([true, true, false]);
    test_round_trip(data)
}

#[test]
fn bool_nullable_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).sliced(1, 3);
    let data = BooleanArray::try_new(DataType::Boolean, [true, true, false].into(), Some(bitmap))?;
    test_round_trip(data)
}

#[test]
fn u32_nullable() -> Result<()> {
    let data = Int32Array::from(&[Some(2), None, Some(1), None]);
    test_round_trip(data)
}

#[test]
fn u32() -> Result<()> {
    let data = Int32Array::from_slice([2, 0, 1, 0]);
    test_round_trip(data)
}

#[test]
fn u32_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).sliced(1, 3);
    let data = Int32Array::try_new(DataType::Int32, vec![1, 2, 3].into(), Some(bitmap))?;
    test_round_trip(data)
}

#[test]
fn decimal() -> Result<()> {
    let data = Int128Array::from_slice([1, 0, 2, 0]);
    test_round_trip(data)
}

#[test]
fn decimal_nullable() -> Result<()> {
    let data = Int128Array::from(&[Some(1), None, Some(2), None]);
    test_round_trip(data)
}

#[test]
fn timestamp_tz() -> Result<()> {
    let data = Int64Array::from(&vec![Some(2), None, None]).to(DataType::Timestamp(
        TimeUnit::Second,
        Some("UTC".to_string()),
    ));
    test_round_trip(data)
}

#[test]
fn utf8_nullable() -> Result<()> {
    let data = Utf8Array::<i32>::from([Some("a"), None, Some("bb"), None]);
    test_round_trip(data)
}

#[test]
fn utf8() -> Result<()> {
    let data = Utf8Array::<i32>::from_slice(["a", "", "bb", ""]);
    test_round_trip(data)
}

#[test]
fn utf8_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).sliced(1, 3);
    let data = Utf8Array::<i32>::try_new(
        DataType::Utf8,
        vec![0, 1, 1, 2].try_into().unwrap(),
        b"ab".to_vec().into(),
        Some(bitmap),
    )?;
    test_round_trip(data)
}

#[test]
fn large_utf8() -> Result<()> {
    let data = Utf8Array::<i64>::from([Some("a"), None, Some("bb"), None]);
    test_round_trip(data)
}

#[test]
fn binary_nullable() -> Result<()> {
    let data = BinaryArray::<i32>::from([Some(b"a".as_ref()), None, Some(b"bb".as_ref()), None]);
    test_round_trip(data)
}

#[test]
fn binary() -> Result<()> {
    let data = BinaryArray::<i32>::from_slice([b"a".as_ref(), b"", b"bb", b""]);
    test_round_trip(data)
}

#[test]
fn binary_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).sliced(1, 3);
    let data = BinaryArray::<i32>::try_new(
        DataType::Binary,
        vec![0, 1, 1, 2].try_into().unwrap(),
        b"ab".to_vec().into(),
        Some(bitmap),
    )?;
    test_round_trip(data)
}

#[test]
fn large_binary() -> Result<()> {
    let data = BinaryArray::<i64>::from([Some(b"a".as_ref()), None, Some(b"bb".as_ref()), None]);
    test_round_trip(data)
}

#[test]
fn fixed_size_binary() -> Result<()> {
    let data = FixedSizeBinaryArray::new(
        DataType::FixedSizeBinary(2),
        vec![1, 2, 3, 4, 5, 6].into(),
        None,
    );
    test_round_trip(data)
}

#[test]
fn fixed_size_binary_nullable() -> Result<()> {
    let data = FixedSizeBinaryArray::new(
        DataType::FixedSizeBinary(2),
        vec![1, 2, 3, 4, 5, 6].into(),
        Some([true, true, false].into()),
    );
    test_round_trip(data)
}

#[test]
fn fixed_size_binary_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).sliced(1, 3);
    let data = FixedSizeBinaryArray::try_new(
        DataType::FixedSizeBinary(2),
        b"ababab".to_vec().into(),
        Some(bitmap),
    )?;
    test_round_trip(data)
}

#[test]
fn list() -> Result<()> {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data)?;

    let array: ListArray<i32> = array.into();

    test_round_trip(array)
}

#[test]
fn list_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).sliced(1, 3);

    let array = ListArray::<i32>::try_new(
        DataType::List(Box::new(Field::new("a", DataType::Int32, true))),
        vec![0, 1, 1, 2].try_into().unwrap(),
        Box::new(PrimitiveArray::<i32>::from_slice([1, 2])),
        Some(bitmap),
    )?;

    test_round_trip(array)
}

#[test]
fn large_list() -> Result<()> {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableListArray::<i64, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data)?;

    let array: ListArray<i64> = array.into();

    test_round_trip(array)
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

    test_round_trip(array)
}

#[test]
fn fixed_size_list_sliced() -> Result<()> {
    let bitmap = Bitmap::from([true, false, false, true]).sliced(1, 3);

    let array = FixedSizeListArray::try_new(
        DataType::FixedSizeList(Box::new(Field::new("a", DataType::Int32, true)), 2),
        Box::new(PrimitiveArray::<i32>::from_vec(vec![1, 2, 3, 4, 5, 6])),
        Some(bitmap),
    )?;

    test_round_trip(array)
}

#[test]
fn list_list() -> Result<()> {
    let data = vec![
        Some(vec![
            Some(vec![None]),
            Some(vec![Some(2)]),
            Some(vec![Some(3)]),
        ]),
        None,
        Some(vec![Some(vec![Some(4), None, Some(6)])]),
    ];

    let mut array =
        MutableListArray::<i32, MutableListArray<i32, MutablePrimitiveArray<i32>>>::new();
    array.try_extend(data)?;

    let array: ListArray<i32> = array.into();

    test_round_trip(array)
}

#[test]
fn struct_() -> Result<()> {
    let data_type = DataType::Struct(vec![Field::new("a", DataType::Int32, true)]);
    let values = vec![Int32Array::from([Some(1), None, Some(3)]).boxed()];
    let validity = Bitmap::from([true, false, true]);

    let array = StructArray::new(data_type, values, validity.into());

    test_round_trip(array)
}

#[test]
fn dict() -> Result<()> {
    let data = vec![Some("a"), Some("a"), None, Some("b")];

    let mut array = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    array.try_extend(data)?;

    let array: DictionaryArray<i32> = array.into();

    test_round_trip(array)
}

#[test]
fn schema() -> Result<()> {
    let field = Field::new(
        "a",
        DataType::List(Box::new(Field::new("a", DataType::UInt32, true))),
        true,
    );
    test_round_trip_schema(field)?;

    let field = Field::new(
        "a",
        DataType::Dictionary(u32::KEY_TYPE, Box::new(DataType::Utf8), false),
        true,
    );
    test_round_trip_schema(field)?;

    let field = Field::new("a", DataType::Int32, true);
    let mut metadata = BTreeMap::new();
    metadata.insert("some".to_string(), "stuff".to_string());
    let field = field.with_metadata(metadata);
    test_round_trip_schema(field)
}

#[test]
fn extension() -> Result<()> {
    let field = Field::new(
        "a",
        DataType::Extension(
            "a".to_string(),
            Box::new(DataType::Int32),
            Some("bla".to_string()),
        ),
        true,
    );
    test_round_trip_schema(field)
}

#[test]
fn extension_children() -> Result<()> {
    let field = Field::new(
        "a",
        DataType::Extension(
            "b".to_string(),
            Box::new(DataType::Struct(vec![Field::new(
                "c",
                DataType::Int32,
                true,
            )])),
            None,
        ),
        true,
    );
    test_round_trip_schema(field)
}
