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

use databend_common_arrow::arrow::array::growable::Growable;
use databend_common_arrow::arrow::array::growable::GrowableStruct;
use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::array::PrimitiveArray;
use databend_common_arrow::arrow::array::StructArray;
use databend_common_arrow::arrow::array::Utf8Array;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::datatypes::Field;

fn some_values() -> (DataType, Vec<Box<dyn Array>>) {
    let strings: Box<dyn Array> = Box::new(Utf8Array::<i32>::from([
        Some("a"),
        Some("aa"),
        None,
        Some("mark"),
        Some("doe"),
    ]));
    let ints: Box<dyn Array> = Box::new(PrimitiveArray::<i32>::from(&[
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));
    let fields = vec![
        Field::new("f1", DataType::Utf8, true),
        Field::new("f2", DataType::Int32, true),
    ];
    (DataType::Struct(fields), vec![strings, ints])
}

#[test]
fn basic() {
    let (fields, values) = some_values();

    let array = StructArray::new(fields.clone(), values.clone(), None);

    let mut a = GrowableStruct::new(vec![&array], false, 0);

    a.extend(0, 1, 2);
    assert_eq!(a.len(), 2);
    let result: StructArray = a.into();

    let expected = StructArray::new(
        fields,
        vec![values[0].sliced(1, 2), values[1].sliced(1, 2)],
        None,
    );
    assert_eq!(result, expected)
}

#[test]
fn offset() {
    let (fields, values) = some_values();

    let array = StructArray::new(fields.clone(), values.clone(), None).sliced(1, 3);

    let mut a = GrowableStruct::new(vec![&array], false, 0);

    a.extend(0, 1, 2);
    assert_eq!(a.len(), 2);
    let result: StructArray = a.into();

    let expected = StructArray::new(
        fields,
        vec![values[0].sliced(2, 2), values[1].sliced(2, 2)],
        None,
    );

    assert_eq!(result, expected);
}

#[test]
fn nulls() {
    let (fields, values) = some_values();

    let array = StructArray::new(
        fields.clone(),
        values.clone(),
        Some(Bitmap::from_u8_slice([0b00000010], 5)),
    );

    let mut a = GrowableStruct::new(vec![&array], false, 0);

    a.extend(0, 1, 2);
    assert_eq!(a.len(), 2);
    let result: StructArray = a.into();

    let expected = StructArray::new(
        fields,
        vec![values[0].sliced(1, 2), values[1].sliced(1, 2)],
        Some(Bitmap::from_u8_slice([0b00000010], 5).sliced(1, 2)),
    );

    assert_eq!(result, expected)
}

#[test]
fn many() {
    let (fields, values) = some_values();

    let array = StructArray::new(fields.clone(), values.clone(), None);

    let mut mutable = GrowableStruct::new(vec![&array, &array], true, 0);

    mutable.extend(0, 1, 2);
    mutable.extend(1, 0, 2);
    mutable.extend_validity(1);
    assert_eq!(mutable.len(), 5);
    let result = mutable.as_box();

    let expected_string: Box<dyn Array> = Box::new(Utf8Array::<i32>::from([
        Some("aa"),
        None,
        Some("a"),
        Some("aa"),
        None,
    ]));
    let expected_int: Box<dyn Array> = Box::new(PrimitiveArray::<i32>::from(vec![
        Some(2),
        Some(3),
        Some(1),
        Some(2),
        None,
    ]));

    let expected = StructArray::new(
        fields,
        vec![expected_string, expected_int],
        Some(Bitmap::from([true, true, true, true, false])),
    );
    assert_eq!(expected, result.as_ref())
}
