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
use databend_common_arrow::arrow::array::growable::GrowableUnion;
use databend_common_arrow::arrow::array::*;
use databend_common_arrow::arrow::datatypes::*;
use databend_common_arrow::arrow::error::Result;

#[test]
fn sparse() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let data_type = DataType::Union(fields, None, UnionMode::Sparse);
    let types = vec![0, 0, 1].into();
    let fields = vec![
        Int32Array::from(&[Some(1), None, Some(2)]).boxed(),
        Utf8Array::<i32>::from([Some("a"), Some("b"), Some("c")]).boxed(),
    ];
    let array = UnionArray::new(data_type, types, fields, None);

    for length in 1..2 {
        for index in 0..(array.len() - length + 1) {
            let mut a = GrowableUnion::new(vec![&array], 10);

            a.extend(0, index, length);
            assert_eq!(a.len(), length);

            let expected = array.clone().sliced(index, length);

            let result: UnionArray = a.into();

            assert_eq!(expected, result);
        }
    }

    Ok(())
}

#[test]
fn dense() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let data_type = DataType::Union(fields, None, UnionMode::Dense);
    let types = vec![0, 0, 1].into();
    let fields = vec![
        Int32Array::from(&[Some(1), None, Some(2)]).boxed(),
        Utf8Array::<i32>::from([Some("c")]).boxed(),
    ];
    let offsets = Some(vec![0, 1, 0].into());

    let array = UnionArray::new(data_type, types, fields, offsets);

    for length in 1..2 {
        for index in 0..(array.len() - length + 1) {
            let mut a = GrowableUnion::new(vec![&array], 10);

            a.extend(0, index, length);
            assert_eq!(a.len(), length);
            let expected = array.clone().sliced(index, length);

            let result: UnionArray = a.into();

            assert_eq!(expected, result);
        }
    }

    Ok(())
}

#[test]
fn complex_dense() -> Result<()> {
    let fixed_size_type =
        DataType::FixedSizeList(Box::new(Field::new("i", DataType::UInt16, true)), 3);

    let fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", fixed_size_type.clone(), true),
    ];

    let data_type = DataType::Union(fields, None, UnionMode::Dense);

    // UnionArray[1, [11, 12, 13], abcd, [21, 22, 23], 2]
    let types = vec![0, 2, 1, 2, 0].into();
    let fields = vec![
        Int32Array::from(&[Some(1), Some(2)]).boxed(),
        Utf8Array::<i32>::from([Some("abcd")]).boxed(),
        FixedSizeListArray::try_new(
            fixed_size_type.clone(),
            UInt16Array::from_iter([11, 12, 13, 21, 22, 23].into_iter().map(Some)).boxed(),
            None,
        )
        .unwrap()
        .boxed(),
    ];
    let offsets = Some(vec![0, 0, 0, 1, 1].into());

    let array1 = UnionArray::new(data_type.clone(), types, fields, offsets);

    // UnionArray[[31, 32, 33], [41, 42, 43], ef, ghijk, 3]
    let types = vec![2, 2, 1, 1, 0].into();
    let fields = vec![
        Int32Array::from(&[Some(3)]).boxed(),
        Utf8Array::<i32>::from([Some("ef"), Some("ghijk")]).boxed(),
        FixedSizeListArray::try_new(
            fixed_size_type.clone(),
            UInt16Array::from_iter([31, 32, 33, 41, 42, 43].into_iter().map(Some)).boxed(),
            None,
        )
        .unwrap()
        .boxed(),
    ];
    let offsets = Some(vec![0, 1, 0, 1, 0].into());

    let array2 = UnionArray::new(data_type.clone(), types, fields, offsets);

    let mut a = GrowableUnion::new(vec![&array1, &array2], 10);

    // Take the whole first array
    a.extend(0, 0, 5);
    // Skip the first value from the second array: [31, 32, 33]
    a.extend(1, 1, 4);
    assert_eq!(a.len(), 9);

    let result: UnionArray = a.into();

    // UnionArray[1, [11, 12, 13], abcd, [21, 22, 23], 2, [41, 42, 43], ef, ghijk, 3]
    let types = vec![0, 2, 1, 2, 0, 2, 1, 1, 0].into();
    let fields = vec![
        Int32Array::from(&[Some(1), Some(2), Some(3)]).boxed(),
        Utf8Array::<i32>::from([Some("abcd"), Some("ef"), Some("ghijk")]).boxed(),
        FixedSizeListArray::try_new(
            fixed_size_type,
            UInt16Array::from_iter([11, 12, 13, 21, 22, 23, 41, 42, 43].into_iter().map(Some))
                .boxed(),
            None,
        )
        .unwrap()
        .boxed(),
    ];
    let offsets = Some(vec![0, 0, 0, 1, 1, 2, 1, 2, 2].into());

    let expected = UnionArray::new(data_type, types, fields, offsets);

    assert_eq!(expected, result);

    Ok(())
}
