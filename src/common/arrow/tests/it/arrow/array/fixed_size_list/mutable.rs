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

use databend_common_arrow::arrow::array::*;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::datatypes::Field;

#[test]
fn primitive() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        Some(vec![None, None, None]),
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut list = MutableFixedSizeListArray::new(MutablePrimitiveArray::<i32>::new(), 3);
    list.try_extend(data).unwrap();
    let list: FixedSizeListArray = list.into();

    let a = list.value(0);
    let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

    let expected = Int32Array::from(vec![Some(1i32), Some(2), Some(3)]);
    assert_eq!(a, &expected);

    let a = list.value(1);
    let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

    let expected = Int32Array::from(vec![None, None, None]);
    assert_eq!(a, &expected)
}

#[test]
fn new_with_field() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        Some(vec![None, None, None]),
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut list = MutableFixedSizeListArray::new_with_field(
        MutablePrimitiveArray::<i32>::new(),
        "custom_items",
        false,
        3,
    );
    list.try_extend(data).unwrap();
    let list: FixedSizeListArray = list.into();

    assert_eq!(
        list.data_type(),
        &DataType::FixedSizeList(
            Box::new(Field::new("custom_items", DataType::Int32, false)),
            3
        )
    );

    let a = list.value(0);
    let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

    let expected = Int32Array::from(vec![Some(1i32), Some(2), Some(3)]);
    assert_eq!(a, &expected);

    let a = list.value(1);
    let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

    let expected = Int32Array::from(vec![None, None, None]);
    assert_eq!(a, &expected)
}

#[test]
fn extend_from_self() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];
    let mut a = MutableFixedSizeListArray::new(MutablePrimitiveArray::<i32>::new(), 3);
    a.try_extend(data.clone()).unwrap();

    a.try_extend_from_self(&a.clone()).unwrap();
    let a: FixedSizeListArray = a.into();

    let mut expected = data.clone();
    expected.extend(data);

    let mut b = MutableFixedSizeListArray::new(MutablePrimitiveArray::<i32>::new(), 3);
    b.try_extend(expected).unwrap();
    let b: FixedSizeListArray = b.into();

    assert_eq!(a, b);
}
