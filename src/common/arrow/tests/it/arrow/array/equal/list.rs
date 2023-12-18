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

use databend_common_arrow::arrow::array::Int32Array;
use databend_common_arrow::arrow::array::ListArray;
use databend_common_arrow::arrow::array::MutableListArray;
use databend_common_arrow::arrow::array::MutablePrimitiveArray;
use databend_common_arrow::arrow::array::TryExtend;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::datatypes::DataType;

use super::test_equal;

fn create_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(data: T) -> ListArray<i32> {
    let iter = data.as_ref().iter().map(|x| {
        x.as_ref()
            .map(|x| x.as_ref().iter().map(|x| Some(*x)).collect::<Vec<_>>())
    });
    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(iter).unwrap();
    array.into()
}

#[test]
fn test_list_equal() {
    let a = create_list_array([Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
    let b = create_list_array([Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
    test_equal(&a, &b, true);

    let b = create_list_array([Some(&[1, 2, 3]), Some(&[4, 5, 7])]);
    test_equal(&a, &b, false);
}

// Test the case where null_count > 0
#[test]
fn test_list_null() {
    let a = create_list_array([Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
    let b = create_list_array([Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
    test_equal(&a, &b, true);

    let b = create_list_array([
        Some(&[1, 2]),
        None,
        Some(&[5, 6]),
        Some(&[3, 4]),
        None,
        None,
    ]);
    test_equal(&a, &b, false);

    let b = create_list_array([Some(&[1, 2]), None, None, Some(&[3, 5]), None, None]);
    test_equal(&a, &b, false);
}

// Test the case where offset != 0
#[test]
fn test_list_offsets() {
    let a = create_list_array([Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
    let b = create_list_array([Some(&[1, 2]), None, None, Some(&[3, 5]), None, None]);

    let a_slice = a.clone().sliced(0, 3);
    let b_slice = b.clone().sliced(0, 3);
    test_equal(&a_slice, &b_slice, true);

    let a_slice = a.clone().sliced(0, 5);
    let b_slice = b.clone().sliced(0, 5);
    test_equal(&a_slice, &b_slice, false);

    let a_slice = a.sliced(4, 1);
    let b_slice = b.sliced(4, 1);
    test_equal(&a_slice, &b_slice, true);
}

#[test]
fn test_bla() {
    let offsets = vec![0, 3, 3, 6].try_into().unwrap();
    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let values = Box::new(Int32Array::from([
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        None,
        Some(6),
    ]));
    let validity = Bitmap::from([true, false, true]);
    let lhs = ListArray::<i32>::new(data_type, offsets, values, Some(validity));
    let lhs = lhs.sliced(1, 2);

    let offsets = vec![0, 0, 3].try_into().unwrap();
    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let values = Box::new(Int32Array::from([Some(4), None, Some(6)]));
    let validity = Bitmap::from([false, true]);
    let rhs = ListArray::<i32>::new(data_type, offsets, values, Some(validity));

    assert_eq!(lhs, rhs);
}
