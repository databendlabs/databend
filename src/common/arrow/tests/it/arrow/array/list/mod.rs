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
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::datatypes::DataType;

mod mutable;

#[test]
fn debug() {
    let values = Buffer::from(vec![1, 2, 3, 4, 5]);
    let values = PrimitiveArray::<i32>::new(DataType::Int32, values, None);

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let array = ListArray::<i32>::new(
        data_type,
        vec![0, 2, 2, 3, 5].try_into().unwrap(),
        Box::new(values),
        None,
    );

    assert_eq!(format!("{array:?}"), "ListArray[[1, 2], [], [3], [4, 5]]");
}

#[test]
#[should_panic]
fn test_nested_panic() {
    let values = Buffer::from(vec![1, 2, 3, 4, 5]);
    let values = PrimitiveArray::<i32>::new(DataType::Int32, values, None);

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let array = ListArray::<i32>::new(
        data_type.clone(),
        vec![0, 2, 2, 3, 5].try_into().unwrap(),
        Box::new(values),
        None,
    );

    // The datatype for the nested array has to be created considering
    // the nested structure of the child data
    let _ = ListArray::<i32>::new(
        data_type,
        vec![0, 2, 4].try_into().unwrap(),
        Box::new(array),
        None,
    );
}

#[test]
fn test_nested_display() {
    let values = Buffer::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let values = PrimitiveArray::<i32>::new(DataType::Int32, values, None);

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let array = ListArray::<i32>::new(
        data_type,
        vec![0, 2, 4, 7, 7, 8, 10].try_into().unwrap(),
        Box::new(values),
        None,
    );

    let data_type = ListArray::<i32>::default_datatype(array.data_type().clone());
    let nested = ListArray::<i32>::new(
        data_type,
        vec![0, 2, 5, 6].try_into().unwrap(),
        Box::new(array),
        None,
    );

    let expected = "ListArray[[[1, 2], [3, 4]], [[5, 6, 7], [], [8]], [[9, 10]]]";
    assert_eq!(format!("{nested:?}"), expected);
}
