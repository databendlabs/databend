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
use databend_common_arrow::arrow::compute::aggregate::estimated_bytes_size;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::datatypes::Field;

#[test]
fn primitive() {
    let a = Int32Array::from_slice([1, 2, 3, 4, 5]);
    assert_eq!(5 * std::mem::size_of::<i32>(), estimated_bytes_size(&a));
}

#[test]
fn boolean() {
    let a = BooleanArray::from_slice([true]);
    assert_eq!(1, estimated_bytes_size(&a));
}

#[test]
fn utf8() {
    let a = Utf8Array::<i32>::from_slice(["aaa"]);
    assert_eq!(3 + 2 * std::mem::size_of::<i32>(), estimated_bytes_size(&a));
}

#[test]
fn fixed_size_list() {
    let data_type =
        DataType::FixedSizeList(Box::new(Field::new("elem", DataType::Float32, false)), 3);
    let values = Box::new(Float32Array::from_slice([1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
    let a = FixedSizeListArray::new(data_type, values, None);
    assert_eq!(6 * std::mem::size_of::<f32>(), estimated_bytes_size(&a));
}
