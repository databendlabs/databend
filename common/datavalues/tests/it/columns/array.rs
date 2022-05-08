// Copyright 2022 Datafuse Labs.
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

use common_datavalues::prelude::*;

#[test]
fn test_empty_array_column() {
    let mut builder = MutableArrayColumn::with_capacity_meta(16, ColumnMeta::Array {
        data_type: Int32Type::new_impl(),
    });
    let data_column: ArrayColumn = builder.finish();
    let mut iter = data_column.iter();
    assert_eq!(None, iter.next());
    assert!(data_column.is_empty());
}

#[test]
fn test_new_from_data() {
    let data_column: PrimitiveColumn<i32> = Int32Column::from_slice(&[1, 2, 3, 4, 5, 6]);
    let offsets: Vec<i64> = vec![0, 3, 6];
    let array_column: ArrayColumn =
        ArrayColumn::from_data(Int32Type::new_impl(), offsets.into(), data_column.arc());

    let v0 = DataValue::Array(vec![1i32.into(), 2i32.into(), 3i32.into()]);
    let v1 = DataValue::Array(vec![4i32.into(), 5i32.into(), 6i32.into()]);
    assert_eq!(v0, array_column.get(0));
    assert_eq!(v1, array_column.get(1));
}

#[test]
fn test_mutable_array_column() {
    let mut builder = MutableArrayColumn::with_capacity_meta(16, ColumnMeta::Array {
        data_type: Int32Type::new_impl(),
    });

    let v0 = ArrayValue::new(vec![1i32.into(), 2i32.into(), 3i32.into()]);
    let v1 = ArrayValue::new(vec![4i32.into(), 5i32.into(), 6i32.into()]);
    builder.append_value(v0.clone());
    builder.append_value(v1.clone());

    assert_eq!(Some(v1), builder.pop_value());
    assert_eq!(Some(v0), builder.pop_value());
    assert_eq!(None, builder.pop_value());

    let v0 = DataValue::Array(vec![1i32.into(), 2i32.into(), 3i32.into()]);
    let v1 = DataValue::Array(vec![4i32.into(), 5i32.into(), 6i32.into()]);
    let _ = builder.append_data_value(v0.clone());
    let _ = builder.append_data_value(v1.clone());
    assert_eq!(v1, builder.pop_data_value().unwrap());
    assert_eq!(v0, builder.pop_data_value().unwrap());
}
