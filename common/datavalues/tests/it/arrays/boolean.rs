// Copyright 2020 Datafuse Labs.
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
fn test_empty_boolean_array() {
    let mut builder = BooleanArrayBuilder::with_capacity(16);
    let data_array: DFBooleanArray = builder.finish();
    let mut iter = data_array.into_iter();
    assert_eq!(None, iter.next());
    assert!(data_array.is_empty());
    assert_eq!(&DataType::Boolean, data_array.data_type());
}

#[test]
fn test_fill_data() {
    let mut builder = BooleanArrayBuilder::with_capacity(16);
    builder.append_value(true);
    builder.append_value(false);
    builder.append_null();
    builder.append_option(Some(false));
    builder.append_option(None);

    let data_array: DFBooleanArray = builder.finish();
    let mut iter = data_array.into_iter();

    assert_eq!(Some(Some(true)), iter.next());
    assert_eq!(Some(Some(false)), iter.next());
    assert_eq!(Some(None), iter.next());
    assert_eq!(Some(Some(false)), iter.next());
    assert_eq!(Some(None), iter.next());
    assert_eq!(None, iter.next());
}

#[test]
fn test_new_from_slice() {
    let data_array: DFBooleanArray = NewDataArray::new_from_slice(&[true, false]);
    let mut iter = data_array.into_iter();
    assert_eq!(Some(Some(true)), iter.next());
    assert_eq!(Some(Some(false)), iter.next());
    assert_eq!(None, iter.next());
}

#[test]
fn test_new_from_opt_slice() {
    let v = [Some(true), Some(false), None];
    let data_array: DFBooleanArray = NewDataArray::new_from_opt_slice(&v);
    let mut iter = data_array.into_iter();
    assert_eq!(Some(Some(true)), iter.next());
    assert_eq!(Some(Some(false)), iter.next());
    assert_eq!(Some(None), iter.next());
    assert_eq!(None, iter.next());
}

#[test]
fn test_new_from_opt_iter() {
    let v = vec![None, Some(true), Some(false), None];
    let mut iter = v.into_iter();
    iter.next(); // move iterator and create data_array from second element
    let data_array: DFBooleanArray = NewDataArray::new_from_opt_iter(iter);
    let mut iter = data_array.into_iter();
    assert_eq!(Some(Some(true)), iter.next());
    assert_eq!(Some(Some(false)), iter.next());
    assert_eq!(Some(None), iter.next());
    assert_eq!(None, iter.next());
}
