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
fn test_empty_array() {
    let mut builder = StringArrayBuilder::with_capacity(16);
    let data_array = builder.finish();
    assert!(data_array.is_empty());
    assert_eq!(&DataType::String, data_array.data_type());
}

#[test]
fn test_fill_data() {
    let mut builder = StringArrayBuilder::with_capacity(16);
    builder.append_value("你好");
    builder.append_option(Some("\u{1F378}"));
    builder.append_null();

    let data_array = builder.finish();
    let mut iter = data_array.into_iter();

    assert_eq!(3, data_array.len());
    assert_eq!(Some(Some("你好".as_bytes())), iter.next());
    assert_eq!(Some(Some("🍸".as_bytes())), iter.next());
    assert_eq!(Some(None), iter.next());
    assert_eq!(None, iter.next());
}

#[test]
fn test_new_from_opt_slice() {
    let data_array = DFStringArray::new_from_opt_slice(&[Some("你好"), None]);
    let mut iter = data_array.into_iter();

    assert_eq!(2, data_array.len());
    assert_eq!(Some(Some("你好".as_bytes())), iter.next());
    assert_eq!(Some(None), iter.next());
    assert_eq!(None, iter.next());
}

#[test]
fn test_new_from_opt_iter() {
    let v = vec![None, Some("你好"), None];
    let mut iter = v.into_iter();
    iter.next(); // move iterator and create data array from second element
    let data_array = DFStringArray::new_from_opt_iter(iter);
    let mut iter = data_array.into_iter();

    assert_eq!(2, data_array.len());
    assert_eq!(Some(Some("你好".as_bytes())), iter.next());
    assert_eq!(Some(None), iter.next());
    assert_eq!(None, iter.next());
}
