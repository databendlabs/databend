// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::prelude::*;

#[test]
fn test_empty_array() {
	let mut builder = Utf8ArrayBuilder::with_capacity(16);
	let data_array = builder.finish();
	assert_eq!(true, data_array.is_empty());
	assert_eq!(DataType::Utf8, data_array.data_type());
}

#[test]
fn test_fill_data() {
	let mut builder = Utf8ArrayBuilder::with_capacity(16);
	builder.append_value("你好");
	builder.append_option(Some("\u{1F378}"));
	builder.append_null();

	let data_array: DataArray<Utf8Type> = builder.finish();
	let mut iter = data_array.into_iter();

	assert_eq!(3, data_array.len());
	assert_eq!(Some(Some("你好")), iter.next());
	assert_eq!(Some(Some("🍸")), iter.next());
	assert_eq!(Some(None), iter.next());
	assert_eq!(None, iter.next());
}

#[test]
fn test_new_from_opt_slice() {
	let data_array: DataArray<Utf8Type> = DataArray::new_from_opt_slice(&[Some("你好"), None]);
	let mut iter = data_array.into_iter();

	assert_eq!(2, data_array.len());
	assert_eq!(Some(Some("你好")), iter.next());
	assert_eq!(Some(None), iter.next());
	assert_eq!(None, iter.next());
}

#[test]
fn test_new_from_opt_iter() {
	let v = vec![None, Some("你好"), None];
	let mut iter = v.into_iter();
	iter.next(); // move iterator and create data array from second element
	let data_array: DataArray<Utf8Type> = DataArray::new_from_opt_iter(iter);
	let mut iter = data_array.into_iter();

	assert_eq!(2, data_array.len());
	assert_eq!(Some(Some("你好")), iter.next());
	assert_eq!(Some(None), iter.next());
	assert_eq!(None, iter.next());
}
