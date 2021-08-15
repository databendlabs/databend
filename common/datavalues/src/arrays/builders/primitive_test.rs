// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::prelude::*;

#[test]
fn test_empty_array() {
	let mut builder = PrimitiveArrayBuilder::<Int64Type>::with_capacity(16);
	let data_array = builder.finish();

	// verify empty data array and data type
	assert_eq!(true, data_array.is_empty());
	assert_eq!(DataType::Int64, data_array.data_type());
}

#[test]
fn test_fill_data() {
	let mut builder = PrimitiveArrayBuilder::<Int64Type>::with_capacity(16);
	builder.append_value(1);
	builder.append_option(Some(2));
	builder.append_null();

	let data_array: DataArray<Int64Type> = builder.finish();
	let mut iter = data_array.into_iter();

	// verify data array with value, option value and null
	assert_eq!(Some(Some(1)), iter.next());
	assert_eq!(Some(Some(2)), iter.next());
	assert_eq!(Some(None), iter.next());
	assert_eq!(None, iter.next());
}

#[test]
fn test_new_from_slice() {
	let data_array: DataArray<Int64Type> = NewDataArray::new_from_slice(&[1, 2]);
	let mut iter = data_array.into_iter();

	// verify NewDataArray::new_from_slice
	assert_eq!(Some(Some(1)), iter.next());
	assert_eq!(Some(Some(2)), iter.next());
	assert_eq!(None, iter.next());
}

#[test]
fn test_new_from_opt_slice() {
	let v = [Some(1), Some(2), None];
	let data_array: DataArray<Int64Type> = NewDataArray::new_from_opt_slice(&v);
	let mut iter = data_array.into_iter();

	// verify NewDataArray::new_from_opt_slice
	assert_eq!(Some(Some(1)), iter.next());
	assert_eq!(Some(Some(2)), iter.next());
	assert_eq!(Some(None), iter.next());
	assert_eq!(None, iter.next());
}

#[test]
fn test_new_from_opt_iter() {
	let v = vec![None, Some(1), Some(2), None];
	let mut iter = v.into_iter();
	iter.next(); // move iterator and create data_array from second element
	let data_array: DataArray<Int64Type> = NewDataArray::new_from_opt_iter(iter);
	let mut iter = data_array.into_iter();

	assert_eq!(Some(Some(1)), iter.next());
	assert_eq!(Some(Some(2)), iter.next());
	assert_eq!(Some(None), iter.next());
	assert_eq!(None, iter.next());
}
