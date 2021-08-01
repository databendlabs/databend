// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::buffer::Buffer;

use crate::prelude::*;

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for single chunk without nulls and an iterator as index.
pub unsafe fn take_primitive_iter_unchecked<T: DFNumericType, I: IntoIterator<Item = usize>>(
    arr: &PrimitiveArray<T::Native>,
    indices: I,
) -> Arc<PrimitiveArray<T::Native>> {
    match arr.null_count() {
        0 => {
            let array_values = arr.values().as_slice();
            let iter = indices
                .into_iter()
                .map(|idx| *array_values.get_unchecked(idx));

            let values = Buffer::from_trusted_len_iter_unchecked(iter);
            Arc::new(PrimitiveArray::from_data(
                T::data_type().to_arrow(),
                values,
                None,
            ))
        }
        _ => {
            let array_values = arr.values();

            let arr = indices
                .into_iter()
                .map(|idx| {
                    if arr.is_valid(idx) {
                        Some(array_values[idx])
                    } else {
                        None
                    }
                })
                .collect();

            Arc::new(arr)
        }
    }
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for a single chunk and an iterator that can produce None values.
/// This is used in join operations.
pub unsafe fn take_primitive_opt_iter_unchecked<
    T: DFNumericType,
    I: IntoIterator<Item = Option<usize>>,
>(
    arr: &PrimitiveArray<T::Native>,
    indices: I,
) -> Arc<PrimitiveArray<T::Native>> {
    match arr.null_count() {
        0 => {
            let array_values = arr.values();

            let iter = indices
                .into_iter()
                .map(|opt_idx| opt_idx.map(|idx| *array_values.get_unchecked(idx)));
            let arr = PrimitiveArray::from_trusted_len_iter_unchecked(iter);

            Arc::new(arr)
        }
        _ => {
            let array_values = arr.values();

            let iter = indices.into_iter().map(|opt_idx| {
                opt_idx.and_then(|idx| {
                    if arr.is_valid(idx) {
                        Some(*array_values.get_unchecked(idx))
                    } else {
                        None
                    }
                })
            });
            let arr = PrimitiveArray::from_trusted_len_iter_unchecked(iter);

            Arc::new(arr)
        }
    }
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for single chunk without nulls and an iterator as index.
pub unsafe fn take_bool_iter_unchecked<I: IntoIterator<Item = usize>>(
    arr: &BooleanArray,
    indices: I,
) -> Arc<BooleanArray> {
    match arr.null_count() {
        0 => {
            let iter = indices.into_iter().map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx))
                }
            });

            Arc::new(iter.collect())
        }
        _ => {
            let iter = indices.into_iter().map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value_unchecked(idx))
                }
            });

            Arc::new(iter.collect())
        }
    }
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for single chunk and an iterator as index.
pub unsafe fn take_bool_opt_iter_unchecked<I: IntoIterator<Item = Option<usize>>>(
    arr: &BooleanArray,
    indices: I,
) -> Arc<BooleanArray> {
    match arr.null_count() {
        0 => {
            let iter = indices
                .into_iter()
                .map(|opt_idx| opt_idx.map(|idx| arr.value_unchecked(idx)));

            Arc::new(iter.collect())
        }
        _ => {
            let iter = indices.into_iter().map(|opt_idx| {
                opt_idx.and_then(|idx| {
                    if arr.is_null(idx) {
                        None
                    } else {
                        Some(arr.value_unchecked(idx))
                    }
                })
            });

            Arc::new(iter.collect())
        }
    }
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
pub unsafe fn take_utf8_iter_unchecked<I: IntoIterator<Item = usize>>(
    arr: &LargeUtf8Array,
    indices: I,
) -> Arc<LargeUtf8Array> {
    match arr.null_count() {
        0 => {
            let iter = indices
                .into_iter()
                .map(|idx| Some(arr.value_unchecked(idx)));
            Arc::new(LargeUtf8Array::from_trusted_len_iter_unchecked(iter))
        }
        _ => {
            let iter = indices.into_iter().map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value_unchecked(idx))
                }
            });
            Arc::new(LargeUtf8Array::from_trusted_len_iter_unchecked(iter))
        }
    }
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
pub unsafe fn take_utf8_opt_iter_unchecked<I: IntoIterator<Item = Option<usize>>>(
    arr: &LargeUtf8Array,
    indices: I,
) -> Arc<LargeUtf8Array> {
    match arr.null_count() {
        0 => {
            let iter = indices
                .into_iter()
                .map(|opt_idx| opt_idx.map(|idx| arr.value_unchecked(idx)));

            Arc::new(LargeUtf8Array::from_trusted_len_iter_unchecked(iter))
        }
        _ => {
            let iter = indices.into_iter().map(|opt_idx| {
                opt_idx.and_then(|idx| {
                    if arr.is_null(idx) {
                        None
                    } else {
                        Some(arr.value_unchecked(idx))
                    }
                })
            });

            Arc::new(LargeUtf8Array::from_trusted_len_iter_unchecked(iter))
        }
    }
}
