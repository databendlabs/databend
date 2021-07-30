// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::mem;
use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::buffer::Buffer;

use crate::arrays::IntoTakeRandom;
use crate::arrays::*;
use crate::prelude::*;

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for single chunk without nulls and arrow array as index.
pub unsafe fn take_no_null_primitive<T: DFNumericType>(
    arr: &PrimitiveArray<T::Native>,
    indices: &UInt32Array,
) -> Arc<PrimitiveArray<T::Native>> {
    assert_eq!(arr.null_count(), 0);

    let array_values = arr.values().as_slice();
    let index_values = indices.values().as_slice();

    let iter = index_values
        .iter()
        .map(|idx| *array_values.get_unchecked(*idx as usize));

    let values = Buffer::from_trusted_len_iter(iter);
    let validity = indices.validity().clone();
    Arc::new(PrimitiveArray::from_data(
        T::get_dtype().to_arrow(),
        values,
        validity,
    ))
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for single chunk without nulls and an iterator as index.
pub unsafe fn take_no_null_primitive_iter_unchecked<
    T: DFNumericType,
    I: IntoIterator<Item = usize>,
>(
    arr: &PrimitiveArray<T::Native>,
    indices: I,
) -> Arc<PrimitiveArray<T::Native>> {
    assert_eq!(arr.null_count(), 0);

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

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for a single chunk with null values and an iterator as index.
pub unsafe fn take_primitive_iter_unchecked<T: DFNumericType, I: IntoIterator<Item = usize>>(
    arr: &PrimitiveArray<T::Native>,
    indices: I,
) -> Arc<PrimitiveArray<T::Native>> {
    let array_values = arr.values();

    let iter = indices.into_iter().map(|idx| {
        if arr.is_valid(idx) {
            Some(*array_values.get_unchecked(idx))
        } else {
            None
        }
    });
    let arr = PrimitiveArray::from_trusted_len_iter(iter);
    Arc::new(arr)
}

/// Take kernel for a single chunk with null values and an iterator as index that does bound checks.
pub fn take_primitive_iter<T: DFNumericType, I: IntoIterator<Item = usize>>(
    arr: &PrimitiveArray<T::Native>,
    indices: I,
) -> Arc<PrimitiveArray<T::Native>> {
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

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for a single chunk without nulls and an iterator that can produce None values.
/// This is used in join operations.
pub unsafe fn take_no_null_primitive_opt_iter_unchecked<
    T: DFNumericType,
    I: IntoIterator<Item = Option<usize>>,
>(
    arr: &PrimitiveArray<T::Native>,
    indices: I,
) -> Arc<PrimitiveArray<T::Native>> {
    let array_values = arr.values();

    let iter = indices
        .into_iter()
        .map(|opt_idx| opt_idx.map(|idx| *array_values.get_unchecked(idx)));
    let arr = PrimitiveArray::from_trusted_len_iter(iter);

    Arc::new(arr)
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
    let arr = PrimitiveArray::from_trusted_len_iter(iter);

    Arc::new(arr)
}

/// Take kernel for multiple chunks. We directly return a DataArray because that path chooses the fastest collection path.
pub fn take_primitive_iter_n_arrays<T: DFNumericType, I: IntoIterator<Item = usize>>(
    ca: &DataArray<T>,
    indices: I,
) -> DataArray<T> {
    let taker = ca.take_rand();
    indices.into_iter().map(|idx| taker.get(idx)).collect()
}

/// Take kernel for multiple chunks where an iterator can produce None values.
/// Used in join operations. We directly return a DataArray because that path chooses the fastest collection path.
pub fn take_primitive_opt_iter_n_arrays<T: DFNumericType, I: IntoIterator<Item = Option<usize>>>(
    ca: &DataArray<T>,
    indices: I,
) -> DataArray<T> {
    let taker = ca.take_rand();
    indices
        .into_iter()
        .map(|opt_idx| opt_idx.and_then(|idx| taker.get(idx)))
        .collect()
}

/// Take kernel for single chunk without nulls and an iterator as index that does bound checks.
pub fn take_no_null_bool_iter<I: IntoIterator<Item = usize>>(
    arr: &BooleanArray,
    indices: I,
) -> Arc<BooleanArray> {
    debug_assert_eq!(arr.null_count(), 0);

    let iter = indices.into_iter().map(|idx| Some(arr.value(idx)));

    Arc::new(iter.collect())
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for single chunk without nulls and an iterator as index.
pub unsafe fn take_no_null_bool_iter_unchecked<I: IntoIterator<Item = usize>>(
    arr: &BooleanArray,
    indices: I,
) -> Arc<BooleanArray> {
    debug_assert_eq!(arr.null_count(), 0);
    let iter = indices
        .into_iter()
        .map(|idx| Some(arr.value_unchecked(idx)));

    Arc::new(iter.collect())
}

/// Take kernel for single chunk and an iterator as index that does bound checks.
pub fn take_bool_iter<I: IntoIterator<Item = usize>>(
    arr: &BooleanArray,
    indices: I,
) -> Arc<BooleanArray> {
    let iter = indices.into_iter().map(|idx| {
        if arr.is_null(idx) {
            None
        } else {
            Some(arr.value(idx))
        }
    });

    Arc::new(iter.collect())
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for single chunk and an iterator as index.
pub unsafe fn take_bool_iter_unchecked<I: IntoIterator<Item = usize>>(
    arr: &BooleanArray,
    indices: I,
) -> Arc<BooleanArray> {
    let iter = indices.into_iter().map(|idx| {
        if arr.is_null(idx) {
            None
        } else {
            Some(arr.value_unchecked(idx))
        }
    });

    Arc::new(iter.collect())
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for single chunk and an iterator as index.
pub unsafe fn take_bool_opt_iter_unchecked<I: IntoIterator<Item = Option<usize>>>(
    arr: &BooleanArray,
    indices: I,
) -> Arc<BooleanArray> {
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

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for single chunk without null values and an iterator as index that may produce None values.
pub unsafe fn take_no_null_bool_opt_iter_unchecked<I: IntoIterator<Item = Option<usize>>>(
    arr: &BooleanArray,
    indices: I,
) -> Arc<BooleanArray> {
    let iter = indices
        .into_iter()
        .map(|opt_idx| opt_idx.map(|idx| arr.value_unchecked(idx)));

    Arc::new(iter.collect())
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
pub unsafe fn take_no_null_utf8_iter_unchecked<I: IntoIterator<Item = usize>>(
    arr: &LargeUtf8Array,
    indices: I,
) -> Arc<LargeUtf8Array> {
    let iter = indices
        .into_iter()
        .map(|idx| Some(arr.value_unchecked(idx)));

    Arc::new(iter.collect())
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
pub unsafe fn take_utf8_iter_unchecked<I: IntoIterator<Item = usize>>(
    arr: &LargeUtf8Array,
    indices: I,
) -> Arc<LargeUtf8Array> {
    let iter = indices.into_iter().map(|idx| {
        if arr.is_null(idx) {
            None
        } else {
            Some(arr.value_unchecked(idx))
        }
    });

    Arc::new(iter.collect())
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
pub unsafe fn take_no_null_utf8_opt_iter_unchecked<I: IntoIterator<Item = Option<usize>>>(
    arr: &LargeUtf8Array,
    indices: I,
) -> Arc<LargeUtf8Array> {
    let iter = indices
        .into_iter()
        .map(|opt_idx| opt_idx.map(|idx| arr.value_unchecked(idx)));

    Arc::new(iter.collect())
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
pub unsafe fn take_utf8_opt_iter_unchecked<I: IntoIterator<Item = Option<usize>>>(
    arr: &LargeUtf8Array,
    indices: I,
) -> Arc<LargeUtf8Array> {
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

pub fn take_no_null_utf8_iter<I: IntoIterator<Item = usize>>(
    arr: &LargeUtf8Array,
    indices: I,
) -> Arc<LargeUtf8Array> {
    let iter = indices.into_iter().map(|idx| Some(arr.value(idx)));

    Arc::new(iter.collect())
}

pub fn take_utf8_iter<I: IntoIterator<Item = usize>>(
    arr: &LargeUtf8Array,
    indices: I,
) -> Arc<LargeUtf8Array> {
    let iter = indices.into_iter().map(|idx| {
        if arr.is_null(idx) {
            None
        } else {
            Some(arr.value(idx))
        }
    });

    Arc::new(LargeUtf8Array::from_trusted_len_iter(iter))
}
