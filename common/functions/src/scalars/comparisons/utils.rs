// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::bitmap::binary;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::compute::comparison;
use common_arrow::arrow::compute::comparison::Simd8Lanes;
use common_datavalues::prelude::*;

/// Evaluate `op(lhs, rhs)` for [`BinaryArray`]s using a specified
/// comparison function.
/// todo: use binaryscalarexpression.
fn compare_op_binary<F>(lhs: &StringColumn, rhs: &StringColumn, op: F) -> BooleanColumn
where F: Fn(&[u8], &[u8]) -> bool {
    let values = lhs
        .scalar_iter()
        .zip(rhs.scalar_iter())
        .map(|(lhs, rhs)| op(lhs, rhs));

    BooleanColumn::from_owned_iterator(values)
}

/// Evaluate `op(lhs, rhs)` for [`BooleanArray`]s using a specified
/// comparison function.
fn compare_op_bool<F>(lhs: &BooleanColumn, rhs: &BooleanColumn, op: F) -> BooleanColumn
where F: Fn(u64, u64) -> u64 {
    let values = binary(lhs.values(), rhs.values(), op);
    BooleanColumn::from_arrow_data(values)
}

/// Evaluate `op(lhs, rhs)` for [`PrimitiveArray`]s using a specified
/// comparison function.
pub(crate) fn compare_op<T, F>(
    lhs: &PrimitiveColumn<T>,
    rhs: &PrimitiveColumn<T>,
    op: F,
) -> BooleanColumn
where
    T: PrimitiveType + comparison::Simd8,
    F: Fn(T::Simd, T::Simd) -> u8,
{
    let values = compare_values_op(lhs.values(), rhs.values(), op);

    BooleanColumn::from_arrow_data(values.into())
}

/// Evaluate `op(left, right)` for [`PrimitiveArray`] and scalar using
/// a specified comparison function.
pub(crate) fn compare_op_scalar<T, F>(lhs: &PrimitiveColumn<T>, rhs: T, op: F) -> BooleanColumn
where
    T: PrimitiveType + comparison::Simd8,
    F: Fn(T::Simd, T::Simd) -> u8,
{
    let values = compare_values_op_scalar(lhs.values(), rhs, op);

    BooleanColumn::from_arrow_data(values.into())
}

fn compare_values_op<T, F>(lhs: &[T], rhs: &[T], op: F) -> MutableBitmap
where
    T: PrimitiveType + comparison::Simd8,
    F: Fn(T::Simd, T::Simd) -> u8,
{
    assert_eq!(lhs.len(), rhs.len());

    let lhs_chunks_iter = lhs.chunks_exact(8);
    let lhs_remainder = lhs_chunks_iter.remainder();
    let rhs_chunks_iter = rhs.chunks_exact(8);
    let rhs_remainder = rhs_chunks_iter.remainder();

    let mut values = Vec::with_capacity((lhs.len() + 7) / 8);
    let iterator = lhs_chunks_iter.zip(rhs_chunks_iter).map(|(lhs, rhs)| {
        let lhs = T::Simd::from_chunk(lhs);
        let rhs = T::Simd::from_chunk(rhs);
        op(lhs, rhs)
    });
    values.extend(iterator);

    if !lhs_remainder.is_empty() {
        let lhs = T::Simd::from_incomplete_chunk(lhs_remainder, T::default());
        let rhs = T::Simd::from_incomplete_chunk(rhs_remainder, T::default());
        values.push(op(lhs, rhs))
    };
    MutableBitmap::from_vec(values, lhs.len())
}

fn compare_values_op_scalar<T, F>(lhs: &[T], rhs: T, op: F) -> MutableBitmap
where
    T: PrimitiveType + comparison::Simd8,
    F: Fn(T::Simd, T::Simd) -> u8,
{
    let rhs = T::Simd::from_chunk(&[rhs; 8]);

    let lhs_chunks_iter = lhs.chunks_exact(8);
    let lhs_remainder = lhs_chunks_iter.remainder();

    let mut values = Vec::with_capacity((lhs.len() + 7) / 8);
    let iterator = lhs_chunks_iter.map(|lhs| {
        let lhs = T::Simd::from_chunk(lhs);
        op(lhs, rhs)
    });
    values.extend(iterator);

    if !lhs_remainder.is_empty() {
        let lhs = T::Simd::from_incomplete_chunk(lhs_remainder, T::default());
        values.push(op(lhs, rhs))
    };

    MutableBitmap::from_vec(values, lhs.len())
}
