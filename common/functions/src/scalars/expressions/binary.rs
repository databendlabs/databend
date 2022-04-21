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

use std::simd::LaneCount;
use std::simd::Simd;
use std::simd::SimdElement;
use std::simd::SupportedLaneCount;

use common_arrow::arrow::compute::comparison::Simd8;
use common_arrow::arrow::compute::comparison::Simd8Lanes;
use common_arrow::bitmap::MutableBitmap;
use common_datavalues::prelude::*;
use common_exception::Result;

use super::EvalContext;

pub fn scalar_binary_op<L: Scalar, R: Scalar, O: Scalar, F>(
    l: &ColumnRef,
    r: &ColumnRef,
    f: F,
    ctx: &mut EvalContext,
) -> Result<<O as Scalar>::ColumnType>
where
    F: Fn(L::RefType<'_>, R::RefType<'_>, &mut EvalContext) -> O,
{
    debug_assert!(
        l.len() == r.len(),
        "Size of columns must match to apply binary expression"
    );

    let result = match (l.is_const(), r.is_const()) {
        (false, true) => {
            let left: &<L as Scalar>::ColumnType = unsafe { Series::static_cast(l) };
            let right = R::try_create_viewer(r)?;

            let b = right.value_at(0);
            let it = left.scalar_iter().map(|a| f(a, b, ctx));
            <O as Scalar>::ColumnType::from_owned_iterator(it)
        }

        (false, false) => {
            let left: &<L as Scalar>::ColumnType = unsafe { Series::static_cast(l) };
            let right: &<R as Scalar>::ColumnType = unsafe { Series::static_cast(r) };

            let it = left
                .scalar_iter()
                .zip(right.scalar_iter())
                .map(|(a, b)| f(a, b, ctx));
            <O as Scalar>::ColumnType::from_owned_iterator(it)
        }

        (true, false) => {
            let left = L::try_create_viewer(l)?;
            let a = left.value_at(0);

            let right: &<R as Scalar>::ColumnType = unsafe { Series::static_cast(r) };
            let it = right.scalar_iter().map(|b| f(a, b, ctx));
            <O as Scalar>::ColumnType::from_owned_iterator(it)
        }

        // True True ?
        (true, true) => {
            let left = L::try_create_viewer(l)?;
            let right = R::try_create_viewer(r)?;

            let it = left.iter().zip(right.iter()).map(|(a, b)| f(a, b, ctx));
            <O as Scalar>::ColumnType::from_owned_iterator(it)
        }
    };

    if let Some(error) = ctx.error.take() {
        return Err(error);
    }
    Ok(result)
}

pub fn scalar_binary_op_ref<'a, L: Scalar, R: Scalar, O: Scalar, F>(
    l: &'a ColumnRef,
    r: &'a ColumnRef,
    f: F,
    ctx: &mut EvalContext,
) -> Result<<O as Scalar>::ColumnType>
where
    F: Fn(L::RefType<'a>, R::RefType<'a>, &mut EvalContext) -> O::RefType<'a>,
{
    debug_assert!(
        l.len() == r.len(),
        "Size of columns must match to apply binary expression"
    );

    let result = match (l.is_const(), r.is_const()) {
        (false, true) => {
            let left: &<L as Scalar>::ColumnType = unsafe { Series::static_cast(l) };
            let right = R::try_create_viewer(r)?;

            let b = right.value_at(0);
            let it = left.scalar_iter().map(|a| f(a, b, ctx));
            <O as Scalar>::ColumnType::from_iterator(it)
        }

        (false, false) => {
            let left: &<L as Scalar>::ColumnType = unsafe { Series::static_cast(l) };
            let right: &<R as Scalar>::ColumnType = unsafe { Series::static_cast(r) };

            let it = left
                .scalar_iter()
                .zip(right.scalar_iter())
                .map(|(a, b)| f(a, b, ctx));
            <O as Scalar>::ColumnType::from_iterator(it)
        }

        (true, false) => {
            let left = L::try_create_viewer(l)?;
            let a = left.value_at(0);

            let right: &<R as Scalar>::ColumnType = unsafe { Series::static_cast(r) };
            let it = right.scalar_iter().map(|b| f(a, b, ctx));
            <O as Scalar>::ColumnType::from_iterator(it)
        }

        // True True ?
        (true, true) => {
            let left = L::try_create_viewer(l)?;
            let right = R::try_create_viewer(r)?;

            let it = left.iter().zip(right.iter()).map(|(a, b)| f(a, b, ctx));
            <O as Scalar>::ColumnType::from_iterator(it)
        }
    };

    if let Some(error) = ctx.error.take() {
        return Err(error);
    }
    Ok(result)
}

pub fn binary_simd_op<T, O, F, const N: usize>(
    l: &ColumnRef,
    r: &ColumnRef,
    op: F,
) -> Result<PrimitiveColumn<O>>
where
    T: PrimitiveType + SimdElement,
    O: PrimitiveType + SimdElement,
    F: Fn(Simd<T, N>, Simd<T, N>) -> Simd<O, N>,
    LaneCount<N>: SupportedLaneCount,
{
    debug_assert!(
        l.len() == r.len(),
        "Size of columns must match to apply binary expression"
    );

    match (l.is_const(), r.is_const()) {
        (false, false) => {
            let lhs: &PrimitiveColumn<T> = Series::check_get(l)?;
            let lhs_chunks = lhs.values().chunks_exact(N);
            let lhs_remainder = lhs_chunks.remainder();

            let rhs: &PrimitiveColumn<T> = Series::check_get(r)?;
            let rhs_chunks = rhs.values().chunks_exact(N);
            let rhs_remainder = rhs_chunks.remainder();

            let mut values = Vec::<O>::with_capacity(lhs.len());
            lhs_chunks.zip(rhs_chunks).for_each(|(lhs, rhs)| {
                let res = op(Simd::from_slice(lhs), Simd::from_slice(rhs));
                values.extend_from_slice(res.as_array())
            });

            if !lhs_remainder.is_empty() {
                let lhs = from_incomplete_chunk(lhs_remainder, T::default());
                let rhs = from_incomplete_chunk(rhs_remainder, T::default());
                let res = op(lhs, rhs);
                values.extend_from_slice(&res.as_array()[0..lhs_remainder.len()])
            };

            Ok(PrimitiveColumn::<O>::new_from_vec(values))
        }
        (false, true) => {
            let lhs: &PrimitiveColumn<T> = Series::check_get(l)?;
            let lhs_chunks = lhs.values().chunks_exact(N);
            let lhs_remainder = lhs_chunks.remainder();

            let rhs = T::try_create_viewer(r)?;
            let r = rhs.value_at(0).to_owned_scalar();
            let rhs = Simd::<T, N>::splat(r);

            let mut values = Vec::<O>::with_capacity(lhs.len());
            lhs_chunks.for_each(|lhs| {
                let res = op(Simd::from_slice(lhs), rhs);
                values.extend_from_slice(res.as_array())
            });

            if !lhs_remainder.is_empty() {
                let lhs = from_incomplete_chunk(lhs_remainder, T::default());
                let res = op(lhs, rhs);
                values.extend_from_slice(&res.as_array()[0..lhs_remainder.len()])
            };

            Ok(PrimitiveColumn::<O>::new_from_vec(values))
        }
        (true, false) => {
            let lhs = T::try_create_viewer(l)?;
            let l = lhs.value_at(0).to_owned_scalar();
            let lhs = Simd::<T, N>::splat(l);

            let rhs: &PrimitiveColumn<T> = Series::check_get(r)?;
            let rhs_chunks = rhs.values().chunks_exact(N);
            let rhs_remainder = rhs_chunks.remainder();

            let mut values = Vec::<O>::with_capacity(rhs.len());
            rhs_chunks.for_each(|rhs| {
                let res = op(lhs, Simd::from_slice(rhs));
                values.extend_from_slice(res.as_array())
            });

            if !rhs_remainder.is_empty() {
                let rhs = from_incomplete_chunk(rhs_remainder, T::default());
                let res = op(lhs, rhs);
                values.extend_from_slice(&res.as_array()[0..rhs_remainder.len()])
            };

            Ok(PrimitiveColumn::<O>::new_from_vec(values))
        }
        (true, true) => unreachable!(),
    }
}

/// QUOTE: (From arrow2::arrow::compute::comparison::primitive)
pub fn primitive_simd_op_boolean<T, F>(l: &ColumnRef, r: &ColumnRef, op: F) -> Result<BooleanColumn>
where
    T: PrimitiveType + Simd8,
    F: Fn(T::Simd, T::Simd) -> u8,
{
    debug_assert!(
        l.len() == r.len(),
        "Size of columns must match to apply binary expression"
    );

    let res = match (l.is_const(), r.is_const()) {
        (false, false) => {
            let lhs: &PrimitiveColumn<T> = Series::check_get(l)?;
            let lhs_chunks_iter = lhs.values().chunks_exact(8);
            let lhs_remainder = lhs_chunks_iter.remainder();

            let rhs: &PrimitiveColumn<T> = Series::check_get(r)?;
            let rhs_chunks_iter = rhs.values().chunks_exact(8);
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
        (false, true) => {
            let lhs: &PrimitiveColumn<T> = Series::check_get(l)?;
            let lhs_chunks_iter = lhs.values().chunks_exact(8);
            let lhs_remainder = lhs_chunks_iter.remainder();

            let rhs = T::try_create_viewer(r)?;
            let r = rhs.value_at(0).to_owned_scalar();
            let rhs = T::Simd::from_chunk(&[r; 8]);

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
        (true, false) => {
            let lhs = T::try_create_viewer(l)?;
            let l = lhs.value_at(0).to_owned_scalar();
            let lhs = T::Simd::from_chunk(&[l; 8]);

            let rhs: &PrimitiveColumn<T> = Series::check_get(r)?;
            let rhs_chunks_iter = rhs.values().chunks_exact(8);
            let rhs_remainder = rhs_chunks_iter.remainder();

            let mut values = Vec::with_capacity((rhs.len() + 7) / 8);
            let iterator = rhs_chunks_iter.map(|rhs| {
                let rhs = T::Simd::from_chunk(rhs);
                op(lhs, rhs)
            });
            values.extend(iterator);

            if !rhs_remainder.is_empty() {
                let rhs = T::Simd::from_incomplete_chunk(rhs_remainder, T::default());
                values.push(op(lhs, rhs))
            };

            MutableBitmap::from_vec(values, rhs.len())
        }
        (true, true) => unreachable!(),
    };
    Ok(BooleanColumn::from_arrow_data(res.into()))
}

pub(crate) fn from_incomplete_chunk<T, const N: usize>(v: &[T], remaining: T) -> Simd<T, N>
where
    T: SimdElement,
    LaneCount<N>: SupportedLaneCount,
{
    let mut res = [remaining; N];
    res.iter_mut().zip(v.iter()).for_each(|(a, b)| *a = *b);
    Simd::from_array(res)
}
