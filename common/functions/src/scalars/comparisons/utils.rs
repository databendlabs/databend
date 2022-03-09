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
use common_arrow::arrow::bitmap::unary;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::compute::comparison::Simd8;
use common_arrow::arrow::compute::comparison::Simd8Lanes;
use common_arrow::arrow::compute::comparison::Simd8PartialEq;
use common_arrow::arrow::compute::comparison::Simd8PartialOrd;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::EvalContext;
pub trait PrimitiveSimdImpl {
    fn vector_vector<T>(lhs: &PrimitiveColumn<T>, rhs: &PrimitiveColumn<T>) -> BooleanColumn
    where
        T: PrimitiveType + Simd8,
        T::Simd: Simd8PartialEq + Simd8PartialOrd;

    fn vector_const<T>(lhs: &PrimitiveColumn<T>, rhs: T) -> BooleanColumn
    where
        T: PrimitiveType + Simd8,
        T::Simd: Simd8PartialEq + Simd8PartialOrd;

    fn const_vector<T>(lhs: T, rhs: &PrimitiveColumn<T>) -> BooleanColumn
    where
        T: PrimitiveType + Simd8,
        T::Simd: Simd8PartialEq + Simd8PartialOrd;
}

pub trait BooleanSimdImpl {
    fn vector_vector(lhs: &BooleanColumn, rhs: &BooleanColumn) -> BooleanColumn;

    fn vector_const(lhs: &BooleanColumn, rhs: bool) -> BooleanColumn;

    fn const_vector(lhs: bool, rhs: &BooleanColumn) -> BooleanColumn;
}

pub trait StringSearchImpl {
    fn vector_vector(
        lhs: &StringColumn,
        rhs: &StringColumn,
        op: impl Fn(bool) -> bool,
    ) -> BooleanColumn;

    fn vector_const(lhs: &StringColumn, rhs: &[u8], op: impl Fn(bool) -> bool) -> BooleanColumn;
}

pub(crate) struct CommonPrimitiveImpl;

impl CommonPrimitiveImpl {
    /// QUOTE: (From common_arrow::arrow::compute::comparison::primitive)
    pub(crate) fn compare_op<T, F>(
        lhs: &PrimitiveColumn<T>,
        rhs: &PrimitiveColumn<T>,
        op: F,
    ) -> BooleanColumn
    where
        T: PrimitiveType + Simd8,
        F: Fn(T::Simd, T::Simd) -> u8,
    {
        let values = Self::compare_values_op(lhs.values(), rhs.values(), op);

        BooleanColumn::from_arrow_data(values.into())
    }

    pub(crate) fn compare_op_scalar<T, F>(lhs: &PrimitiveColumn<T>, rhs: T, op: F) -> BooleanColumn
    where
        T: PrimitiveType + Simd8,
        F: Fn(T::Simd, T::Simd) -> u8,
    {
        let values = Self::compare_values_op_scalar(lhs.values(), rhs, op);

        BooleanColumn::from_arrow_data(values.into())
    }

    fn compare_values_op<T, F>(lhs: &[T], rhs: &[T], op: F) -> MutableBitmap
    where
        T: PrimitiveType + Simd8,
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
        T: PrimitiveType + Simd8,
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
}

pub(crate) struct CommonBooleanImpl;

impl CommonBooleanImpl {
    /// QUOTE: (From common_arrow::arrow::compute::comparison::boolean)
    pub(crate) fn compare_op<F>(lhs: &BooleanColumn, rhs: &BooleanColumn, op: F) -> BooleanColumn
    where F: Fn(u64, u64) -> u64 {
        let values = binary(lhs.values(), rhs.values(), op);
        BooleanColumn::from_arrow_data(values)
    }

    pub(crate) fn compare_op_scalar<F>(lhs: &BooleanColumn, rhs: bool, op: F) -> BooleanColumn
    where F: Fn(u64, u64) -> u64 {
        let rhs = if rhs { !0 } else { 0 };

        let values = unary(lhs.values(), |x| op(x, rhs));
        BooleanColumn::from_arrow_data(values)
    }
}

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

fn primitive_simd_op<T, F>(l: &ColumnRef, r: &ColumnRef) -> Result<BooleanColumn>
where
    T: PrimitiveType + Simd8,
    T::Simd: Simd8PartialEq + Simd8PartialOrd,
    F: PrimitiveSimdImpl,
{
    debug_assert!(
        l.len() == r.len(),
        "Size of columns must match to apply binary expression"
    );

    let res = match (l.is_const(), r.is_const()) {
        (false, false) => {
            let lhs: &PrimitiveColumn<T> = Series::check_get(&l)?;
            let rhs: &PrimitiveColumn<T> = Series::check_get(&r)?;
            F::vector_vector::<T>(lhs, rhs)
        }
        (false, true) => {
            let lhs: &PrimitiveColumn<T> = Series::check_get(&l)?;
            let rhs = T::try_create_viewer(&r)?;
            let r = rhs.value_at(0).to_owned_scalar();
            F::vector_const::<T>(lhs, r)
        }
        (true, false) => {
            let lhs = T::try_create_viewer(&l)?;
            let l = lhs.value_at(0).to_owned_scalar();

            let rhs: &PrimitiveColumn<T> = Series::check_get(&r)?;
            F::const_vector::<T>(l, rhs)
        }
        (true, true) => unreachable!(),
    };
    Ok(res)
}

fn boolean_simd_op<F: BooleanSimdImpl>(l: &ColumnRef, r: &ColumnRef) -> Result<BooleanColumn> {
    debug_assert!(
        l.len() == r.len(),
        "Size of columns must match to apply binary expression"
    );

    let res = match (l.is_const(), r.is_const()) {
        (false, false) => {
            let lhs: &BooleanColumn = Series::check_get(l)?;
            let rhs: &BooleanColumn = Series::check_get(r)?;
            F::vector_vector(lhs, rhs)
        }
        (false, true) => {
            let lhs: &BooleanColumn = Series::check_get(l)?;
            let r = r.get_bool(0)?;
            F::vector_const(lhs, r)
        }
        (true, false) => {
            let l = l.get_bool(0)?;
            let rhs: &BooleanColumn = Series::check_get(r)?;
            F::const_vector(l, rhs)
        }
        (true, true) => unreachable!(),
    };
    Ok(res)
}

fn string_search_op<T: StringSearchImpl, F>(
    l: &ColumnRef,
    r: &ColumnRef,
    f: F,
) -> Result<BooleanColumn>
where
    F: Fn(bool) -> bool,
{
    let res = match r.is_const() {
        true => {
            let lhs: &StringColumn = Series::check_get(l)?;
            let r = r.get_string(0)?;
            T::vector_const(lhs, &r, f)
        }
        false => {
            let lhs: &StringColumn = Series::check_get(l)?;
            let rhs: &StringColumn = Series::check_get(r)?;
            T::vector_vector(lhs, rhs, f)
        }
    };
    Ok(res)
}
