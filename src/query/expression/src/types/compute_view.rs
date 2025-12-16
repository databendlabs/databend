// Copyright 2021 Datafuse Labs
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

use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Range;

use databend_common_exception::Result;
use num_traits::AsPrimitive;

use super::AccessType;
use super::Number;
use super::NumberType;
use super::SimpleDomain;
use crate::Column;
use crate::Domain;
use crate::ScalarRef;

pub trait Compute<F, T>: Debug + Clone + PartialEq + 'static
where
    F: AccessType,
    T: AccessType,
{
    fn compute<'a>(value: F::ScalarRef<'a>) -> T::ScalarRef<'a>;

    fn compute_domain(domain: &F::Domain) -> T::Domain;
}

impl<T> Compute<T, T> for T
where T: AccessType
{
    fn compute<'a>(value: T::ScalarRef<'a>) -> T::ScalarRef<'a> {
        value
    }

    fn compute_domain(domain: &T::Domain) -> T::Domain {
        domain.to_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeView<C, F, T>(PhantomData<(C, F, T)>);

impl<C, F, T> AccessType for ComputeView<C, F, T>
where
    F: AccessType,
    T: AccessType,
    C: Compute<F, T>,
{
    type Scalar = T::Scalar;
    type ScalarRef<'a> = T::ScalarRef<'a>;
    type Column = F::Column;
    type Domain = T::Domain;
    type ColumnIterator<'a> =
        std::iter::Map<F::ColumnIterator<'a>, fn(F::ScalarRef<'a>) -> T::ScalarRef<'a>>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        T::to_owned_scalar(scalar)
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        T::to_scalar_ref(scalar)
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        F::try_downcast_scalar(scalar).map(C::compute)
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        F::try_downcast_column(col)
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        F::try_downcast_domain(domain).map(|domain| C::compute_domain(&domain))
    }

    fn column_len(col: &Self::Column) -> usize {
        F::column_len(col)
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        F::index_column(col, index).map(C::compute)
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        unsafe {
            let scalar = F::index_column_unchecked(col, index);
            C::compute(scalar)
        }
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        F::slice_column(col, range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        F::iter_column(col).map(C::compute)
    }

    fn scalar_memory_size(_: &Self::ScalarRef<'_>) -> usize {
        std::mem::size_of::<F>()
    }

    fn column_memory_size(col: &Self::Column, _gc: bool) -> usize {
        F::column_len(col) * std::mem::size_of::<F>()
    }

    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        T::compare(lhs, rhs)
    }

    fn equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::equal(left, right)
    }

    fn not_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::not_equal(left, right)
    }

    fn greater_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::greater_than(left, right)
    }

    fn less_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::less_than(left, right)
    }

    fn greater_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::greater_than_equal(left, right)
    }

    fn less_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::less_than_equal(left, right)
    }
}

/// For number convert
pub type NumberConvertView<F, T> = ComputeView<NumberConvert<F, T>, NumberType<F>, NumberType<T>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumberConvert<F, T>(std::marker::PhantomData<(F, T)>);

impl<F, T> Compute<NumberType<F>, NumberType<T>> for NumberConvert<F, T>
where
    F: Number + AsPrimitive<T>,
    T: Number,
{
    fn compute<'a>(
        value: <NumberType<F> as AccessType>::ScalarRef<'a>,
    ) -> <NumberType<T> as AccessType>::ScalarRef<'a> {
        value.as_()
    }

    fn compute_domain(domain: &SimpleDomain<F>) -> SimpleDomain<T> {
        let min = domain.min.as_();
        let max = domain.max.as_();
        SimpleDomain { min, max }
    }
}
