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

use databend_common_column::buffer::Buffer;
use num_traits::AsPrimitive;

use super::simple_type::SimpleType;
use super::AccessType;
use crate::types::CoreNumber;
use crate::types::Number;
use crate::types::SimpleDomain;
use crate::Column;
use crate::Domain;
use crate::ScalarRef;

pub trait Compute<F, T>: Debug + Clone + PartialEq + 'static
where
    F: SimpleType,
    T: SimpleType,
{
    fn compute(value: &F::Scalar) -> T::Scalar;

    fn compute_domain(domain: &F::Domain) -> T::Domain;
}

impl<T> Compute<T, T> for T
where T: SimpleType
{
    fn compute(value: &T::Scalar) -> T::Scalar {
        *value
    }

    fn compute_domain(domain: &T::Domain) -> T::Domain {
        *domain
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeView<C, F, T>(PhantomData<(C, F, T)>);

impl<C, F, T> AccessType for ComputeView<C, F, T>
where
    F: SimpleType,
    T: SimpleType,
    C: Compute<F, T>,
{
    type Scalar = T::Scalar;
    type ScalarRef<'a> = T::Scalar;
    type Column = Buffer<F::Scalar>;
    type Domain = T::Domain;
    type ColumnIterator<'a> =
        std::iter::Map<std::slice::Iter<'a, F::Scalar>, fn(&'a F::Scalar) -> T::Scalar>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::ScalarRef<'a>> {
        F::downcast_scalar(scalar).map(|v| C::compute(&v))
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        F::downcast_column(col)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        F::downcast_domain(domain).map(|domain| C::compute_domain(&domain))
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.get(index).map(C::compute)
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        debug_assert!(index < col.len());
        C::compute(col.get_unchecked(index))
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.end - range.start)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter().map(C::compute as fn(&F::Scalar) -> T::Scalar)
    }

    fn scalar_memory_size(_: &Self::ScalarRef<'_>) -> usize {
        std::mem::size_of::<F>()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.len() * std::mem::size_of::<F>()
    }

    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        T::compare(&lhs, &rhs)
    }

    fn equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left == right
    }

    fn not_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left != right
    }

    fn greater_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::greater_than(&left, &right)
    }

    fn less_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::less_than(&left, &right)
    }

    fn greater_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::greater_than_equal(&left, &right)
    }

    fn less_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::less_than_equal(&left, &right)
    }
}

/// For number convert
pub type NumberConvertView<F, T> = ComputeView<NumberConvert<F, T>, CoreNumber<F>, CoreNumber<T>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumberConvert<F, T>(std::marker::PhantomData<(F, T)>);

impl<F, T> Compute<CoreNumber<F>, CoreNumber<T>> for NumberConvert<F, T>
where
    F: Number + AsPrimitive<T>,
    T: Number,
{
    fn compute(value: &F) -> T {
        value.as_()
    }

    fn compute_domain(domain: &SimpleDomain<F>) -> SimpleDomain<T> {
        let min = domain.min.as_();
        let max = domain.max.as_();
        SimpleDomain { min, max }
    }
}
