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
use std::iter::FusedIterator;
use std::iter::TrustedLen;
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

// Keep C in the iterator's type, not in a fn pointer. Otherwise even identity
// conversions can leave an indirect call in each row of a vectorized function.
pub struct ComputeIter<'a, C, F: AccessType, T> {
    inner: F::ColumnIterator<'a>,
    _compute: PhantomData<fn() -> (C, T)>,
}

impl<'a, C, F: AccessType, T> Debug for ComputeIter<'a, C, F, T>
where F::ColumnIterator<'a>: Debug
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputeIter")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<'a, C, F: AccessType, T> Clone for ComputeIter<'a, C, F, T>
where F::ColumnIterator<'a>: Clone
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _compute: PhantomData,
        }
    }
}

impl<'a, C, F, T> Iterator for ComputeIter<'a, C, F, T>
where
    F: AccessType,
    T: AccessType,
    C: Compute<F, T>,
{
    type Item = T::ScalarRef<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(C::compute)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline]
    fn fold<B, G>(self, init: B, mut f: G) -> B
    where G: FnMut(B, Self::Item) -> B {
        self.inner
            .fold(init, |acc, value| f(acc, C::compute(value)))
    }
}

impl<'a, C, F, T> DoubleEndedIterator for ComputeIter<'a, C, F, T>
where
    F: AccessType,
    T: AccessType,
    C: Compute<F, T>,
    F::ColumnIterator<'a>: DoubleEndedIterator,
{
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(C::compute)
    }

    #[inline]
    fn rfold<B, G>(self, init: B, mut f: G) -> B
    where G: FnMut(B, Self::Item) -> B {
        self.inner
            .rfold(init, |acc, value| f(acc, C::compute(value)))
    }
}

impl<'a, C, F, T> ExactSizeIterator for ComputeIter<'a, C, F, T>
where
    F: AccessType,
    T: AccessType,
    C: Compute<F, T>,
    F::ColumnIterator<'a>: ExactSizeIterator,
{
}

impl<'a, C, F, T> FusedIterator for ComputeIter<'a, C, F, T>
where
    F: AccessType,
    T: AccessType,
    C: Compute<F, T>,
    F::ColumnIterator<'a>: FusedIterator,
{
}

// SAFETY: AccessType requires the inner iterator to implement TrustedLen.
// Computing exactly one output for each input preserves its length contract.
unsafe impl<C, F, T> TrustedLen for ComputeIter<'_, C, F, T>
where
    F: AccessType,
    T: AccessType,
    C: Compute<F, T>,
{
}

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
    type ColumnIterator<'a> = ComputeIter<'a, C, F, T>;

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
        ComputeIter {
            inner: F::iter_column(col),
            _compute: PhantomData,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_iterator_preserves_values_and_length() {
        type View = NumberConvertView<i32, i64>;
        let column = vec![i32::MIN, -1, 0, 1, i32::MAX].into();
        let mut iter = View::iter_column(&column);
        assert_eq!(iter.size_hint(), (5, Some(5)));
        assert_eq!(iter.len(), 5);
        assert_eq!(
            std::mem::size_of_val(&iter),
            std::mem::size_of_val(&NumberType::<i32>::iter_column(&column))
        );
        assert_eq!(iter.next(), Some(i32::MIN as i64));
        assert_eq!(iter.next_back(), Some(i32::MAX as i64));
        assert_eq!(iter.size_hint(), (3, Some(3)));
        assert_eq!(iter.clone().collect::<Vec<_>>(), vec![-1, 0, 1]);
        assert_eq!(iter.fold((0, 0), |(n, sum), v| (n + 1, sum + v)), (3, 0));
        let empty = Vec::<i32>::new().into();
        let mut iter = View::iter_column(&empty);
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    #[expect(
        clippy::double_ended_iterator_last,
        reason = "Compare Map's traversal side effects"
    )]
    fn compute_iterator_matches_map_traversal() {
        use std::cell::Cell;
        thread_local! { static CALLS: Cell<usize> = const { Cell::new(0) }; }
        #[derive(Debug, Clone, PartialEq)]
        struct Counting;
        impl Compute<NumberType<i64>, NumberType<i64>> for Counting {
            fn compute<'a>(
                value: <NumberType<i64> as AccessType>::ScalarRef<'a>,
            ) -> <NumberType<i64> as AccessType>::ScalarRef<'a> {
                CALLS.with(|calls| calls.set(calls.get() + 1));
                value
            }
            fn compute_domain(domain: &SimpleDomain<i64>) -> SimpleDomain<i64> {
                *domain
            }
        }
        type View = ComputeView<Counting, NumberType<i64>, NumberType<i64>>;
        let column = vec![1, 2, 3, 4, 5].into();
        let mut mapped = NumberType::<i64>::iter_column(&column).map(Counting::compute);
        let mut typed = View::iter_column(&column);
        assert_eq!(typed.nth(1), mapped.nth(1));
        assert_eq!(typed.nth_back(1), mapped.nth_back(1));
        assert_eq!(typed.len(), mapped.len());
        assert_eq!(CALLS.with(Cell::get), 8);
        CALLS.with(|calls| calls.set(0));
        assert_eq!(typed.last(), mapped.last());
        assert_eq!(CALLS.with(Cell::get), 2);
        CALLS.with(|calls| calls.set(0));
        assert_eq!(View::iter_column(&column).count(), 5);
        assert_eq!(CALLS.with(Cell::get), 5);
        assert_eq!(View::iter_column(&column).rfold(0, |sum, v| sum + v), 15);
    }

    #[test]
    fn compute_iterator_matches_indexing_and_composes() {
        type View = NumberConvertView<i64, i8>;
        type Identity = ComputeView<View, View, View>;
        for values in [vec![], vec![
            i64::MIN,
            -129,
            -128,
            -1,
            0,
            127,
            128,
            i64::MAX,
        ]] {
            let column = values.into();
            let expected = (0..View::column_len(&column))
                .map(|row| View::index_column(&column, row).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(View::iter_column(&column).collect::<Vec<_>>(), expected);
            assert_eq!(Identity::iter_column(&column).collect::<Vec<_>>(), expected);
            assert_eq!(
                View::iter_column(&column).rev().collect::<Vec<_>>(),
                expected.iter().rev().copied().collect::<Vec<_>>()
            );
        }
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
