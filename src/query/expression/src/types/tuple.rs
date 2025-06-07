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
use std::iter::TrustedLen;
use std::marker::PhantomData;
use std::ops::Range;

use super::AccessType;
use crate::property::Domain;
use crate::types::ScalarRef;
use crate::values::Column;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pair<A, B>(PhantomData<(A, B)>);

impl<A: AccessType, B: AccessType> AccessType for Pair<A, B> {
    type Scalar = (A::Scalar, B::Scalar);
    type ScalarRef<'a> = (A::ScalarRef<'a>, B::ScalarRef<'a>);
    type Column = PairColumn<A, B>;
    type Domain = (A::Domain, B::Domain);
    type ColumnIterator<'a> = PairIterator<'a, A, B>;

    fn to_owned_scalar((k, v): Self::ScalarRef<'_>) -> Self::Scalar {
        (A::to_owned_scalar(k), B::to_owned_scalar(v))
    }

    fn to_scalar_ref((k, v): &Self::Scalar) -> Self::ScalarRef<'_> {
        (A::to_scalar_ref(k), B::to_scalar_ref(v))
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Tuple(fields) if fields.len() == 2 => Some((
                A::try_downcast_scalar(&fields[0])?,
                B::try_downcast_scalar(&fields[1])?,
            )),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        match col {
            Column::Tuple(fields) if fields.len() == 2 => Some(PairColumn(
                A::try_downcast_column(&fields[0])?,
                B::try_downcast_column(&fields[1])?,
            )),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Tuple(fields) if fields.len() == 2 => Some((
                A::try_downcast_domain(&fields[0])?,
                B::try_downcast_domain(&fields[1])?,
            )),
            _ => None,
        }
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.index_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter()
    }

    fn scalar_memory_size((k, v): &Self::ScalarRef<'_>) -> usize {
        A::scalar_memory_size(k) + B::scalar_memory_size(v)
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }

    #[inline(always)]
    fn compare(
        (lhs_k, lhs_v): Self::ScalarRef<'_>,
        (rhs_k, rhs_v): Self::ScalarRef<'_>,
    ) -> Ordering {
        match A::compare(lhs_k, rhs_k) {
            Ordering::Equal => B::compare(lhs_v, rhs_v),
            ord => ord,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PairColumn<A: AccessType, B: AccessType>(A::Column, B::Column);

impl<A: AccessType, B: AccessType> PairColumn<A, B> {
    pub fn len(&self) -> usize {
        A::column_len(&self.0)
    }

    pub fn index(&self, index: usize) -> Option<(A::ScalarRef<'_>, B::ScalarRef<'_>)> {
        Some((
            A::index_column(&self.0, index)?,
            B::index_column(&self.1, index)?,
        ))
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> (A::ScalarRef<'_>, B::ScalarRef<'_>) {
        (
            A::index_column_unchecked(&self.0, index),
            B::index_column_unchecked(&self.1, index),
        )
    }

    fn slice(&self, range: Range<usize>) -> Self {
        PairColumn(
            A::slice_column(&self.0, range.clone()),
            B::slice_column(&self.1, range),
        )
    }

    pub fn iter(&self) -> PairIterator<A, B> {
        PairIterator(A::iter_column(&self.0), B::iter_column(&self.1))
    }

    pub fn memory_size(&self) -> usize {
        A::column_memory_size(&self.0) + B::column_memory_size(&self.1)
    }
}

pub struct PairIterator<'a, A: AccessType, B: AccessType>(
    A::ColumnIterator<'a>,
    B::ColumnIterator<'a>,
);

impl<'a, A: AccessType, B: AccessType> Iterator for PairIterator<'a, A, B> {
    type Item = (A::ScalarRef<'a>, B::ScalarRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        Some((self.0.next()?, self.1.next()?))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        assert_eq!(self.0.size_hint(), self.1.size_hint());
        self.0.size_hint()
    }
}

unsafe impl<A: AccessType, B: AccessType> TrustedLen for PairIterator<'_, A, B> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Triple<A, B, C>(PhantomData<(A, B, C)>);
