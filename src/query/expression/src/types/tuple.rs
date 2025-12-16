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
use std::iter::Map;
use std::iter::Zip;
use std::marker::PhantomData;

use databend_common_exception::Result;

use super::AccessType;
use super::AnyType;
use super::column_type_error;
use super::domain_type_error;
use super::scalar_type_error;
use crate::Column;
use crate::Domain;
use crate::ScalarRef;

type Iter<'a, T> = <T as AccessType>::ColumnIterator<'a>;

type SR<'a, T> = <T as AccessType>::ScalarRef<'a>;

pub type AnyUnaryType = UnaryType<AnyType>;
pub type AnyPairType = PairType<AnyType, AnyType>;
pub type AnyTernaryType = TernaryType<AnyType, AnyType, AnyType>;
pub type AnyQuaternaryType = QuaternaryType<AnyType, AnyType, AnyType, AnyType>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnaryType<T>(PhantomData<T>);

impl<A> AccessType for UnaryType<A>
where A: AccessType
{
    type Scalar = A::Scalar;
    type ScalarRef<'a> = A::ScalarRef<'a>;
    type Column = A::Column;
    type Domain = A::Domain;
    type ColumnIterator<'a> = A::ColumnIterator<'a>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        A::to_owned_scalar(scalar)
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        A::to_scalar_ref(scalar)
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        let Some(tuple) = scalar.as_tuple() else {
            return Err(scalar_type_error::<Self>(scalar));
        };
        let [a] = tuple.as_slice() else {
            return Err(scalar_type_error::<Self>(scalar));
        };
        A::try_downcast_scalar(a)
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        let Some(tuple) = domain.as_tuple() else {
            return Err(domain_type_error::<Self>(domain));
        };
        let [a] = tuple.as_slice() else {
            return Err(domain_type_error::<Self>(domain));
        };
        A::try_downcast_domain(a)
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        let Some(tuple) = col.as_tuple() else {
            return Err(column_type_error::<Self>(col));
        };
        let [a] = tuple.as_slice() else {
            return Err(column_type_error::<Self>(col));
        };
        A::try_downcast_column(a)
    }

    fn column_len(col: &Self::Column) -> usize {
        A::column_len(col)
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        A::index_column(col, index)
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        unsafe { A::index_column_unchecked(col, index) }
    }

    fn slice_column(col: &Self::Column, range: std::ops::Range<usize>) -> Self::Column {
        A::slice_column(col, range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        A::iter_column(col)
    }

    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        A::compare(lhs, rhs)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairType<A, B>(PhantomData<(A, B)>);

impl<A, B> AccessType for PairType<A, B>
where
    A: AccessType,
    B: AccessType,
{
    type Scalar = (A::Scalar, B::Scalar);
    type ScalarRef<'a> = (A::ScalarRef<'a>, B::ScalarRef<'a>);
    type Column = (A::Column, B::Column);
    type Domain = (A::Domain, B::Domain);
    type ColumnIterator<'a> = Zip<Iter<'a, A>, Iter<'a, B>>;

    fn to_owned_scalar((a, b): Self::ScalarRef<'_>) -> Self::Scalar {
        (A::to_owned_scalar(a), B::to_owned_scalar(b))
    }

    fn to_scalar_ref((a, b): &Self::Scalar) -> Self::ScalarRef<'_> {
        (A::to_scalar_ref(a), B::to_scalar_ref(b))
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        let Some(tuple) = scalar.as_tuple() else {
            return Err(scalar_type_error::<Self>(scalar));
        };
        let [a, b] = tuple.as_slice() else {
            return Err(scalar_type_error::<Self>(scalar));
        };
        Ok((A::try_downcast_scalar(a)?, B::try_downcast_scalar(b)?))
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        let Some(tuple) = domain.as_tuple() else {
            return Err(domain_type_error::<Self>(domain));
        };
        let [a, b] = tuple.as_slice() else {
            return Err(domain_type_error::<Self>(domain));
        };
        Ok((A::try_downcast_domain(a)?, B::try_downcast_domain(b)?))
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        let Some(tuple) = col.as_tuple() else {
            return Err(column_type_error::<Self>(col));
        };
        let [a, b] = tuple.as_slice() else {
            return Err(column_type_error::<Self>(col));
        };
        Ok((A::try_downcast_column(a)?, B::try_downcast_column(b)?))
    }

    fn column_len((a, _b): &Self::Column) -> usize {
        debug_assert_eq!(A::column_len(a), B::column_len(_b));
        A::column_len(a)
    }

    fn index_column((a, b): &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        Some((A::index_column(a, index)?, B::index_column(b, index)?))
    }

    unsafe fn index_column_unchecked((a, b): &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        unsafe {
            (
                A::index_column_unchecked(a, index),
                B::index_column_unchecked(b, index),
            )
        }
    }

    fn slice_column((a, b): &Self::Column, range: std::ops::Range<usize>) -> Self::Column {
        (A::slice_column(a, range.clone()), B::slice_column(b, range))
    }

    fn iter_column((a, b): &Self::Column) -> Self::ColumnIterator<'_> {
        A::iter_column(a).zip(B::iter_column(b))
    }

    fn compare(
        (lhs_a, lhs_b): Self::ScalarRef<'_>,
        (rhs_a, rhs_b): Self::ScalarRef<'_>,
    ) -> Ordering {
        A::compare(lhs_a, rhs_a).then_with(|| B::compare(lhs_b, rhs_b))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TernaryType<A, B, C>(PhantomData<(A, B, C)>);

impl<A, B, C> AccessType for TernaryType<A, B, C>
where
    A: AccessType,
    B: AccessType,
    C: AccessType,
{
    type Scalar = (A::Scalar, B::Scalar, C::Scalar);
    type ScalarRef<'a> = (A::ScalarRef<'a>, B::ScalarRef<'a>, C::ScalarRef<'a>);
    type Column = (A::Column, B::Column, C::Column);
    type Domain = (A::Domain, B::Domain, C::Domain);
    type ColumnIterator<'a> = Map<
        Zip<Zip<Iter<'a, A>, Iter<'a, B>>, Iter<'a, C>>,
        fn(((SR<'a, A>, SR<'a, B>), SR<'a, C>)) -> Self::ScalarRef<'a>,
    >;

    fn to_owned_scalar((a, b, c): Self::ScalarRef<'_>) -> Self::Scalar {
        (
            A::to_owned_scalar(a),
            B::to_owned_scalar(b),
            C::to_owned_scalar(c),
        )
    }

    fn to_scalar_ref((a, b, c): &Self::Scalar) -> Self::ScalarRef<'_> {
        (
            A::to_scalar_ref(a),
            B::to_scalar_ref(b),
            C::to_scalar_ref(c),
        )
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        let Some(tuple) = scalar.as_tuple() else {
            return Err(scalar_type_error::<Self>(scalar));
        };
        let [a, b, c] = tuple.as_slice() else {
            return Err(scalar_type_error::<Self>(scalar));
        };
        Ok((
            A::try_downcast_scalar(a)?,
            B::try_downcast_scalar(b)?,
            C::try_downcast_scalar(c)?,
        ))
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        let Some(tuple) = domain.as_tuple() else {
            return Err(domain_type_error::<Self>(domain));
        };
        let [a, b, c] = tuple.as_slice() else {
            return Err(domain_type_error::<Self>(domain));
        };
        Ok((
            A::try_downcast_domain(a)?,
            B::try_downcast_domain(b)?,
            C::try_downcast_domain(c)?,
        ))
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        let Some(tuple) = col.as_tuple() else {
            return Err(column_type_error::<Self>(col));
        };
        let [a, b, c] = tuple.as_slice() else {
            return Err(column_type_error::<Self>(col));
        };
        Ok((
            A::try_downcast_column(a)?,
            B::try_downcast_column(b)?,
            C::try_downcast_column(c)?,
        ))
    }

    fn column_len((a, _b, _c): &Self::Column) -> usize {
        debug_assert_eq!(A::column_len(a), B::column_len(_b));
        debug_assert_eq!(A::column_len(a), C::column_len(_c));
        A::column_len(a)
    }

    fn index_column((a, b, c): &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        Some((
            A::index_column(a, index)?,
            B::index_column(b, index)?,
            C::index_column(c, index)?,
        ))
    }

    unsafe fn index_column_unchecked(
        (a, b, c): &Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'_> {
        unsafe {
            (
                A::index_column_unchecked(a, index),
                B::index_column_unchecked(b, index),
                C::index_column_unchecked(c, index),
            )
        }
    }

    fn slice_column((a, b, c): &Self::Column, range: std::ops::Range<usize>) -> Self::Column {
        (
            A::slice_column(a, range.clone()),
            B::slice_column(b, range.clone()),
            C::slice_column(c, range),
        )
    }

    fn iter_column((a, b, c): &Self::Column) -> Self::ColumnIterator<'_> {
        A::iter_column(a)
            .zip(B::iter_column(b))
            .zip(C::iter_column(c))
            .map(|((a, b), c)| (a, b, c))
    }

    fn compare(
        (lhs_a, lhs_b, lhs_c): Self::ScalarRef<'_>,
        (rhs_a, rhs_b, rhs_c): Self::ScalarRef<'_>,
    ) -> Ordering {
        A::compare(lhs_a, rhs_a)
            .then_with(|| B::compare(lhs_b, rhs_b))
            .then_with(|| C::compare(lhs_c, rhs_c))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuaternaryType<A, B, C, D>(PhantomData<(A, B, C, D)>);

impl<A, B, C, D> AccessType for QuaternaryType<A, B, C, D>
where
    A: AccessType,
    B: AccessType,
    C: AccessType,
    D: AccessType,
{
    type Scalar = (A::Scalar, B::Scalar, C::Scalar, D::Scalar);
    type ScalarRef<'a> = (SR<'a, A>, SR<'a, B>, SR<'a, C>, SR<'a, D>);
    type Column = (A::Column, B::Column, C::Column, D::Column);
    type Domain = (A::Domain, B::Domain, C::Domain, D::Domain);
    type ColumnIterator<'a> = Map<
        Zip<Zip<Zip<Iter<'a, A>, Iter<'a, B>>, Iter<'a, C>>, Iter<'a, D>>,
        fn((((SR<'a, A>, SR<'a, B>), SR<'a, C>), SR<'a, D>)) -> Self::ScalarRef<'a>,
    >;

    fn to_owned_scalar((a, b, c, d): Self::ScalarRef<'_>) -> Self::Scalar {
        (
            A::to_owned_scalar(a),
            B::to_owned_scalar(b),
            C::to_owned_scalar(c),
            D::to_owned_scalar(d),
        )
    }

    fn to_scalar_ref((a, b, c, d): &Self::Scalar) -> Self::ScalarRef<'_> {
        (
            A::to_scalar_ref(a),
            B::to_scalar_ref(b),
            C::to_scalar_ref(c),
            D::to_scalar_ref(d),
        )
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        let Some(tuple) = scalar.as_tuple() else {
            return Err(scalar_type_error::<Self>(scalar));
        };
        let [a, b, c, d] = tuple.as_slice() else {
            return Err(scalar_type_error::<Self>(scalar));
        };
        Ok((
            A::try_downcast_scalar(a)?,
            B::try_downcast_scalar(b)?,
            C::try_downcast_scalar(c)?,
            D::try_downcast_scalar(d)?,
        ))
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        let Some(tuple) = domain.as_tuple() else {
            return Err(domain_type_error::<Self>(domain));
        };
        let [a, b, c, d] = tuple.as_slice() else {
            return Err(domain_type_error::<Self>(domain));
        };
        Ok((
            A::try_downcast_domain(a)?,
            B::try_downcast_domain(b)?,
            C::try_downcast_domain(c)?,
            D::try_downcast_domain(d)?,
        ))
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        let Some(tuple) = col.as_tuple() else {
            return Err(column_type_error::<Self>(col));
        };
        let [a, b, c, d] = tuple.as_slice() else {
            return Err(column_type_error::<Self>(col));
        };
        Ok((
            A::try_downcast_column(a)?,
            B::try_downcast_column(b)?,
            C::try_downcast_column(c)?,
            D::try_downcast_column(d)?,
        ))
    }

    fn column_len((a, _b, _c, _d): &Self::Column) -> usize {
        debug_assert_eq!(A::column_len(a), B::column_len(_b));
        debug_assert_eq!(A::column_len(a), C::column_len(_c));
        debug_assert_eq!(A::column_len(a), D::column_len(_d));
        A::column_len(a)
    }

    fn index_column((a, b, c, d): &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        Some((
            A::index_column(a, index)?,
            B::index_column(b, index)?,
            C::index_column(c, index)?,
            D::index_column(d, index)?,
        ))
    }

    unsafe fn index_column_unchecked(
        (a, b, c, d): &Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'_> {
        unsafe {
            (
                A::index_column_unchecked(a, index),
                B::index_column_unchecked(b, index),
                C::index_column_unchecked(c, index),
                D::index_column_unchecked(d, index),
            )
        }
    }

    fn slice_column((a, b, c, d): &Self::Column, range: std::ops::Range<usize>) -> Self::Column {
        (
            A::slice_column(a, range.clone()),
            B::slice_column(b, range.clone()),
            C::slice_column(c, range.clone()),
            D::slice_column(d, range),
        )
    }

    fn iter_column((a, b, c, d): &Self::Column) -> Self::ColumnIterator<'_> {
        A::iter_column(a)
            .zip(B::iter_column(b))
            .zip(C::iter_column(c))
            .zip(D::iter_column(d))
            .map(|(((a, b), c), d)| (a, b, c, d))
    }

    fn compare(
        (lhs_a, lhs_b, lhs_c, lhs_d): Self::ScalarRef<'_>,
        (rhs_a, rhs_b, rhs_c, rhs_d): Self::ScalarRef<'_>,
    ) -> Ordering {
        A::compare(lhs_a, rhs_a)
            .then_with(|| B::compare(lhs_b, rhs_b))
            .then_with(|| C::compare(lhs_c, rhs_c))
            .then_with(|| D::compare(lhs_d, rhs_d))
    }
}
