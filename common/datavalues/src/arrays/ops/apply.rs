// Copyright 2020 Datafuse Labs.
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
use std::borrow::Cow;

use common_arrow::arrow::array::*;
use common_arrow::arrow::compute::arity::unary;

use crate::prelude::*;
use crate::utils::NoNull;

macro_rules! apply {
    ($self:expr, $f:expr) => {{
        if $self.null_count() == 0 {
            $self.into_no_null_iter().map($f).collect()
        } else {
            $self.inner().iter().map(|opt_v| opt_v.map($f)).collect()
        }
    }};
}

macro_rules! apply_enumerate {
    ($self:expr, $f:expr) => {{
        if $self.null_count() == 0 {
            $self.into_no_null_iter().enumerate().map($f).collect()
        } else {
            $self
                .inner()
                .iter()
                .enumerate()
                .map(|(idx, opt_v)| opt_v.map(|v| $f((idx, v))))
                .collect()
        }
    }};
}

pub trait ArrayApply<'a, A, B> {
    /// Apply a closure elementwise and cast to a Numeric DFPrimitiveArray. This is fastest when the null check branching is more expensive
    /// than the closure application.
    ///
    /// Null values remain null.
    fn apply_cast_numeric<F, S>(&'a self, f: F) -> DFPrimitiveArray<S>
    where
        F: Fn(A) -> S + Copy,
        S: DFPrimitiveType;

    /// Apply a closure on optional values and cast to Numeric DFPrimitiveArray without null values.
    fn branch_apply_cast_numeric_no_null<F, S>(&'a self, f: F) -> DFPrimitiveArray<S>
    where
        F: Fn(Option<A>) -> S + Copy,
        S: DFPrimitiveType;

    /// Apply a closure elementwise. This is fastest when the null check branching is more expensive
    /// than the closure application. Often it is.
    ///
    /// Null values remain null.
    ///
    /// ```
    fn apply<F>(&'a self, f: F) -> Self
    where F: Fn(A) -> B + Copy;

    /// Apply a closure elementwise. The closure gets the index of the element as first argument.
    fn apply_with_idx<F>(&'a self, f: F) -> Self
    where F: Fn((usize, A)) -> B + Copy;

    /// Apply a closure elementwise. The closure gets the index of the element as first argument.
    fn apply_with_idx_on_opt<F>(&'a self, f: F) -> Self
    where F: Fn((usize, Option<A>)) -> Option<B> + Copy;
}

impl<'a, T> ArrayApply<'a, T, T> for DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    fn apply_cast_numeric<F, S>(&self, f: F) -> DFPrimitiveArray<S>
    where
        F: Fn(T) -> S + Copy,
        S: DFPrimitiveType,
    {
        let array = unary(self.inner(), f, S::data_type().to_arrow());
        DFPrimitiveArray::<S>::new(array)
    }

    fn branch_apply_cast_numeric_no_null<F, S>(&self, f: F) -> DFPrimitiveArray<S>
    where
        F: Fn(Option<T>) -> S + Copy,
        S: DFPrimitiveType,
    {
        let array = unary(self.inner(), |n| f(Some(n)), S::data_type().to_arrow());
        DFPrimitiveArray::<S>::new(array)
    }

    fn apply<F>(&'a self, f: F) -> Self
    where F: Fn(T) -> T + Copy {
        let array = unary(self.inner(), f, T::data_type().to_arrow());
        DFPrimitiveArray::<T>::new(array)
    }

    fn apply_with_idx<F>(&'a self, f: F) -> Self
    where F: Fn((usize, T)) -> T + Copy {
        if self.null_count() == 0 {
            let ca: NoNull<_> = self
                .into_no_null_iter()
                .enumerate()
                .map(|(idx, v)| f((idx, *v)))
                .collect_trusted();
            ca.into_inner()
        } else {
            self.into_iter()
                .enumerate()
                .map(|(idx, opt_v)| opt_v.map(|v| f((idx, *v))))
                .collect_trusted()
        }
    }

    fn apply_with_idx_on_opt<F>(&'a self, f: F) -> Self
    where F: Fn((usize, Option<T>)) -> Option<T> + Copy {
        self.into_iter()
            .enumerate()
            .map(|(idx, v)| f((idx, v.copied())))
            .collect_trusted()
    }
}

impl<'a> ArrayApply<'a, bool, bool> for DFBooleanArray {
    fn apply_cast_numeric<F, S>(&self, f: F) -> DFPrimitiveArray<S>
    where
        F: Fn(bool) -> S + Copy,
        S: DFPrimitiveType,
    {
        let values = self.array.values().iter().map(f);
        let values = AlignedVec::<_>::from_trusted_len_iter(values);
        let validity = self.array.validity().clone();
        to_primitive::<S>(values, validity)
    }

    fn branch_apply_cast_numeric_no_null<F, S>(&self, f: F) -> DFPrimitiveArray<S>
    where
        F: Fn(Option<bool>) -> S + Copy,
        S: DFPrimitiveType,
    {
        let av: AlignedVec<_> = self.into_iter().map(f).collect();
        to_primitive::<S>(av, None)
    }

    fn apply<F>(&self, f: F) -> Self
    where F: Fn(bool) -> bool + Copy {
        apply!(self, f)
    }
    fn apply_with_idx<F>(&'a self, f: F) -> Self
    where F: Fn((usize, bool)) -> bool + Copy {
        apply_enumerate!(self, f)
    }

    fn apply_with_idx_on_opt<F>(&'a self, f: F) -> Self
    where F: Fn((usize, Option<bool>)) -> Option<bool> + Copy {
        self.into_iter().enumerate().map(f).collect()
    }
}

impl<'a> ArrayApply<'a, &'a str, Cow<'a, str>> for DFUtf8Array {
    fn apply_cast_numeric<F, S>(&'a self, f: F) -> DFPrimitiveArray<S>
    where
        F: Fn(&'a str) -> S + Copy,
        S: DFPrimitiveType,
    {
        let arr = self.inner();
        let values_iter = arr.values_iter().map(|x| f(x));
        let av = AlignedVec::<_>::from_trusted_len_iter(values_iter);

        let (_, validity) = self.null_bits();
        to_primitive::<S>(av, validity.clone())
    }

    fn branch_apply_cast_numeric_no_null<F, S>(&'a self, f: F) -> DFPrimitiveArray<S>
    where
        F: Fn(Option<&'a str>) -> S + Copy,
        S: DFPrimitiveType,
    {
        let av: AlignedVec<_> = AlignedVec::<_>::from_trusted_len_iter(self.inner().iter().map(f));
        let (_, validity) = self.null_bits();
        to_primitive::<S>(av, validity.clone())
    }

    fn apply<F>(&'a self, f: F) -> Self
    where F: Fn(&'a str) -> Cow<'a, str> + Copy {
        apply!(self, f)
    }
    fn apply_with_idx<F>(&'a self, f: F) -> Self
    where F: Fn((usize, &'a str)) -> Cow<'a, str> + Copy {
        apply_enumerate!(self, f)
    }

    fn apply_with_idx_on_opt<F>(&'a self, f: F) -> Self
    where F: Fn((usize, Option<&'a str>)) -> Option<Cow<'a, str>> + Copy {
        self.into_iter().enumerate().map(f).collect()
    }
}
