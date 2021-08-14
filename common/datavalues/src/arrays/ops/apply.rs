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
use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::compute::arity::unary;

use crate::arrays::DataArray;
use crate::prelude::*;
use crate::utils::NoNull;

macro_rules! apply {
    ($self:expr, $f:expr) => {{
        if $self.null_count() == 0 {
            $self.into_no_null_iter().map($f).collect()
        } else {
            $self.downcast_iter().map(|opt_v| opt_v.map($f)).collect()
        }
    }};
}

macro_rules! apply_enumerate {
    ($self:expr, $f:expr) => {{
        if $self.null_count() == 0 {
            $self.into_no_null_iter().enumerate().map($f).collect()
        } else {
            $self
                .downcast_iter()
                .enumerate()
                .map(|(idx, opt_v)| opt_v.map(|v| $f((idx, v))))
                .collect()
        }
    }};
}

pub trait ArrayApplyKernel<A> {
    /// Apply kernel and return result as a new DataArray.
    fn apply_kernel<F>(&self, f: F) -> Self
    where F: Fn(&A) -> ArrayRef;

    /// Apply a kernel that outputs an array of different type.
    fn apply_kernel_cast<F, S>(&self, f: F) -> DataArray<S>
    where
        F: Fn(&A) -> ArrayRef,
        S: DFDataType;
}

pub trait ArrayApply<'a, A, B> {
    /// Apply a closure elementwise and cast to a Numeric DataArray. This is fastest when the null check branching is more expensive
    /// than the closure application.
    ///
    /// Null values remain null.
    fn apply_cast_numeric<F, S>(&'a self, f: F) -> DataArray<S>
    where
        F: Fn(A) -> S::Native + Copy,
        S: DFNumericType;

    /// Apply a closure on optional values and cast to Numeric DataArray without null values.
    fn branch_apply_cast_numeric_no_null<F, S>(&'a self, f: F) -> DataArray<S>
    where
        F: Fn(Option<A>) -> S::Native + Copy,
        S: DFNumericType;

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

impl<'a, T> ArrayApply<'a, T::Native, T::Native> for DataArray<T>
where T: DFNumericType
{
    fn apply_cast_numeric<F, S>(&self, f: F) -> DataArray<S>
    where
        F: Fn(T::Native) -> S::Native + Copy,
        S: DFNumericType,
    {
        let array = unary(self.downcast_ref(), |n| f(n), S::data_type().to_arrow());
        DataArray::<S>::from_arrow_array(array)
    }

    fn branch_apply_cast_numeric_no_null<F, S>(&self, f: F) -> DataArray<S>
    where
        F: Fn(Option<T::Native>) -> S::Native + Copy,
        S: DFNumericType,
    {
        let array = unary(
            self.downcast_ref(),
            |n| f(Some(n)),
            S::data_type().to_arrow(),
        );
        DataArray::<S>::from_arrow_array(array)
    }

    fn apply<F>(&'a self, f: F) -> Self
    where F: Fn(T::Native) -> T::Native + Copy {
        let array = unary(self.downcast_ref(), |n| f(n), T::data_type().to_arrow());
        DataArray::<T>::from_arrow_array(array)
    }

    fn apply_with_idx<F>(&'a self, f: F) -> Self
    where F: Fn((usize, T::Native)) -> T::Native + Copy {
        if self.null_count() == 0 {
            let ca: NoNull<_> = self
                .into_no_null_iter()
                .enumerate()
                .map(f)
                .trust_my_length(self.len())
                .collect_trusted();
            ca.into_inner()
        } else {
            self.into_iter()
                .enumerate()
                .map(|(idx, opt_v)| opt_v.map(|v| f((idx, v))))
                .trust_my_length(self.len())
                .collect_trusted()
        }
    }

    fn apply_with_idx_on_opt<F>(&'a self, f: F) -> Self
    where F: Fn((usize, Option<T::Native>)) -> Option<T::Native> + Copy {
        self.into_iter()
            .enumerate()
            .map(f)
            .trust_my_length(self.len())
            .collect_trusted()
    }
}

impl<'a> ArrayApply<'a, bool, bool> for DFBooleanArray {
    fn apply_cast_numeric<F, S>(&self, f: F) -> DataArray<S>
    where
        F: Fn(bool) -> S::Native + Copy,
        S: DFNumericType,
    {
        self.apply_kernel_cast(|array| {
            let values = array.values().iter().map(f);
            let values = AlignedVec::<_>::from_trusted_len_iter(values);
            let validity = array.validity().clone();
            let arr = to_primitive::<S>(values, validity);
            Arc::new(arr)
        })
    }

    fn branch_apply_cast_numeric_no_null<F, S>(&self, f: F) -> DataArray<S>
    where
        F: Fn(Option<bool>) -> S::Native + Copy,
        S: DFNumericType,
    {
        self.apply_kernel_cast(|array| {
            let av: AlignedVec<_> = array
                .into_iter()
                .map(f)
                .trust_my_length(self.len())
                .collect();
            Arc::new(to_primitive::<S>(av, None)) as ArrayRef
        })
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
        self.downcast_iter().enumerate().map(f).collect()
    }
}

impl<'a> ArrayApply<'a, &'a str, Cow<'a, str>> for DFUtf8Array {
    fn apply_cast_numeric<F, S>(&'a self, f: F) -> DataArray<S>
    where
        F: Fn(&'a str) -> S::Native + Copy,
        S: DFNumericType,
    {
        let arr = self.downcast_ref();
        let values_iter = arr.values_iter().map(|x| f(x));
        let av = AlignedVec::<_>::from_trusted_len_iter(values_iter);

        let (_, validity) = self.null_bits();
        let array = Arc::new(to_primitive::<S>(av, validity.clone())) as ArrayRef;
        array.into()
    }

    fn branch_apply_cast_numeric_no_null<F, S>(&'a self, f: F) -> DataArray<S>
    where
        F: Fn(Option<&'a str>) -> S::Native + Copy,
        S: DFNumericType,
    {
        let av: AlignedVec<_> = AlignedVec::<_>::from_trusted_len_iter(self.downcast_iter().map(f));
        let (_, validity) = self.null_bits();
        let array = Arc::new(to_primitive::<S>(av, validity.clone())) as ArrayRef;
        array.into()
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
        self.downcast_iter().enumerate().map(f).collect()
    }
}

impl ArrayApplyKernel<BooleanArray> for DFBooleanArray {
    fn apply_kernel<F>(&self, f: F) -> Self
    where F: Fn(&BooleanArray) -> ArrayRef {
        let array = self.downcast_ref();
        let array_ref = f(array);

        DFBooleanArray::from(array_ref)
    }

    fn apply_kernel_cast<F, S>(&self, f: F) -> DataArray<S>
    where
        F: Fn(&BooleanArray) -> ArrayRef,
        S: DFDataType,
    {
        let array = self.downcast_ref();
        let array_ref = f(array);
        DataArray::<S>::from(array_ref)
    }
}

impl<T> ArrayApplyKernel<PrimitiveArray<T::Native>> for DataArray<T>
where T: DFNumericType
{
    fn apply_kernel<F>(&self, f: F) -> Self
    where F: Fn(&PrimitiveArray<T::Native>) -> ArrayRef {
        self.apply_kernel_cast(f)
    }
    fn apply_kernel_cast<F, S>(&self, f: F) -> DataArray<S>
    where
        F: Fn(&PrimitiveArray<T::Native>) -> ArrayRef,
        S: DFDataType,
    {
        let array = self.downcast_ref();
        let array_ref = f(array);
        DataArray::<S>::from(array_ref)
    }
}

impl ArrayApplyKernel<LargeUtf8Array> for DFUtf8Array {
    fn apply_kernel<F>(&self, f: F) -> Self
    where F: Fn(&LargeUtf8Array) -> ArrayRef {
        self.apply_kernel_cast(f)
    }

    fn apply_kernel_cast<F, S>(&self, f: F) -> DataArray<S>
    where
        F: Fn(&LargeUtf8Array) -> ArrayRef,
        S: DFDataType,
    {
        let array = self.downcast_ref();
        let array_ref = f(array);
        DataArray::<S>::from(array_ref)
    }
}
