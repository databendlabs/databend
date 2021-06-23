//! Implementations of the ArrayApply Trait.
use std::borrow::Cow;
use std::convert::TryFrom;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::LargeStringArray;
use common_arrow::arrow::array::PrimitiveArray;

use crate::arrays::kernels::*;
use crate::arrays::DataArrayBase;
use crate::utils::NoNull;
use crate::vec::AlignedVec;
use crate::*;

macro_rules! apply {
    ($self:expr, $f:expr) => {{
        if $self.null_count() == 0 {
            $self.into_no_null_iter().map($f).collect()
        } else {
            $self.into_iter().map(|opt_v| opt_v.map($f)).collect()
        }
    }};
}

macro_rules! apply_enumerate {
    ($self:expr, $f:expr) => {{
        if $self.null_count() == 0 {
            $self.into_no_null_iter().enumerate().map($f).collect()
        } else {
            $self
                .into_iter()
                .enumerate()
                .map(|(idx, opt_v)| opt_v.map(|v| $f((idx, v))))
                .collect()
        }
    }};
}

pub trait ArrayApplyKernel<A> {
    /// Apply kernel and return result as a new DataArrayBase.
    fn apply_kernel<F>(&self, f: F) -> Self
    where F: Fn(&A) -> ArrayRef;

    /// Apply a kernel that outputs an array of different type.
    fn apply_kernel_cast<F, S>(&self, f: F) -> DataArrayBase<S>
    where
        F: Fn(&A) -> ArrayRef,
        S: DFDataType;
}

pub trait ArrayApply<'a, A, B> {
    /// Apply a closure elementwise and cast to a Numeric DataArrayBase. This is fastest when the null check branching is more expensive
    /// than the closure application.
    ///
    /// Null values remain null.
    fn apply_cast_numeric<F, S>(&'a self, f: F) -> DataArrayBase<S>
    where
        F: Fn(A) -> S::Native + Copy,
        S: DFNumericType;

    /// Apply a closure on optional values and cast to Numeric DataArrayBase without null values.
    fn branch_apply_cast_numeric_no_null<F, S>(&'a self, f: F) -> DataArrayBase<S>
    where
        F: Fn(Option<A>) -> S::Native + Copy,
        S: DFNumericType;

    /// Apply a closure elementwise. This is fastest when the null check branching is more expensive
    /// than the closure application. Often it is.
    ///
    /// Null values remain null.
    ///
    /// # Example
    ///
    /// ```
    /// use DF_core::prelude::*;
    /// fn double(ca: &UInt32Chunked) -> UInt32Chunked {
    ///     ca.apply(|v| v * 2)
    /// }
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

impl<'a, T> ArrayApply<'a, T::Native, T::Native> for DataArrayBase<T>
where T: DFNumericType
{
    fn apply_cast_numeric<F, S>(&self, f: F) -> DataArrayBase<S>
    where
        F: Fn(T::Native) -> S::Native + Copy,
        S: DFNumericType,
    {
        let vec: AlignedVec<_> = self.data_views().map(|c| f(*c)).collect();
        let (_, buffer) = self.null_bits();

        let array = Arc::new(vec.into_primitive_array::<S>(buffer)) as ArrayRef;
        array.into()
    }

    fn branch_apply_cast_numeric_no_null<F, S>(&self, f: F) -> DataArrayBase<S>
    where
        F: Fn(Option<T::Native>) -> S::Native + Copy,
        S: DFNumericType,
    {
        let array = self.downcast_ref();
        let (_, buffer) = self.null_bits();

        let av: AlignedVec<_> = if array.null_count() == 0 {
            array.values().iter().map(|&v| f(Some(v))).collect()
        } else {
            array.into_iter().map(f).collect()
        };

        let array = Arc::new(av.into_primitive_array::<S>(buffer)) as ArrayRef;
        array.into()
    }

    fn apply<F>(&'a self, f: F) -> Self
    where F: Fn(T::Native) -> T::Native + Copy {
        let vec: AlignedVec<_> = self.data_views().map(|c| f(*c)).collect();
        let (_, buffer) = self.null_bits();

        let array = Arc::new(vec.into_primitive_array::<T>(buffer)) as ArrayRef;
        array.into()
    }

    fn apply_with_idx<F>(&'a self, f: F) -> Self
    where F: Fn((usize, T::Native)) -> T::Native + Copy {
        if self.null_count() == 0 {
            let ca: NoNull<_> = self.into_no_null_iter().enumerate().map(f).collect();
            ca.into_inner()
        } else {
            self.downcast_iter()
                .enumerate()
                .map(|(idx, opt_v)| opt_v.map(|v| f((idx, v))))
                .collect()
        }
    }

    fn apply_with_idx_on_opt<F>(&'a self, f: F) -> Self
    where F: Fn((usize, Option<T::Native>)) -> Option<T::Native> + Copy {
        self.downcast_iter().enumerate().map(f).collect()
    }
}

impl<'a> ArrayApply<'a, bool, bool> for DFBooleanArray {
    fn apply_cast_numeric<F, S>(&self, f: F) -> DataArrayBase<S>
    where
        F: Fn(bool) -> S::Native + Copy,
        S: DFNumericType,
    {
        self.apply_kernel_cast(|array| {
            let av: AlignedVec<_> = (0..array.len())
                .map(|idx| unsafe { f(array.value_unchecked(idx)) })
                .collect();
            let null_bit_buffer = array.data_ref().null_buffer().cloned();
            Arc::new(av.into_primitive_array::<S>(null_bit_buffer)) as ArrayRef
        })
    }

    fn branch_apply_cast_numeric_no_null<F, S>(&self, f: F) -> DataArrayBase<S>
    where
        F: Fn(Option<bool>) -> S::Native + Copy,
        S: DFNumericType,
    {
        self.apply_kernel_cast(|array| {
            let av: AlignedVec<_> = array.into_iter().map(f).collect();
            Arc::new(av.into_primitive_array::<S>(None)) as ArrayRef
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
        self.into_iter().enumerate().map(f).collect()
    }
}

impl<'a> ArrayApply<'a, &'a str, Cow<'a, str>> for DFStringArray {
    fn apply_cast_numeric<F, S>(&'a self, f: F) -> DataArrayBase<S>
    where
        F: Fn(&'a str) -> S::Native + Copy,
        S: DFNumericType,
    {
        let arr = self.downcast_ref();
        let av: AlignedVec<_> = (0..arr.len())
            .map(|idx| unsafe { f(arr.value_unchecked(idx)) })
            .collect();

        let null_bit_buffer = self.array.data_ref().null_buffer().cloned();
        let array = Arc::new(av.into_primitive_array::<S>(null_bit_buffer)) as ArrayRef;

        array.into()
    }

    fn branch_apply_cast_numeric_no_null<F, S>(&'a self, f: F) -> DataArrayBase<S>
    where
        F: Fn(Option<&'a str>) -> S::Native + Copy,
        S: DFNumericType,
    {
        let av: AlignedVec<_> = self.downcast_iter().map(f).collect();
        let null_bit_buffer = self.array.data_ref().null_buffer().cloned();
        let array = Arc::new(av.into_primitive_array::<S>(null_bit_buffer)) as ArrayRef;

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
        self.into_iter().enumerate().map(f).collect()
    }
}

impl ArrayApplyKernel<BooleanArray> for DFBooleanArray {
    fn apply_kernel<F>(&self, f: F) -> Self
    where F: Fn(&BooleanArray) -> ArrayRef {
        let array = self.downcast_ref();
        let array_ref = f(array);

        DFBooleanArray::from(array_ref)
    }

    fn apply_kernel_cast<F, S>(&self, f: F) -> DataArrayBase<S>
    where
        F: Fn(&BooleanArray) -> ArrayRef,
        S: DFDataType,
    {
        let chunks = self
            .downcast_iter()
            .into_iter()
            .map(|array| f(array))
            .collect();
        DataArrayBase::<S>::new_from_chunks(self.name(), chunks)
    }
}

impl<T> ArrayApplyKernel<PrimitiveArray<T>> for DataArrayBase<T>
where T: DFNumericType
{
    fn apply_kernel<F>(&self, f: F) -> Self
    where F: Fn(&PrimitiveArray<T>) -> ArrayRef {
        self.apply_kernel_cast(f)
    }
    fn apply_kernel_cast<F, S>(&self, f: F) -> DataArrayBase<S>
    where
        F: Fn(&PrimitiveArray<T>) -> ArrayRef,
        S: DFDataType,
    {
        let chunks = self.downcast_iter().into_iter().map(f).collect();
        DataArrayBase::new_from_chunks(self.name(), chunks)
    }
}

impl ArrayApplyKernel<LargeStringArray> for DFStringArray {
    fn apply_kernel<F>(&self, f: F) -> Self
    where F: Fn(&LargeStringArray) -> ArrayRef {
        self.apply_kernel_cast(f)
    }

    fn apply_kernel_cast<F, S>(&self, f: F) -> DataArrayBase<S>
    where
        F: Fn(&LargeStringArray) -> ArrayRef,
        S: DFDataType,
    {
        let chunks = self.downcast_iter().into_iter().map(f).collect();
        DataArrayBase::new_from_chunks(self.name(), chunks)
    }
}

impl<'a> ArrayApply<'a, Series, Series> for ListChunked {
    fn apply_cast_numeric<F, S>(&self, f: F) -> DataArrayBase<S>
    where
        F: Fn(Series) -> S::Native + Copy,
        S: DFNumericType,
    {
        let chunks = self
            .downcast_iter()
            .into_iter()
            .map(|array| {
                let av: AlignedVec<_> = (0..array.len())
                    .map(|idx| {
                        let arrayref = unsafe { array.value_unchecked(idx) };
                        let series = Series::try_from(("", arrayref)).unwrap();
                        f(series)
                    })
                    .collect();
                let null_bit_buffer = array.data_ref().null_buffer().cloned();
                Arc::new(av.into_primitive_array::<S>(null_bit_buffer)) as ArrayRef
            })
            .collect();
        DataArrayBase::new_from_chunks(self.name(), chunks)
    }

    fn branch_apply_cast_numeric_no_null<F, S>(&self, f: F) -> DataArrayBase<S>
    where
        F: Fn(Option<Series>) -> S::Native + Copy,
        S: DFNumericType,
    {
        let chunks = self
            .downcast_iter()
            .into_iter()
            .map(|array| {
                let av: AlignedVec<_> = (0..array.len())
                    .map(|idx| {
                        let v = if array.is_valid(idx) {
                            let arrayref = unsafe { array.value_unchecked(idx) };
                            let series = Series::try_from(("", arrayref)).unwrap();
                            Some(series)
                        } else {
                            None
                        };

                        f(v)
                    })
                    .collect();
                let null_bit_buffer = array.data_ref().null_buffer().cloned();
                Arc::new(av.into_primitive_array::<S>(null_bit_buffer)) as ArrayRef
            })
            .collect();
        DataArrayBase::new_from_chunks(self.name(), chunks)
    }

    /// Apply a closure `F` elementwise.
    fn apply<F>(&'a self, f: F) -> Self
    where F: Fn(Series) -> Series + Copy {
        apply!(self, f)
    }

    /// Apply a closure elementwise. The closure gets the index of the element as first argument.
    fn apply_with_idx<F>(&'a self, f: F) -> Self
    where F: Fn((usize, Series)) -> Series + Copy {
        apply_enumerate!(self, f)
    }

    /// Apply a closure elementwise. The closure gets the index of the element as first argument.
    fn apply_with_idx_on_opt<F>(&'a self, f: F) -> Self
    where F: Fn((usize, Option<Series>)) -> Option<Series> + Copy {
        self.into_iter().enumerate().map(f).collect()
    }
}
