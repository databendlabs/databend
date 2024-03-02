// Copyright (c) 2020 Ritchie Vink
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

mod ffi;
pub(crate) mod fmt;
mod from;
mod iterator;
mod mutable;
mod view;

mod private {
    pub trait Sealed: Send + Sync {}

    impl Sealed for str {}

    impl Sealed for [u8] {}
}

use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use either::Either;
pub use iterator::BinaryViewValueIter;
pub use mutable::MutableBinaryViewArray;
use private::Sealed;
pub use view::View;

use crate::arrow::array::binview::view::validate_binary_view;
use crate::arrow::array::binview::view::validate_utf8_only;
use crate::arrow::array::binview::view::validate_utf8_view;
use crate::arrow::array::iterator::NonNullValuesIter;
use crate::arrow::array::Array;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::utils::ZipValidity;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::buffer::Buffer;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Error;
use crate::arrow::error::Result;

static BIN_VIEW_TYPE: DataType = DataType::BinaryView;
static UTF8_VIEW_TYPE: DataType = DataType::Utf8View;

const UNKNOWN_LEN: u64 = u64::MAX;

pub trait ViewType: Sealed + 'static + PartialEq + AsRef<Self> {
    const IS_UTF8: bool;
    const DATA_TYPE: DataType;
    type Owned: Debug + Clone + Sync + Send + AsRef<Self>;

    /// # Safety
    /// The caller must ensure `index < self.len()`.
    unsafe fn from_bytes_unchecked(slice: &[u8]) -> &Self;

    fn to_bytes(&self) -> &[u8];

    #[allow(clippy::wrong_self_convention)]
    fn into_owned(&self) -> Self::Owned;

    fn data_type() -> &'static DataType;
}

impl ViewType for str {
    const IS_UTF8: bool = true;
    const DATA_TYPE: DataType = DataType::Utf8View;
    type Owned = String;

    #[inline(always)]
    unsafe fn from_bytes_unchecked(slice: &[u8]) -> &Self {
        std::str::from_utf8_unchecked(slice)
    }

    #[inline(always)]
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    fn into_owned(&self) -> Self::Owned {
        self.to_string()
    }

    fn data_type() -> &'static DataType {
        &UTF8_VIEW_TYPE
    }
}

impl ViewType for [u8] {
    const IS_UTF8: bool = false;
    const DATA_TYPE: DataType = DataType::BinaryView;
    type Owned = Vec<u8>;

    #[inline(always)]
    unsafe fn from_bytes_unchecked(slice: &[u8]) -> &Self {
        slice
    }

    #[inline(always)]
    fn to_bytes(&self) -> &[u8] {
        self
    }

    fn into_owned(&self) -> Self::Owned {
        self.to_vec()
    }

    fn data_type() -> &'static DataType {
        &BIN_VIEW_TYPE
    }
}

pub struct BinaryViewArrayGeneric<T: ViewType + ?Sized> {
    data_type: DataType,
    views: Buffer<View>,
    buffers: Arc<[Buffer<u8>]>,
    // Raw buffer access. (pointer, len).
    raw_buffers: Arc<[(*const u8, usize)]>,
    validity: Option<Bitmap>,
    phantom: PhantomData<T>,
    /// Total bytes length if we would concat them all
    total_bytes_len: AtomicU64,
    /// Total bytes in the buffer (exclude remaining capacity)
    total_buffer_len: usize,
}

impl<T: ViewType + ?Sized> PartialEq for BinaryViewArrayGeneric<T> {
    fn eq(&self, other: &Self) -> bool {
        self.into_iter().zip(other).all(|(l, r)| l == r)
    }
}

impl<T: ViewType + ?Sized> Clone for BinaryViewArrayGeneric<T> {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            views: self.views.clone(),
            buffers: self.buffers.clone(),
            raw_buffers: self.raw_buffers.clone(),
            validity: self.validity.clone(),
            phantom: Default::default(),
            total_bytes_len: AtomicU64::new(self.total_bytes_len.load(Ordering::Relaxed)),
            total_buffer_len: self.total_buffer_len,
        }
    }
}

unsafe impl<T: ViewType + ?Sized> Send for BinaryViewArrayGeneric<T> {}

unsafe impl<T: ViewType + ?Sized> Sync for BinaryViewArrayGeneric<T> {}

fn buffers_into_raw<T>(buffers: &[Buffer<T>]) -> Arc<[(*const T, usize)]> {
    buffers
        .iter()
        .map(|buf| (buf.data_ptr(), buf.len()))
        .collect()
}

impl<T: ViewType + ?Sized> BinaryViewArrayGeneric<T> {
    pub fn new_unchecked(
        data_type: DataType,
        views: Buffer<View>,
        buffers: Arc<[Buffer<u8>]>,
        validity: Option<Bitmap>,
        total_bytes_len: usize,
        total_buffer_len: usize,
    ) -> Self {
        let raw_buffers = buffers_into_raw(&buffers);
        // # Safety
        // The caller must ensure
        // - the data is valid utf8 (if required)
        // - the offsets match the buffers.
        Self {
            data_type,
            views,
            buffers,
            raw_buffers,
            validity,
            phantom: Default::default(),
            total_bytes_len: AtomicU64::new(total_bytes_len as u64),
            total_buffer_len,
        }
    }

    /// Create a new BinaryViewArray but initialize a statistics compute.
    /// # Safety
    /// The caller must ensure the invariants
    pub unsafe fn new_unchecked_unknown_md(
        data_type: DataType,
        views: Buffer<View>,
        buffers: Arc<[Buffer<u8>]>,
        validity: Option<Bitmap>,
        total_buffer_len: Option<usize>,
    ) -> Self {
        let total_bytes_len = UNKNOWN_LEN as usize;
        let total_buffer_len =
            total_buffer_len.unwrap_or_else(|| buffers.iter().map(|b| b.len()).sum());
        Self::new_unchecked(
            data_type,
            views,
            buffers,
            validity,
            total_bytes_len,
            total_buffer_len,
        )
    }

    pub fn data_buffers(&self) -> &Arc<[Buffer<u8>]> {
        &self.buffers
    }

    pub fn variadic_buffer_lengths(&self) -> Vec<i64> {
        self.buffers.iter().map(|buf| buf.len() as i64).collect()
    }

    pub fn views(&self) -> &Buffer<View> {
        &self.views
    }

    pub fn try_new(
        data_type: DataType,
        views: Buffer<View>,
        buffers: Arc<[Buffer<u8>]>,
        validity: Option<Bitmap>,
    ) -> Result<Self> {
        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(Error::oos(
                "BinaryViewArray can only be initialized with DataType::BinaryView or DataType::Utf8View",
            ));
        }
        if T::IS_UTF8 {
            validate_utf8_view(views.as_ref(), buffers.as_ref())?;
        } else {
            validate_binary_view(views.as_ref(), buffers.as_ref())?;
        }

        if let Some(validity) = &validity {
            if validity.len() != views.len() {
                return Err(Error::oos(
                    "validity mask length must match the number of values",
                ));
            }
        }

        unsafe {
            Ok(Self::new_unchecked_unknown_md(
                data_type, views, buffers, validity, None,
            ))
        }
    }

    /// Returns a new [`BinaryViewArrayGeneric`] from a slice of `&T`.
    // Note: this can't be `impl From` because Rust does not allow double `AsRef` on it.
    pub fn from<V: AsRef<T>, P: AsRef<[Option<V>]>>(slice: P) -> Self {
        MutableBinaryViewArray::<T>::from(slice).into()
    }

    /// Creates an empty [`BinaryViewArrayGeneric`], i.e. whose `.len` is zero.
    #[inline]
    pub fn new_empty(data_type: DataType) -> Self {
        Self::new_unchecked(data_type, Buffer::new(), Arc::from([]), None, 0, 0)
    }

    /// Returns a new null [`BinaryViewArrayGeneric`] of `length`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let validity = Some(Bitmap::new_zeroed(length));
        Self::new_unchecked(
            data_type,
            Buffer::zeroed(length),
            Arc::from([]),
            validity,
            0,
            0,
        )
    }

    /// Returns the element at index `i`
    /// # Panics
    /// iff `i >= self.len()`
    #[inline]
    pub fn value(&self, i: usize) -> &T {
        assert!(i < self.len());
        unsafe { self.value_unchecked(i) }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &T {
        let v = *self.views.get_unchecked(i);
        let len = v.length;

        // view layout:
        // length: 4 bytes
        // prefix: 4 bytes
        // buffer_index: 4 bytes
        // offset: 4 bytes

        // inlined layout:
        // length: 4 bytes
        // data: 12 bytes

        let bytes = if len <= 12 {
            let ptr = self.views.data_ptr() as *const u8;
            std::slice::from_raw_parts(ptr.add(i * 16 + 4), len as usize)
        } else {
            let (data_ptr, data_len) = *self.raw_buffers.get_unchecked(v.buffer_idx as usize);
            let data = std::slice::from_raw_parts(data_ptr, data_len);
            let offset = v.offset as usize;
            data.get_unchecked(offset..offset + len as usize)
        };
        T::from_bytes_unchecked(bytes)
    }

    /// Returns an iterator of `Option<&T>` over every element of this array.
    pub fn iter(&self) -> ZipValidity<&T, BinaryViewValueIter<T>, BitmapIter> {
        ZipValidity::new_with_validity(self.values_iter(), self.validity.as_ref())
    }

    /// Returns an iterator of `&[u8]` over every element of this array, ignoring the validity
    pub fn values_iter(&self) -> BinaryViewValueIter<T> {
        BinaryViewValueIter::new(self)
    }

    pub fn len_iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.views.iter().map(|v| v.length)
    }

    /// Returns an iterator of the non-null values.
    pub fn non_null_values_iter(&self) -> NonNullValuesIter<'_, BinaryViewArrayGeneric<T>> {
        NonNullValuesIter::new(self, self.validity())
    }

    /// Returns an iterator of the non-null values.
    pub fn non_null_views_iter(&self) -> NonNullValuesIter<'_, Buffer<View>> {
        NonNullValuesIter::new(self.views(), self.validity())
    }

    impl_sliced!();
    impl_mut_validity!();
    impl_into_array!();

    pub fn from_slice<S: AsRef<T>, P: AsRef<[Option<S>]>>(slice: P) -> Self {
        let mutable = MutableBinaryViewArray::from_iterator(
            slice.as_ref().iter().map(|opt_v| opt_v.as_ref()),
        );
        mutable.into()
    }

    pub fn from_slice_values<S: AsRef<T>, P: AsRef<[S]>>(slice: P) -> Self {
        let mutable =
            MutableBinaryViewArray::from_values_iter(slice.as_ref().iter().map(|v| v.as_ref()));
        mutable.into()
    }

    /// Get the total length of bytes that it would take to concatenate all binary/str values in this array.
    pub fn total_bytes_len(&self) -> usize {
        let total = self.total_bytes_len.load(Ordering::Relaxed);
        if total == UNKNOWN_LEN {
            let total = self.len_iter().map(|v| v as usize).sum::<usize>();
            self.total_bytes_len.store(total as u64, Ordering::Relaxed);
            total
        } else {
            total as usize
        }
    }

    /// Get the length of bytes that are stored in the variadic buffers.
    pub fn total_buffer_len(&self) -> usize {
        self.total_buffer_len
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.views.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Garbage collect
    pub fn gc(self) -> Self {
        if self.buffers.is_empty() {
            return self;
        }
        let mut mutable = MutableBinaryViewArray::with_capacity(self.len());
        let buffers = self.raw_buffers.as_ref();

        for view in self.views.as_ref() {
            unsafe { mutable.push_view(*view, buffers) }
        }
        mutable.freeze().with_validity(self.validity)
    }

    pub fn is_sliced(&self) -> bool {
        self.views.as_ptr() != self.views.data_ptr()
    }

    pub fn maybe_gc(self) -> Self {
        const GC_MINIMUM_SAVINGS: usize = 16 * 1024; // At least 16 KiB.

        if self.total_buffer_len <= GC_MINIMUM_SAVINGS {
            return self;
        }

        // Subtract the maximum amount of inlined strings to get a lower bound
        // on the number of buffer bytes needed (assuming no dedup).
        let total_bytes_len = self.total_bytes_len();
        let buffer_req_lower_bound = total_bytes_len.saturating_sub(self.len() * 12);

        let lower_bound_mem_usage_post_gc = self.len() * 16 + buffer_req_lower_bound;
        let current_mem_usage = self.len() * 16 + self.total_buffer_len();
        let savings_upper_bound = current_mem_usage.saturating_sub(lower_bound_mem_usage_post_gc);

        if savings_upper_bound >= GC_MINIMUM_SAVINGS
            && current_mem_usage >= 4 * lower_bound_mem_usage_post_gc
        {
            self.gc()
        } else {
            self
        }
    }

    pub fn make_mut(self) -> MutableBinaryViewArray<T> {
        let views = self.views.make_mut();
        let completed_buffers = self.buffers.to_vec();
        let validity = self.validity.map(|bitmap| bitmap.make_mut());
        MutableBinaryViewArray {
            views,
            completed_buffers,
            in_progress_buffer: vec![],
            validity,
            phantom: Default::default(),
            total_bytes_len: self.total_bytes_len.load(Ordering::Relaxed) as usize,
            total_buffer_len: self.total_buffer_len,
        }
    }

    #[must_use]
    pub fn into_mut(self) -> Either<Self, MutableBinaryViewArray<T>> {
        use Either::*;
        let is_unique = (Arc::strong_count(&self.buffers) + Arc::weak_count(&self.buffers)) == 1;

        if let Some(bitmap) = self.validity {
            match bitmap.into_mut() {
                Left(bitmap) => Left(Self::new_unchecked(
                    self.data_type,
                    self.views,
                    self.buffers,
                    Some(bitmap),
                    self.total_bytes_len.load(Ordering::Relaxed) as usize,
                    self.total_buffer_len,
                )),
                Right(mutable_bitmap) => match (self.views.into_mut(), is_unique) {
                    (Right(views), true) => Right(MutableBinaryViewArray {
                        views,
                        completed_buffers: self.buffers.to_vec(),
                        in_progress_buffer: vec![],
                        validity: Some(mutable_bitmap),
                        phantom: Default::default(),
                        total_bytes_len: self.total_bytes_len.load(Ordering::Relaxed) as usize,
                        total_buffer_len: self.total_buffer_len,
                    }),
                    (Right(views), false) => Left(Self::new_unchecked(
                        self.data_type,
                        views.into(),
                        self.buffers,
                        Some(mutable_bitmap.into()),
                        self.total_bytes_len.load(Ordering::Relaxed) as usize,
                        self.total_buffer_len,
                    )),
                    (Left(views), _) => Left(Self::new_unchecked(
                        self.data_type,
                        views,
                        self.buffers,
                        Some(mutable_bitmap.into()),
                        self.total_bytes_len.load(Ordering::Relaxed) as usize,
                        self.total_buffer_len,
                    )),
                },
            }
        } else {
            match (self.views.into_mut(), is_unique) {
                (Right(views), true) => Right(MutableBinaryViewArray {
                    views,
                    completed_buffers: self.buffers.to_vec(),
                    in_progress_buffer: vec![],
                    validity: None,
                    phantom: Default::default(),
                    total_bytes_len: self.total_bytes_len.load(Ordering::Relaxed) as usize,
                    total_buffer_len: self.total_buffer_len,
                }),
                (Right(views), false) => Left(Self::new_unchecked(
                    self.data_type,
                    views.into(),
                    self.buffers,
                    None,
                    self.total_bytes_len.load(Ordering::Relaxed) as usize,
                    self.total_buffer_len,
                )),
                (Left(views), _) => Left(Self::new_unchecked(
                    self.data_type,
                    views,
                    self.buffers,
                    None,
                    self.total_bytes_len.load(Ordering::Relaxed) as usize,
                    self.total_buffer_len,
                )),
            }
        }
    }

    pub fn default_data_type() -> &'static DataType {
        T::data_type()
    }
}

pub type BinaryViewArray = BinaryViewArrayGeneric<[u8]>;
pub type Utf8ViewArray = BinaryViewArrayGeneric<str>;

impl BinaryViewArray {
    /// Validate the underlying bytes on UTF-8.
    pub fn validate_utf8(&self) -> Result<()> {
        // SAFETY: views are correct
        unsafe { validate_utf8_only(&self.views, &self.buffers) }
    }

    /// Convert [`BinaryViewArray`] to [`Utf8ViewArray`].
    pub fn to_utf8view(&self) -> Result<Utf8ViewArray> {
        self.validate_utf8()?;
        unsafe { Ok(self.to_utf8view_unchecked()) }
    }

    /// Convert [`BinaryViewArray`] to [`Utf8ViewArray`] without checking UTF-8.
    ///
    /// # Safety
    /// The caller must ensure the underlying data is valid UTF-8.
    pub unsafe fn to_utf8view_unchecked(&self) -> Utf8ViewArray {
        Utf8ViewArray::new_unchecked(
            DataType::Utf8View,
            self.views.clone(),
            self.buffers.clone(),
            self.validity.clone(),
            self.total_bytes_len.load(Ordering::Relaxed) as usize,
            self.total_buffer_len,
        )
    }
}

impl Utf8ViewArray {
    pub fn to_binview(&self) -> BinaryViewArray {
        BinaryViewArray::new_unchecked(
            DataType::BinaryView,
            self.views.clone(),
            self.buffers.clone(),
            self.validity.clone(),
            self.total_bytes_len.load(Ordering::Relaxed) as usize,
            self.total_buffer_len,
        )
    }
}

impl<T: ViewType + ?Sized> Array for BinaryViewArrayGeneric<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[inline(always)]
    fn len(&self) -> usize {
        BinaryViewArrayGeneric::len(self)
    }

    fn data_type(&self) -> &DataType {
        T::data_type()
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        debug_assert!(offset + length <= self.len());
        self.validity = self
            .validity
            .take()
            .map(|bitmap| bitmap.sliced_unchecked(offset, length))
            .filter(|bitmap| bitmap.unset_bits() > 0);
        self.views.slice_unchecked(offset, length);
        self.total_bytes_len.store(UNKNOWN_LEN, Ordering::Relaxed)
    }

    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        let mut new = self.clone();
        new.validity = validity;
        Box::new(new)
    }

    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(self.clone())
    }
}
