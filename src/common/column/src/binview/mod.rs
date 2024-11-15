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

mod builder;
pub(crate) mod fmt;
mod iterator;
mod view;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType;
pub use builder::BinaryViewColumnBuilder;
use either::Either;
pub use iterator::BinaryViewColumnIter;
use private::Sealed;
use view::validate_utf8_only;
pub use view::CheckUTF8;
pub use view::View;

use crate::binary::BinaryColumn;
use crate::binary::BinaryColumnBuilder;
use crate::buffer::Buffer;
use crate::error::Result;
use crate::impl_sliced;

mod private {
    pub trait Sealed: Send + Sync {}

    impl Sealed for str {}

    impl Sealed for [u8] {}
}

const UNKNOWN_LEN: u64 = u64::MAX;

pub trait ViewType: Sealed + 'static + PartialEq + AsRef<Self> {
    const IS_UTF8: bool;
    type Owned: Debug + Clone + Sync + Send + AsRef<Self>;

    /// # Safety
    /// The caller must ensure `index < self.len()`.
    unsafe fn from_bytes_unchecked(slice: &[u8]) -> &Self;

    fn to_bytes(&self) -> &[u8];

    #[allow(clippy::wrong_self_convention)]
    fn into_owned(&self) -> Self::Owned;

    fn name() -> &'static str {
        if Self::IS_UTF8 {
            "StringView"
        } else {
            "BinaryView"
        }
    }
}

impl ViewType for str {
    const IS_UTF8: bool = true;
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
}

impl ViewType for [u8] {
    const IS_UTF8: bool = false;
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
}

pub struct BinaryViewColumnGeneric<T: ViewType + ?Sized> {
    views: Buffer<View>,
    buffers: Arc<[Buffer<u8>]>,
    phantom: PhantomData<T>,
    /// Total bytes length if we would concat them all
    total_bytes_len: AtomicU64,
    /// Total bytes in the buffer (exclude remaining capacity)
    total_buffer_len: usize,
}

impl<T: ViewType + ?Sized> Clone for BinaryViewColumnGeneric<T> {
    fn clone(&self) -> Self {
        Self {
            views: self.views.clone(),
            buffers: self.buffers.clone(),

            phantom: Default::default(),
            total_bytes_len: AtomicU64::new(self.total_bytes_len.load(Ordering::Relaxed)),
            total_buffer_len: self.total_buffer_len,
        }
    }
}

unsafe impl<T: ViewType + ?Sized> Send for BinaryViewColumnGeneric<T> {}

unsafe impl<T: ViewType + ?Sized> Sync for BinaryViewColumnGeneric<T> {}

impl<T: ViewType + ?Sized> BinaryViewColumnGeneric<T> {
    pub fn new_unchecked(
        views: Buffer<View>,
        buffers: Arc<[Buffer<u8>]>,

        total_bytes_len: usize,
        total_buffer_len: usize,
    ) -> Self {
        #[cfg(debug_assertions)]
        {
            if total_bytes_len != UNKNOWN_LEN as usize {
                let total = views.iter().map(|v| v.length as usize).sum::<usize>();
                assert_eq!(total, total_bytes_len);
            }

            if total_buffer_len != UNKNOWN_LEN as usize {
                let total = buffers.iter().map(|v| v.len()).sum::<usize>();
                assert_eq!(total, total_buffer_len);
            }
        }
        // # Safety
        // The caller must ensure
        // - the data is valid utf8 (if required)
        // - the offsets match the buffers.
        Self {
            views,
            buffers,

            phantom: Default::default(),
            total_bytes_len: AtomicU64::new(total_bytes_len as u64),
            total_buffer_len,
        }
    }

    /// Create a new BinaryViewColumn but initialize a statistics compute.
    /// # Safety
    /// The caller must ensure the invariants
    pub unsafe fn new_unchecked_unknown_md(
        views: Buffer<View>,
        buffers: Arc<[Buffer<u8>]>,

        total_buffer_len: Option<usize>,
    ) -> Self {
        let total_bytes_len = UNKNOWN_LEN as usize;
        let total_buffer_len =
            total_buffer_len.unwrap_or_else(|| buffers.iter().map(|b| b.len()).sum());
        Self::new_unchecked(views, buffers, total_bytes_len, total_buffer_len)
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

    pub fn try_new(views: Buffer<View>, buffers: Arc<[Buffer<u8>]>) -> Result<Self> {
        #[cfg(debug_assertions)]
        {
            if T::IS_UTF8 {
                crate::binview::view::validate_utf8_view(views.as_ref(), buffers.as_ref())?;
            } else {
                crate::binview::view::validate_binary_view(views.as_ref(), buffers.as_ref())?;
            }
        }

        unsafe { Ok(Self::new_unchecked_unknown_md(views, buffers, None)) }
    }

    /// Returns a new [`BinaryViewColumnGeneric`] from a slice of `&T`.
    // Note: this can't be `impl From` because Rust does not allow double `AsRef` on it.
    pub fn from<V: AsRef<T>, P: AsRef<[V]>>(slice: P) -> Self {
        BinaryViewColumnBuilder::<T>::from(slice).into()
    }

    /// Creates an empty [`BinaryViewColumnGeneric`], i.e. whose `.len` is zero.
    #[inline]
    pub fn new_empty() -> Self {
        Self::new_unchecked(Buffer::new(), Arc::from([]), 0, 0)
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
    #[inline]
    pub fn index(&self, i: usize) -> Option<&T> {
        if i < self.len() {
            Some(unsafe { self.value_unchecked(i) })
        } else {
            None
        }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &T {
        let v = self.views.get_unchecked(i);
        T::from_bytes_unchecked(v.get_slice_unchecked(&self.buffers))
    }

    /// same as value_unchecked
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn index_unchecked(&self, i: usize) -> &T {
        let v = self.views.get_unchecked(i);
        T::from_bytes_unchecked(v.get_slice_unchecked(&self.buffers))
    }

    /// same as value_unchecked, yet it will return bytes
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn index_unchecked_bytes(&self, i: usize) -> &[u8] {
        let v = self.views.get_unchecked(i);
        v.get_slice_unchecked(&self.buffers)
    }

    /// Returns an iterator of `&[u8]` over every element of this array, ignoring the validity
    pub fn iter(&self) -> BinaryViewColumnIter<T> {
        BinaryViewColumnIter::new(self)
    }

    pub fn len_iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.views.iter().map(|v| v.length)
    }

    pub fn from_slice<S: AsRef<T>, P: AsRef<[S]>>(slice: P) -> Self {
        let mutable = BinaryViewColumnBuilder::from_iterator(
            slice.as_ref().iter().map(|opt_v| opt_v.as_ref()),
        );
        mutable.into()
    }

    pub fn from_slice_values<S: AsRef<T>, P: AsRef<[S]>>(slice: P) -> Self {
        let mutable =
            BinaryViewColumnBuilder::from_values_iter(slice.as_ref().iter().map(|v| v.as_ref()));
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

    pub fn memory_size(&self) -> usize {
        self.total_buffer_len() + self.len() * 12
    }

    fn total_unshared_buffer_len(&self) -> usize {
        // Given this function is only called in `maybe_gc()`,
        // it may not be worthy to add an extra field for this.
        self.buffers
            .iter()
            .map(|buf| {
                if buf.shared_count_strong() > 1 {
                    0
                } else {
                    buf.len()
                }
            })
            .sum()
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
        let mut mutable = BinaryViewColumnBuilder::with_capacity(self.len());
        let buffers = self.buffers.as_ref();

        for view in self.views.as_ref() {
            unsafe { mutable.push_view_unchecked(*view, buffers) }
        }
        mutable.freeze()
    }

    pub fn is_sliced(&self) -> bool {
        self.views.as_ptr() != self.views.data_ptr()
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
        self.views.slice_unchecked(offset, length);
        self.total_bytes_len.store(UNKNOWN_LEN, Ordering::Relaxed)
    }

    impl_sliced!();

    pub fn maybe_gc(self) -> Self {
        const GC_MINIMUM_SAVINGS: usize = 16 * 1024; // At least 16 KiB.

        if self.total_buffer_len <= GC_MINIMUM_SAVINGS {
            return self;
        }

        // if Arc::strong_count(&self.buffers) != 1 {
        //     // There are multiple holders of this `buffers`.
        //     // If we allow gc in this case,
        //     // it may end up copying the same content multiple times.
        //     return self;
        // }

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

    pub fn make_mut(self) -> BinaryViewColumnBuilder<T> {
        let views = self.views.make_mut();
        let completed_buffers = self.buffers.to_vec();
        BinaryViewColumnBuilder {
            views,
            completed_buffers,
            in_progress_buffer: vec![],

            phantom: Default::default(),
            total_bytes_len: self.total_bytes_len.load(Ordering::Relaxed) as usize,
            total_buffer_len: self.total_buffer_len,
        }
    }

    #[must_use]
    pub fn into_mut(self) -> Either<Self, BinaryViewColumnBuilder<T>> {
        use Either::*;
        let is_unique = (Arc::strong_count(&self.buffers) + Arc::weak_count(&self.buffers)) == 1;

        match (self.views.into_mut(), is_unique) {
            (Right(views), true) => Right(BinaryViewColumnBuilder {
                views,
                completed_buffers: self.buffers.to_vec(),
                in_progress_buffer: vec![],
                phantom: Default::default(),
                total_bytes_len: self.total_bytes_len.load(Ordering::Relaxed) as usize,
                total_buffer_len: self.total_buffer_len,
            }),
            (Right(views), false) => Left(Self::new_unchecked(
                views.into(),
                self.buffers,
                self.total_bytes_len.load(Ordering::Relaxed) as usize,
                self.total_buffer_len,
            )),
            (Left(views), _) => Left(Self::new_unchecked(
                views,
                self.buffers,
                self.total_bytes_len.load(Ordering::Relaxed) as usize,
                self.total_buffer_len,
            )),
        }
    }
}

pub type BinaryViewColumn = BinaryViewColumnGeneric<[u8]>;
pub type Utf8ViewColumn = BinaryViewColumnGeneric<str>;
pub type StringColumn = BinaryViewColumnGeneric<str>;

pub type Utf8ViewColumnBuilder = BinaryViewColumnBuilder<str>;
pub type StringColumnBuilder = BinaryViewColumnBuilder<str>;

impl BinaryViewColumn {
    /// Validate the underlying bytes on UTF-8.
    pub fn validate_utf8(&self) -> Result<()> {
        // SAFETY: views are correct
        unsafe { validate_utf8_only(&self.views, &self.buffers) }
    }

    /// Convert [`BinaryViewColumn`] to [`Utf8ViewColumn`].
    pub fn to_utf8view(&self) -> Result<Utf8ViewColumn> {
        self.validate_utf8()?;
        unsafe { Ok(self.to_utf8view_unchecked()) }
    }

    /// Convert [`BinaryViewColumn`] to [`Utf8ViewColumn`] without checking UTF-8.
    ///
    /// # Safety
    /// The caller must ensure the underlying data is valid UTF-8.
    pub unsafe fn to_utf8view_unchecked(&self) -> Utf8ViewColumn {
        Utf8ViewColumn::new_unchecked(
            self.views.clone(),
            self.buffers.clone(),
            self.total_bytes_len.load(Ordering::Relaxed) as usize,
            self.total_buffer_len,
        )
    }
}

impl Utf8ViewColumn {
    pub fn to_binview(&self) -> BinaryViewColumn {
        BinaryViewColumn::new_unchecked(
            self.views.clone(),
            self.buffers.clone(),
            self.total_bytes_len.load(Ordering::Relaxed) as usize,
            self.total_buffer_len,
        )
    }

    pub fn compare(col_i: &Self, i: usize, col_j: &Self, j: usize) -> std::cmp::Ordering {
        let view_i = unsafe { col_i.views().as_slice().get_unchecked(i) };
        let view_j = unsafe { col_j.views().as_slice().get_unchecked(j) };

        if view_i.prefix == view_j.prefix {
            unsafe {
                let value_i = col_i.value_unchecked(i);
                let value_j = col_j.value_unchecked(j);
                value_i.cmp(value_j)
            }
        } else {
            view_i
                .prefix
                .to_le_bytes()
                .cmp(&view_j.prefix.to_le_bytes())
        }
    }

    pub fn compare_str(col: &Self, i: usize, value: &str) -> std::cmp::Ordering {
        let view = unsafe { col.views().as_slice().get_unchecked(i) };
        let prefix = load_prefix(value.as_bytes());

        if view.prefix == prefix {
            let value_i = unsafe { col.value_unchecked(i) };
            value_i.cmp(value)
        } else {
            view.prefix.to_le_bytes().as_slice().cmp(value.as_bytes())
        }
    }
}

impl PartialEq for Utf8ViewColumn {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for Utf8ViewColumn {}

impl PartialOrd for Utf8ViewColumn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Utf8ViewColumn {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for i in 0..self.len().max(other.len()) {
            match Self::compare(self, i, other, i) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            }
        }

        std::cmp::Ordering::Equal
    }
}

impl TryFrom<BinaryColumn> for Utf8ViewColumn {
    type Error = crate::error::Error;

    fn try_from(col: BinaryColumn) -> Result<Utf8ViewColumn> {
        let builder = Utf8ViewColumnBuilder::try_from_bin_column(col)?;
        Ok(builder.into())
    }
}

impl From<StringColumn> for BinaryColumn {
    fn from(col: Utf8ViewColumn) -> BinaryColumn {
        BinaryColumnBuilder::from_iter(col.iter()).into()
    }
}

impl From<Utf8ViewColumn> for ArrayData {
    fn from(column: Utf8ViewColumn) -> Self {
        let builder = ArrayDataBuilder::new(DataType::Utf8View)
            .len(column.len())
            .add_buffer(column.views.into())
            .add_buffers(
                column
                    .buffers
                    .into_iter()
                    .map(|x| x.clone().into())
                    .collect::<Vec<_>>(),
            );
        unsafe { builder.build_unchecked() }
    }
}

impl From<BinaryViewColumn> for ArrayData {
    fn from(column: BinaryViewColumn) -> Self {
        let builder = ArrayDataBuilder::new(DataType::BinaryView)
            .len(column.len())
            .add_buffer(column.views.into())
            .add_buffers(
                column
                    .buffers
                    .into_iter()
                    .map(|x| x.clone().into())
                    .collect::<Vec<_>>(),
            );
        unsafe { builder.build_unchecked() }
    }
}

impl From<ArrayData> for Utf8ViewColumn {
    fn from(data: ArrayData) -> Self {
        let views = data.buffers()[0].clone();
        let buffers = data.buffers()[1..]
            .iter()
            .map(|x| x.clone().into())
            .collect();

        unsafe { Utf8ViewColumn::new_unchecked_unknown_md(views.into(), buffers, None) }
    }
}

// Loads (up to) the first 4 bytes of s as little-endian, padded with zeros.
#[inline]
fn load_prefix(s: &[u8]) -> u32 {
    let start = &s[..s.len().min(4)];
    let mut tmp = [0u8; 4];
    tmp[..start.len()].copy_from_slice(start);
    u32::from_le_bytes(tmp)
}
