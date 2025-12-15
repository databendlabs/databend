// Copyright 2020-2022 Jorge C. Leitão
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

use std::iter::FromIterator;
use std::ops::Deref;
use std::sync::Arc;

use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType;
use either::Either;
use num_traits::Zero;

use super::Bytes;
use crate::bitmap::Bitmap;
use crate::bitmap::utils::BitmapIter;
use crate::bitmap::utils::ZipValidity;
use crate::types::NativeType;

/// [`Buffer`] is a contiguous memory region that can be shared across
/// thread boundaries.
///
/// The easiest way to think about [`Buffer<T>`] is being equivalent to
/// a `Arc<Vec<T>>`, with the following differences:
/// * slicing and cloning is `O(1)`.
/// * it supports external allocated memory
///
/// The easiest way to create one is to use its implementation of `From<Vec<T>>`.
///
/// # Examples
/// ```
/// use crate::buffer::Buffer;
///
/// let mut buffer: Buffer<u32> = vec![1, 2, 3].into();
/// assert_eq!(buffer.as_ref(), [1, 2, 3].as_ref());
///
/// // it supports copy-on-write semantics (i.e. back to a `Vec`)
/// let vec: Vec<u32> = buffer.into_mut().right().unwrap();
/// assert_eq!(vec, vec![1, 2, 3]);
///
/// // cloning and slicing is `O(1)` (data is shared)
/// let mut buffer: Buffer<u32> = vec![1, 2, 3].into();
/// let mut sliced = buffer.clone();
/// sliced.slice(1, 1);
/// assert_eq!(sliced.as_ref(), [2].as_ref());
/// // but cloning forbids getting mut since `slice` and `buffer` now share data
/// assert_eq!(buffer.get_mut_slice(), None);
/// ```
#[derive(Clone)]
pub struct Buffer<T> {
    /// the internal byte buffer.
    data: Arc<Bytes<T>>,

    /// Pointer into `data` valid
    ///
    /// We store a pointer instead of an offset to avoid pointer arithmetic
    /// which causes LLVM to fail to vectorise code correctly
    ptr: *const T,

    // the length of the buffer. Given a region `data` of N bytes, [offset..offset+length] is visible
    // to this buffer.
    length: usize,
}

unsafe impl<T: Send> Send for Buffer<T> {}
unsafe impl<T: Sync> Sync for Buffer<T> {}

impl<T: PartialEq> PartialEq for Buffer<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Buffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

impl<T> Default for Buffer<T> {
    #[inline]
    fn default() -> Self {
        Vec::new().into()
    }
}

impl<T> Buffer<T> {
    /// Creates an empty [`Buffer`].
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Auxiliary method to create a new Buffer
    pub(crate) fn from_bytes(bytes: Bytes<T>) -> Self {
        let length = bytes.len();
        let ptr = bytes.as_ptr();
        Buffer {
            data: Arc::new(bytes),
            ptr,
            length,
        }
    }

    /// Returns the number of bytes in the buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether underlying data is sliced.
    /// If sliced the [`Buffer`] is backed by
    /// more data than the length of `Self`.
    pub fn is_sliced(&self) -> bool {
        self.data.len() != self.length
    }

    /// Returns the byte slice stored in this buffer
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        self
    }

    /// Returns a new [`Buffer`] that is a slice of this buffer starting at `offset`.
    /// Doing so allows the same memory region to be shared between buffers.
    /// # Panics
    /// Panics iff `offset + length` is larger than `len`.
    #[inline]
    pub fn sliced(self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        // Safety: we just checked bounds
        unsafe { self.sliced_unchecked(offset, length) }
    }

    /// Slices this buffer starting at `offset`.
    /// # Panics
    /// Panics iff `offset + length` is larger than `len`.
    #[inline]
    pub fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        // Safety: we just checked bounds
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a new [`Buffer`] that is a slice of this buffer starting at `offset`.
    /// Doing so allows the same memory region to be shared between buffers.
    /// # Safety
    /// The caller must ensure `offset + length <= self.len()`
    #[inline]
    #[must_use]
    pub unsafe fn sliced_unchecked(mut self, offset: usize, length: usize) -> Self {
        self.slice_unchecked(offset, length);
        self
    }

    /// Slices this buffer starting at `offset`.
    /// # Safety
    /// The caller must ensure `offset + length <= self.len()`
    #[inline]
    pub unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        self.ptr = self.ptr.add(offset);
        self.length = length;
    }

    /// Returns a pointer to the start of this buffer.
    #[inline]
    pub(crate) fn data_ptr(&self) -> *const T {
        self.data.as_ptr()
    }

    /// Returns the offset of this buffer.
    #[inline]
    pub fn offset(&self) -> usize {
        unsafe { self.ptr.offset_from(self.data_ptr()) as usize }
    }

    /// # Safety
    /// The caller must ensure that the buffer was properly initialized up to `len`.
    #[inline]
    pub unsafe fn set_len(&mut self, len: usize) {
        self.length = len;
    }

    /// Returns a mutable reference to its underlying [`Vec`], if possible.
    ///
    /// This operation returns [`Either::Right`] iff this [`Buffer`]:
    /// * has not been cloned (i.e. [`Arc`]`::get_mut` yields [`Some`])
    /// * has not been imported from the c data interface (FFI)
    #[inline]
    pub fn into_mut(mut self) -> Either<Self, Vec<T>> {
        match Arc::get_mut(&mut self.data)
            .and_then(|b| b.get_vec())
            .map(std::mem::take)
        {
            Some(inner) => Either::Right(inner),
            None => Either::Left(self),
        }
    }

    /// Returns a mutable reference to its underlying `Vec`, if possible.
    /// Note that only `[self.offset(), self.offset() + self.len()[` in this vector is visible
    /// by this buffer.
    ///
    /// This operation returns [`Some`] iff this [`Buffer`]:
    /// * has not been cloned (i.e. [`Arc`]`::get_mut` yields [`Some`])
    /// * has not been imported from the c data interface (FFI)
    /// # Safety
    /// The caller must ensure that the vector in the mutable reference keeps a length of at least `self.offset() + self.len() - 1`.
    #[inline]
    pub unsafe fn get_mut(&mut self) -> Option<&mut Vec<T>> {
        Arc::get_mut(&mut self.data).and_then(|b| b.get_vec())
    }

    /// Returns a mutable reference to its slice, if possible.
    ///
    /// This operation returns [`Some`] iff this [`Buffer`]:
    /// * has not been cloned (i.e. [`Arc`]`::get_mut` yields [`Some`])
    /// * has not been imported from the c data interface (FFI)
    #[inline]
    pub fn get_mut_slice(&mut self) -> Option<&mut [T]> {
        let offset = self.offset();
        Arc::get_mut(&mut self.data)
            .and_then(|b| b.get_vec())
            // Safety: the invariant of this struct
            .map(|x| unsafe { x.get_unchecked_mut(offset..offset + self.length) })
    }

    /// Get the strong count of underlying `Arc` data buffer.
    pub fn shared_count_strong(&self) -> usize {
        Arc::strong_count(&self.data)
    }

    /// Get the weak count of underlying `Arc` data buffer.
    pub fn shared_count_weak(&self) -> usize {
        Arc::weak_count(&self.data)
    }

    /// Creates a `[Bitmap]` from its internal representation.
    /// This is the inverted from `[Bitmap::into_inner]`
    ///
    /// # Safety
    /// Callers must ensure all invariants of this struct are upheld.
    pub unsafe fn from_inner_unchecked(data: Arc<Bytes<T>>, offset: usize, length: usize) -> Self {
        let ptr = data.as_ptr().add(offset);
        Self { data, ptr, length }
    }
}

impl<T: Clone> Buffer<T> {
    pub fn make_mut(self) -> Vec<T> {
        match self.into_mut() {
            Either::Right(v) => v,
            Either::Left(same) => same.as_slice().to_vec(),
        }
    }
}

impl<T: Zero + Copy> Buffer<T> {
    pub fn zeroed(len: usize) -> Self {
        vec![T::zero(); len].into()
    }
}

impl<T> From<Vec<T>> for Buffer<T> {
    #[inline]
    fn from(p: Vec<T>) -> Self {
        let bytes: Bytes<T> = p.into();
        let ptr = bytes.as_ptr();
        Self {
            ptr,
            length: bytes.len(),
            data: Arc::new(bytes),
        }
    }
}

impl<T> std::ops::Deref for Buffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        debug_assert!(self.offset() + self.length <= self.data.len());
        unsafe { std::slice::from_raw_parts(self.ptr, self.length) }
    }
}

impl<T> AsRef<[T]> for Buffer<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self
    }
}

impl<T> FromIterator<T> for Buffer<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Vec::from_iter(iter).into()
    }
}

impl<'a, T> IntoIterator for &'a Buffer<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_ref().iter()
    }
}

impl<T: Copy> IntoIterator for Buffer<T> {
    type Item = T;

    type IntoIter = super::iterator::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        super::iterator::IntoIter::new(self)
    }
}

impl<T: Copy> Buffer<T> {
    pub fn option_iter<'a>(
        &self,
        validity: Option<&'a Bitmap>,
    ) -> ZipValidity<T, super::iterator::IntoIter<T>, BitmapIter<'a>> {
        let iter = IntoIterator::into_iter(self.clone());
        let bitmap_iter = validity.as_ref().map(|v| v.iter());
        ZipValidity::new(iter, bitmap_iter)
    }
}

impl<T: crate::types::NativeType> From<arrow_buffer::Buffer> for Buffer<T> {
    fn from(value: arrow_buffer::Buffer) -> Self {
        Self::from_bytes(crate::buffer::to_bytes(value))
    }
}

impl<T: crate::types::NativeType> From<Buffer<T>> for arrow_buffer::Buffer {
    fn from(value: Buffer<T>) -> Self {
        let offset = value.offset();
        crate::buffer::to_buffer(value.data).slice_with_length(
            offset * std::mem::size_of::<T>(),
            value.length * std::mem::size_of::<T>(),
        )
    }
}

pub fn buffer_to_array_data<T: NativeType>(value: (Buffer<T>, DataType)) -> ArrayData {
    let l = value.0.len();
    let buffer = value.0.into();
    let builder = ArrayDataBuilder::new(value.1).len(l).buffers(vec![buffer]);
    unsafe { builder.build_unchecked() }
}

impl<const N: usize> From<arrow_buffer::Buffer> for Buffer<[u64; N]> {
    fn from(value: arrow_buffer::Buffer) -> Self {
        Self::from_bytes(crate::buffer::to_bytes(value))
    }
}

impl<const N: usize> From<Buffer<[u64; N]>> for arrow_buffer::Buffer {
    fn from(value: Buffer<[u64; N]>) -> Self {
        let offset = value.offset();
        let size = std::mem::size_of::<u64>() * N;
        crate::buffer::to_buffer(value.data).slice_with_length(offset * size, value.length * size)
    }
}
