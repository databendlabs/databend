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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::iter::TrustedLen;
use std::sync::Arc;

use super::view::CheckUTF8;
use crate::binary::BinaryColumn;
use crate::binview::iterator::BinaryViewBuilderIter;
use crate::binview::view::validate_utf8_only;
use crate::binview::BinaryViewColumnGeneric;
use crate::binview::View;
use crate::binview::ViewType;
use crate::buffer::Buffer;
use crate::error::Result;
use crate::types::NativeType;

const DEFAULT_BLOCK_SIZE: usize = 8 * 1024;

pub struct BinaryViewColumnBuilder<T: ViewType + ?Sized> {
    pub(super) views: Vec<View>,
    pub(super) completed_buffers: Vec<Buffer<u8>>,
    pub(super) in_progress_buffer: Vec<u8>,
    pub(super) phantom: std::marker::PhantomData<T>,
    /// Total bytes length if we would concatenate them all.
    pub total_bytes_len: usize,
    /// Total bytes in the buffer (excluding remaining capacity)
    pub total_buffer_len: usize,
}

impl<T: ViewType + ?Sized> Clone for BinaryViewColumnBuilder<T> {
    fn clone(&self) -> Self {
        Self {
            views: self.views.clone(),
            completed_buffers: self.completed_buffers.clone(),
            in_progress_buffer: self.in_progress_buffer.clone(),
            phantom: Default::default(),
            total_bytes_len: self.total_bytes_len,
            total_buffer_len: self.total_buffer_len,
        }
    }
}

impl<T: ViewType + ?Sized> Debug for BinaryViewColumnBuilder<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "BinaryViewColumnBuilder{:?}", T::name())
    }
}

impl<T: ViewType + ?Sized> Default for BinaryViewColumnBuilder<T> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl<T: ViewType + ?Sized> From<BinaryViewColumnBuilder<T>> for BinaryViewColumnGeneric<T> {
    fn from(mut value: BinaryViewColumnBuilder<T>) -> Self {
        value.finish_in_progress();
        Self::new_unchecked(
            value.views.into(),
            Arc::from(value.completed_buffers),
            value.total_bytes_len,
            value.total_buffer_len,
        )
    }
}

impl<T: ViewType + ?Sized> BinaryViewColumnBuilder<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            views: Vec::with_capacity(capacity),
            completed_buffers: vec![],
            in_progress_buffer: vec![],
            phantom: Default::default(),
            total_buffer_len: 0,
            total_bytes_len: 0,
        }
    }

    #[inline]
    pub fn views_mut(&mut self) -> &mut Vec<View> {
        &mut self.views
    }

    #[inline]
    pub fn views(&self) -> &[View] {
        &self.views
    }

    /// Reserves `additional` elements and `additional_buffer` on the buffer.
    pub fn reserve(&mut self, additional: usize) {
        self.views.reserve(additional);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.views.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.views.capacity()
    }

    pub fn memory_size(&self) -> usize {
        self.views.len() * 16 + self.total_buffer_len
    }

    /// # Safety
    /// - caller must allocate enough capacity
    /// - caller must ensure the view and buffers match.
    #[inline]
    pub(crate) unsafe fn push_view_unchecked(&mut self, v: View, buffers: &[Buffer<u8>]) {
        let len = v.length;
        if len <= 12 {
            self.total_bytes_len += len as usize;
            self.views.push(v)
        } else {
            let data = buffers.get_unchecked(v.buffer_idx as usize);
            let offset = v.offset as usize;
            let bytes = data.get_unchecked(offset..offset + len as usize);
            let t = T::from_bytes_unchecked(bytes);
            self.push_value(t)
        }
    }

    /// # Safety
    /// - caller ensure that view is duplicated
    #[inline]
    pub(crate) unsafe fn push_duplicated_view_unchecked(&mut self, v: View) {
        let len = v.length;
        self.total_bytes_len += len as usize;
        self.views.push(v);
    }

    pub fn push_value<V: AsRef<T>>(&mut self, value: V) {
        let value = value.as_ref();
        let bytes = value.to_bytes();
        self.total_bytes_len += bytes.len();
        let len: u32 = bytes.len().try_into().unwrap();
        let mut payload = [0; 16];
        payload[0..4].copy_from_slice(&len.to_le_bytes());

        if len <= 12 {
            // |   len   |  prefix  |  remaining(zero-padded)  |
            //     ^          ^             ^
            // | 4 bytes | 4 bytes |      8 bytes              |
            payload[4..4 + bytes.len()].copy_from_slice(bytes);
        } else {
            // |   len   |  prefix  |  buffer |  offsets  |
            //     ^          ^          ^         ^
            // | 4 bytes | 4 bytes | 4 bytes |  4 bytes  |
            //
            // buffer index + offset -> real binary data
            self.total_buffer_len += bytes.len();
            let required_cap = self.in_progress_buffer.len() + bytes.len();

            let does_not_fit_in_buffer = self.in_progress_buffer.capacity() < required_cap;
            let offset_will_not_fit = self.in_progress_buffer.len() > u32::MAX as usize;

            if does_not_fit_in_buffer || offset_will_not_fit {
                let new_capacity = (self.in_progress_buffer.capacity() * 2)
                    .clamp(DEFAULT_BLOCK_SIZE, 16 * 1024 * 1024)
                    .max(bytes.len());
                let in_progress = Vec::with_capacity(new_capacity);
                let flushed = std::mem::replace(&mut self.in_progress_buffer, in_progress);
                if !flushed.is_empty() {
                    self.completed_buffers.push(flushed.into())
                }
            }
            let offset = self.in_progress_buffer.len() as u32;
            self.in_progress_buffer.extend_from_slice(bytes);

            // set prefix
            unsafe { payload[4..8].copy_from_slice(bytes.get_unchecked(0..4)) };
            let buffer_idx: u32 = self.completed_buffers.len().try_into().unwrap();
            payload[8..12].copy_from_slice(&buffer_idx.to_le_bytes());
            payload[12..16].copy_from_slice(&offset.to_le_bytes());
        }
        let value = View::from_le_bytes(payload);
        self.views.push(value);
    }

    pub fn extend_constant<V: AsRef<T>>(&mut self, additional: usize, value: V) {
        let old_bytes_len = self.total_bytes_len;

        self.push_value(value);
        let value = self.views.pop().unwrap();
        self.total_bytes_len = old_bytes_len + value.length as usize * additional;
        self.views.extend(std::iter::repeat_n(value, additional));
    }

    #[inline]
    pub fn extend_values<I, P>(&mut self, iterator: I)
    where
        I: Iterator<Item = P>,
        P: AsRef<T>,
    {
        self.reserve(iterator.size_hint().0);
        for v in iterator {
            self.push_value(v)
        }
    }

    #[inline]
    pub fn extend_trusted_len_values<I, P>(&mut self, iterator: I)
    where
        I: TrustedLen<Item = P>,
        P: AsRef<T>,
    {
        self.extend_values(iterator)
    }

    #[inline]
    pub fn extend<I, P>(&mut self, iterator: I)
    where
        I: Iterator<Item = P>,
        P: AsRef<T>,
    {
        self.reserve(iterator.size_hint().0);
        for p in iterator {
            self.push_value(p)
        }
    }

    #[inline]
    pub fn extend_trusted_len<I, P>(&mut self, iterator: I)
    where
        I: TrustedLen<Item = P>,
        P: AsRef<T>,
    {
        self.extend(iterator)
    }

    #[inline]
    pub fn from_iterator<I, P>(iterator: I) -> Self
    where
        I: Iterator<Item = P>,
        P: AsRef<T>,
    {
        let mut builder = Self::with_capacity(iterator.size_hint().0);
        builder.extend(iterator);
        builder
    }

    pub fn from_values_iter<I, P>(iterator: I) -> Self
    where
        I: Iterator<Item = P>,
        P: AsRef<T>,
    {
        let mut builder = Self::with_capacity(iterator.size_hint().0);
        builder.extend_values(iterator);
        builder
    }

    pub fn from<S: AsRef<T>, P: AsRef<[S]>>(slice: P) -> Self {
        Self::from_iterator(slice.as_ref().iter().map(|opt_v| opt_v.as_ref()))
    }

    fn finish_in_progress(&mut self) {
        if !self.in_progress_buffer.is_empty() {
            self.completed_buffers
                .push(std::mem::take(&mut self.in_progress_buffer).into());
        }
    }

    #[inline]
    pub fn freeze(self) -> BinaryViewColumnGeneric<T> {
        self.into()
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &T {
        let v = *self.views.get_unchecked(i);
        let len = v.length;

        // view layout:
        // for no-inlined layout:
        // length: 4 bytes
        // prefix: 4 bytes
        // buffer_index: 4 bytes
        // offset: 4 bytes

        // for inlined layout:
        // length: 4 bytes
        // data: 12 bytes
        let bytes = if len <= 12 {
            let ptr = self.views.as_ptr() as *const u8;
            std::slice::from_raw_parts(ptr.add(i * 16 + 4), len as usize)
        } else {
            let buffer_idx = v.buffer_idx as usize;
            let offset = v.offset;

            let data = if buffer_idx == self.completed_buffers.len() {
                self.in_progress_buffer.as_slice()
            } else {
                self.completed_buffers.get_unchecked(buffer_idx)
            };

            let offset = offset as usize;
            data.get_unchecked(offset..offset + len as usize)
        };
        T::from_bytes_unchecked(bytes)
    }

    /// Returns an iterator of `&[u8]` over every element of this array
    pub fn iter(&self) -> BinaryViewBuilderIter<T> {
        BinaryViewBuilderIter::new(self)
    }

    pub fn values(&self) -> Vec<&T> {
        self.iter().collect()
    }
}

impl BinaryViewColumnBuilder<[u8]> {
    pub fn validate_utf8(&mut self) -> Result<()> {
        self.finish_in_progress();
        // views are correct
        unsafe { validate_utf8_only(&self.views, &self.completed_buffers) }
    }
}

impl BinaryViewColumnBuilder<str> {
    pub fn try_from_bin_column(col: BinaryColumn) -> Result<Self> {
        let mut data = Self::with_capacity(col.len());
        col.data.as_slice().check_utf8()?;

        for v in col.iter() {
            data.push_value(unsafe { std::str::from_utf8_unchecked(v) });
        }

        Ok(data)
    }

    pub fn pop(&mut self) -> Option<String> {
        if self.is_empty() {
            return None;
        }

        let value = unsafe { self.value_unchecked(self.len() - 1).to_string() };
        self.total_bytes_len -= value.len();
        self.views.pop();
        Some(value)
    }
}

impl<T: ViewType + ?Sized, P: AsRef<T>> Extend<P> for BinaryViewColumnBuilder<T> {
    #[inline]
    fn extend<I: IntoIterator<Item = P>>(&mut self, iter: I) {
        Self::extend(self, iter.into_iter())
    }
}

impl<T: ViewType + ?Sized, P: AsRef<T>> FromIterator<P> for BinaryViewColumnBuilder<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = P>>(iter: I) -> Self {
        Self::from_iterator(iter.into_iter())
    }
}
