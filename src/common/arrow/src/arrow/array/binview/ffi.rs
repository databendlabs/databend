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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::arrow::array::binview::BinaryViewArrayGeneric;
use crate::arrow::array::binview::View;
use crate::arrow::array::binview::ViewType;
use crate::arrow::array::FromFfi;
use crate::arrow::array::ToFfi;
use crate::arrow::bitmap::align;
use crate::arrow::error::Result;
use crate::arrow::ffi;

unsafe impl<T: ViewType + ?Sized> ToFfi for BinaryViewArrayGeneric<T> {
    fn buffers(&self) -> Vec<Option<*const u8>> {
        let mut buffers = Vec::with_capacity(self.buffers.len() + 2);
        buffers.push(self.validity.as_ref().map(|x| x.as_ptr()));
        buffers.push(Some(self.views.data_ptr().cast::<u8>()));
        buffers.extend(self.buffers.iter().map(|b| Some(b.data_ptr())));
        buffers
    }

    fn offset(&self) -> Option<usize> {
        let offset = self.views.offset();
        if let Some(bitmap) = self.validity.as_ref() {
            if bitmap.offset() == offset {
                Some(offset)
            } else {
                None
            }
        } else {
            Some(offset)
        }
    }

    fn to_ffi_aligned(&self) -> Self {
        let offset = self.views.offset();

        let validity = self.validity.as_ref().map(|bitmap| {
            if bitmap.offset() == offset {
                bitmap.clone()
            } else {
                align(bitmap, offset)
            }
        });

        Self {
            data_type: self.data_type.clone(),
            validity,
            views: self.views.clone(),
            buffers: self.buffers.clone(),
            raw_buffers: self.raw_buffers.clone(),
            phantom: Default::default(),
            total_bytes_len: AtomicU64::new(self.total_bytes_len.load(Ordering::Relaxed)),
            total_buffer_len: self.total_buffer_len,
        }
    }
}

impl<T: ViewType + ?Sized, A: ffi::ArrowArrayRef> FromFfi<A> for BinaryViewArrayGeneric<T> {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.data_type().clone();

        let validity = unsafe { array.validity() }?;
        let views = unsafe { array.buffer::<View>(1) }?;

        // n_buffers - 2, 2 means validity + views
        let n_buffers = array.n_buffers();
        let mut remaining_buffers = n_buffers - 2;
        if remaining_buffers <= 1 {
            return Ok(Self::new_unchecked_unknown_md(
                data_type,
                views,
                Arc::from([]),
                validity,
                None,
            ));
        }

        let n_variadic_buffers = remaining_buffers - 1;
        let variadic_buffer_offset = n_buffers - 1;

        let variadic_buffer_sizes =
            array.buffer_known_len::<i64>(variadic_buffer_offset, n_variadic_buffers)?;
        remaining_buffers -= 1;

        let mut variadic_buffers = Vec::with_capacity(remaining_buffers);

        let offset = 2;
        for (i, &size) in (offset..remaining_buffers + offset).zip(variadic_buffer_sizes.iter()) {
            let values = unsafe { array.buffer_known_len::<u8>(i, size as usize) }?;
            variadic_buffers.push(values);
        }

        Ok(Self::new_unchecked_unknown_md(
            data_type,
            views,
            Arc::from(variadic_buffers),
            validity,
            None,
        ))
    }
}
