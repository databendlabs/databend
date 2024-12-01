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

use std::cell::UnsafeCell;
use std::sync::Arc;

use bytes::Bytes;

// This is very unsafe, but it can significantly improve memory utilization. we assume:
// - The same parquet is always deserialized in one thread
// - No other column deserialization will start before the current column's deserialization is finished.
// Note: cannot be accessed between multiple threads at the same time.
pub struct UncompressedBuffer {
    buffer: UnsafeCell<Vec<u8>>,
}

unsafe impl Send for UncompressedBuffer {}

unsafe impl Sync for UncompressedBuffer {}

impl UncompressedBuffer {
    pub fn new(capacity: usize) -> Arc<UncompressedBuffer> {
        Arc::new(UncompressedBuffer {
            buffer: UnsafeCell::new(Vec::with_capacity(capacity)),
        })
    }

    #[allow(clippy::mut_from_ref)]
    pub(in crate::io::read::block::parquet) fn buffer_mut(&self, size: usize) -> &mut Vec<u8> {
        unsafe {
            let buffer = &mut *self.buffer.get();

            if size > buffer.capacity() {
                // avoid reallocate
                let mut new_buffer = vec![0; size];
                std::mem::swap(buffer, &mut new_buffer);
            } else if size > buffer.len() {
                buffer.resize(size, 0);
            } else {
                buffer.truncate(size);
            }
            buffer
        }
    }

    pub(in crate::io::read::block::parquet) fn borrow_bytes(&self) -> Bytes {
        unsafe {
            let vec = &mut *self.buffer.get();
            Bytes::from_static(std::slice::from_raw_parts_mut(vec.as_mut_ptr(), vec.len()))
        }
    }
}
