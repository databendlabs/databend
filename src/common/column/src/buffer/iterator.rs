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

use std::iter::TrustedLen;

use super::Buffer;

/// This crates' equivalent of [`std::vec::IntoIter`] for [`Buffer`].
#[derive(Debug, Clone)]
pub struct IntoIter<T: Copy> {
    buffer: Buffer<T>,
    current: *const T,
    end: *const T,
}

impl<T: Copy> IntoIter<T> {
    #[inline]
    pub fn new(buffer: Buffer<T>) -> Self {
        let ptr = buffer.as_ptr();
        let len = buffer.len();
        Self {
            current: ptr,
            end: unsafe { ptr.add(len) },
            buffer,
        }
    }
}

impl<T: Copy> Iterator for IntoIter<T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            None
        } else {
            let value = unsafe { *self.current };
            self.current = unsafe { self.current.add(1) };
            Some(value)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = unsafe { self.end.offset_from(self.current) } as usize;
        (len, Some(len))
    }
}

impl<T: Copy> DoubleEndedIterator for IntoIter<T> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            None
        } else {
            self.end = unsafe { self.end.sub(1) };
            Some(unsafe { *self.end })
        }
    }
}

unsafe impl<T: Copy> TrustedLen for IntoIter<T> {}
