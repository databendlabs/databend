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

//! Utility methods and structs for working with memory.

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::ops::Index;

use bytes::Bytes;

// ----------------------------------------------------------------------
// Immutable Buffer (BufferPtr) classes

/// An representation of a slice on a reference-counting and read-only byte array.
/// Sub-slices can be further created from this. The byte array will be released
/// when all slices are dropped.
///
/// TODO: Remove and replace with [`bytes::Bytes`]
#[derive(Clone, Debug, PartialEq)]
pub struct ByteBufferPtr {
    data: Bytes,
}

impl ByteBufferPtr {
    /// Creates new buffer from a vector.
    pub fn new(v: Vec<u8>) -> Self {
        Self { data: v.into() }
    }

    /// Returns slice of data in this buffer.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Returns length of this buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns whether this buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns a shallow copy of the buffer.
    /// Reference counted pointer to the data is copied.
    pub fn all(&self) -> Self {
        self.clone()
    }

    /// Returns a shallow copy of the buffer that starts with `start` position.
    pub fn start_from(&self, start: usize) -> Self {
        Self {
            data: self.data.slice(start..),
        }
    }

    /// Returns a shallow copy that is a range slice within this buffer.
    pub fn range(&self, start: usize, len: usize) -> Self {
        Self {
            data: self.data.slice(start..start + len),
        }
    }
}

impl Index<usize> for ByteBufferPtr {
    type Output = u8;

    fn index(&self, index: usize) -> &u8 {
        &self.data[index]
    }
}

impl Display for ByteBufferPtr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}", self.data)
    }
}

impl AsRef<[u8]> for ByteBufferPtr {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl From<Vec<u8>> for ByteBufferPtr {
    fn from(data: Vec<u8>) -> Self {
        Self { data: data.into() }
    }
}

impl From<Bytes> for ByteBufferPtr {
    fn from(data: Bytes) -> Self {
        Self { data }
    }
}

impl From<ByteBufferPtr> for Bytes {
    fn from(value: ByteBufferPtr) -> Self {
        value.data
    }
}

impl Eq for ByteBufferPtr {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_ptr() {
        let values = (0..50).collect();
        let ptr = ByteBufferPtr::new(values);
        assert_eq!(ptr.len(), 50);
        assert_eq!(ptr[40], 40);

        let ptr2 = ptr.all();
        assert_eq!(ptr2.len(), 50);
        assert_eq!(ptr2[40], 40);

        let ptr3 = ptr.start_from(20);
        assert_eq!(ptr3.len(), 30);
        assert_eq!(ptr3[0], 20);

        let ptr4 = ptr3.range(10, 10);
        assert_eq!(ptr4.len(), 10);
        assert_eq!(ptr4[0], 30);

        let expected: Vec<u8> = (30..40).collect();
        assert_eq!(ptr4.as_ref(), expected.as_slice());
    }
}
