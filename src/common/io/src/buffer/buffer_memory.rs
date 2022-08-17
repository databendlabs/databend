// Copyright 2021 Datafuse Labs.
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

use std::fmt;
use std::io;
use std::io::IoSliceMut;
use std::io::Read;
use std::io::ReadBuf;
use std::io::Result;

use crate::buffer::BufferRead;

pub struct MemoryReader {
    inner: &'static [u8],
    owned_memory: Vec<u8>,
}

impl MemoryReader {
    pub fn new(mut owned_memory: Vec<u8>) -> MemoryReader {
        unsafe {
            let memory_ptr = owned_memory.as_mut_ptr();
            let memory_size = owned_memory.len();
            let inner = &*std::ptr::slice_from_raw_parts_mut(memory_ptr, memory_size);
            MemoryReader {
                inner,
                owned_memory,
            }
        }
    }
}

impl Read for MemoryReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.inner.read_vectored(bufs)
    }

    fn is_read_vectored(&self) -> bool {
        self.inner.is_read_vectored()
    }

    // The inner reader might have an optimized `read_to_end`. Drain our buffer and then
    // delegate to the inner implementation.
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        self.inner.read_to_end(buf)
    }

    // The inner reader might have an optimized `read_to_end`. Drain our buffer and then
    // delegate to the inner implementation.
    fn read_to_string(&mut self, buf: &mut String) -> Result<usize> {
        self.inner.read_to_string(buf)
    }

    // Small read_exacts from a BufReader are extremely common when used with a deserializer.
    // The default implementation calls read in a loop, which results in surprisingly poor code
    // generation for the common path where the buffer has enough bytes to fill the passed-in
    // buffer.
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.inner.read_exact(buf)
    }

    fn read_buf(&mut self, buf: &mut ReadBuf<'_>) -> Result<()> {
        self.inner.read_buf(buf)
    }
}

impl BufferRead for MemoryReader {
    fn working_buf(&self) -> &[u8] {
        self.inner
    }

    fn fill_buf(&mut self) -> Result<&[u8]> {
        Ok(self.inner)
    }

    fn consume(&mut self, amt: usize) {
        self.inner = &self.inner[amt..];
    }

    fn preadd_buf(&mut self, buf: &[u8]) -> Result<()> {
        unsafe {
            let mut new_inner = buf.to_vec();
            new_inner.extend_from_slice(self.inner);
            self.owned_memory = new_inner;
            let memory_ptr = self.owned_memory.as_mut_ptr();
            let memory_size = self.owned_memory.len();
            self.inner = &*std::ptr::slice_from_raw_parts_mut(memory_ptr, memory_size);
        }

        Ok(())
    }
}

impl fmt::Debug for MemoryReader {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("BufferU8Reader")
            .field("inner", &self.inner)
            .finish()
    }
}
