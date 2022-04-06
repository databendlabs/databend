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

use std::cmp;
use std::fmt;
use std::io;
use std::io::IoSliceMut;
use std::io::Read;
use std::io::ReadBuf;
use std::io::Result;
use std::mem::MaybeUninit;

use super::default_read_exact;
use crate::buffer::BufferRead;

pub struct BufferU8Reader<'a> {
    inner: &'a [u8],
    buf: Box<[MaybeUninit<u8>]>,
    pos: usize,
    cap: usize,
    init: usize,
}

impl<'a> BufferU8Reader<'a> {
    pub fn new(inner: &'a [u8]) -> BufferU8Reader<'a> {
        BufferU8Reader::with_capacity(8 * 1024, inner)
    }

    pub fn with_capacity(capacity: usize, inner: &'a [u8]) -> BufferU8Reader<'a> {
        let buf = Box::new_uninit_slice(capacity);
        BufferU8Reader {
            inner,
            buf,
            pos: 0,
            cap: 0,
            init: 0,
        }
    }

    pub fn get_mut(&mut self) -> &mut &'a [u8] {
        &mut self.inner
    }

    pub fn buffer(&self) -> &[u8] {
        unsafe { MaybeUninit::slice_assume_init_ref(&self.buf[self.pos..self.cap]) }
    }

    pub fn buffer_mut(&mut self) -> &[u8] {
        unsafe { MaybeUninit::slice_assume_init_mut(&mut self.buf[self.pos..self.cap]) }
    }

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    pub fn into_inner(self) -> &'a [u8] {
        self.inner
    }

    /// Invalidates all data in the internal buffer.
    #[inline]
    fn discard_buffer(&mut self) {
        self.pos = 0;
        self.cap = 0;
    }
}

impl<'a> std::io::Read for BufferU8Reader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.pos == self.cap && buf.len() >= self.buf.len() {
            self.discard_buffer();
            return self.inner.read(buf);
        }
        let nread = {
            let mut rem = self.fill_buf()?;
            rem.read(buf)?
        };
        self.consume(nread);
        Ok(nread)
    }

    fn read_buf(&mut self, buf: &mut ReadBuf<'_>) -> io::Result<()> {
        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.pos == self.cap && buf.remaining() >= self.buf.len() {
            self.discard_buffer();
            return self.inner.read_buf(buf);
        }

        let prev = buf.filled_len();

        let mut rem = self.fill_buf()?;
        rem.read_buf(buf)?;

        self.consume(buf.filled_len() - prev); //slice impl of read_buf known to never unfill buf

        Ok(())
    }

    // Small read_exacts from a BufReader are extremely common when used with a deserializer.
    // The default implementation calls read in a loop, which results in surprisingly poor code
    // generation for the common path where the buffer has enough bytes to fill the passed-in
    // buffer.
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        if self.buffer().len() >= buf.len() {
            buf.copy_from_slice(&self.buffer()[..buf.len()]);
            self.consume(buf.len());
            return Ok(());
        }

        default_read_exact(self, buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        let total_len = bufs.iter().map(|b| b.len()).sum::<usize>();
        if self.pos == self.cap && total_len >= self.buf.len() {
            self.discard_buffer();
            return self.inner.read_vectored(bufs);
        }
        let nread = {
            let mut rem = self.fill_buf()?;
            rem.read_vectored(bufs)?
        };
        self.consume(nread);
        Ok(nread)
    }

    fn is_read_vectored(&self) -> bool {
        self.inner.is_read_vectored()
    }

    // The inner reader might have an optimized `read_to_end`. Drain our buffer and then
    // delegate to the inner implementation.
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let nread = self.cap - self.pos;
        buf.extend_from_slice(self.buffer());
        self.discard_buffer();
        Ok(nread + self.inner.read_to_end(buf)?)
    }

    // The inner reader might have an optimized `read_to_end`. Drain our buffer and then
    // delegate to the inner implementation.
    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        let mut bytes = Vec::new();
        self.read_to_end(&mut bytes)?;
        let string = std::str::from_utf8(&bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "stream did not contain valid UTF-8",
            )
        })?;
        *buf += string;
        Ok(string.len())
    }
}

impl<'a> BufferRead for BufferU8Reader<'a> {
    fn working_buf(&self) -> &[u8] {
        self.buffer()
    }

    fn fill_buf(&mut self) -> Result<&[u8]> {
        if self.pos >= self.cap {
            debug_assert!(self.pos == self.cap);
            let mut readbuf = ReadBuf::uninit(&mut self.buf);

            // SAFETY: `self.init` is either 0 or set to `readbuf.initialized_len()`
            // from the last time this function was called
            unsafe {
                readbuf.assume_init(self.init);
            }

            self.inner.read_buf(&mut readbuf)?;

            self.cap = readbuf.filled_len();
            self.init = readbuf.initialized_len();

            self.pos = 0;
        }
        Ok(self.buffer())
    }

    fn consume(&mut self, amt: usize) {
        self.pos = cmp::min(self.pos + amt, self.cap);
    }

    fn preadd_buf(&mut self, buf: &[u8]) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        // Just Copy
        if buf.len() < self.pos {
            let v = unsafe {
                MaybeUninit::slice_assume_init_mut(&mut self.buf[self.pos - buf.len()..self.pos])
            };
            v.copy_from_slice(buf);
            self.pos -= buf.len();
        } else {
            let total = buf.len() + self.cap - self.pos;
            let mut new_cap = self.cap;

            while new_cap < total {
                new_cap <<= 1;
            }
            let mut new_buf = Box::new_uninit_slice(new_cap);
            let mut readbuf = ReadBuf::uninit(&mut new_buf);
            unsafe {
                readbuf.assume_init(0);
            }

            readbuf.append(buf);
            readbuf.append(self.buffer());

            self.pos = 0;
            self.cap = readbuf.filled_len();
            self.init = readbuf.initialized_len();
            self.buf = new_buf;
        }

        Ok(())
    }
}

impl<'a> fmt::Debug for BufferU8Reader<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("BufferU8Reader")
            .field("reader", &self.inner)
            .field(
                "buffer",
                &format_args!("{}/{}", self.cap - self.pos, self.buf.len()),
            )
            .finish()
    }
}
