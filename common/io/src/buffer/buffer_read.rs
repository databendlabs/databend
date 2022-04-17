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

use std::io::ErrorKind;
use std::io::Result;

/// Portable std::io::BufRead .
pub trait BufferRead: std::io::Read {
    fn working_buf(&self) -> &[u8];

    fn fill_buf(&mut self) -> Result<&[u8]>;

    fn consume(&mut self, amt: usize);
    // Insert buf before current working buffer
    fn preadd_buf(&mut self, buf: &[u8]) -> Result<()>;

    fn has_data_left(&mut self) -> Result<bool> {
        self.fill_buf().map(|b| !b.is_empty())
    }

    fn read_until(&mut self, byte: u8, buf: &mut Vec<u8>) -> Result<usize> {
        read_until(self, byte, buf)
    }
}

fn read_until<R: BufferRead + ?Sized>(r: &mut R, delim: u8, buf: &mut Vec<u8>) -> Result<usize> {
    let mut read = 0;
    loop {
        let (done, used) = {
            let available = match r.fill_buf() {
                Ok(n) => n,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            match core::slice::memchr::memchr(delim, available) {
                Some(i) => {
                    buf.extend_from_slice(&available[..=i]);
                    (true, i + 1)
                }
                None => {
                    buf.extend_from_slice(available);
                    (false, available.len())
                }
            }
        };
        r.consume(used);
        read += used;
        if done || used == 0 {
            return Ok(read);
        }
    }
}

impl<S: BufferRead + ?Sized> BufferRead for Box<S> {
    fn working_buf(&self) -> &[u8] {
        S::working_buf(self)
    }

    fn fill_buf(&mut self) -> Result<&[u8]> {
        S::fill_buf(self)
    }

    fn consume(&mut self, amt: usize) {
        S::consume(self, amt)
    }

    fn preadd_buf(&mut self, buf: &[u8]) -> Result<()> {
        S::preadd_buf(self, buf)
    }
}
