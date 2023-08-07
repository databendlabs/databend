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

use std::io::BufRead;
use std::io::Cursor;
use std::io::ErrorKind;
use std::io::Result;

pub trait ReadBytesExt {
    fn peek(&self) -> Option<char>;
    fn ignore(&mut self, f: impl Fn(u8) -> bool) -> bool;
    fn ignores(&mut self, f: impl Fn(u8) -> bool) -> usize;
    fn ignore_byte(&mut self, b: u8) -> bool;
    fn ignore_bytes(&mut self, bs: &[u8]) -> bool;
    fn ignore_insensitive_bytes(&mut self, bs: &[u8]) -> bool;
    fn ignore_white_spaces(&mut self) -> bool;
    fn until(&mut self, delim: u8, buf: &mut Vec<u8>) -> usize;
    fn keep_read(&mut self, buf: &mut Vec<u8>, f: impl Fn(u8) -> bool) -> usize;
    fn eof(&mut self) -> bool;
    fn must_eof(&mut self) -> Result<()>;
    fn must_ignore(&mut self, f: impl Fn(u8) -> bool) -> Result<()> {
        if !self.ignore(f) {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Expected to ignore a byte",
            ));
        }
        Ok(())
    }

    fn must_ignore_byte(&mut self, b: u8) -> Result<()>;

    fn must_ignore_bytes(&mut self, bs: &[u8]) -> Result<()> {
        if !self.ignore_bytes(bs) {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("Expected to have bytes {:?}", bs),
            ));
        }
        Ok(())
    }

    fn must_ignore_insensitive_bytes(&mut self, bs: &[u8]) -> Result<()> {
        if !self.ignore_insensitive_bytes(bs) {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("Expected to have insensitive bytes {:?}", bs),
            ));
        }
        Ok(())
    }
}

impl<T> ReadBytesExt for Cursor<T>
where T: AsRef<[u8]>
{
    fn peek(&self) -> Option<char> {
        let buf = self.remaining_slice();
        if buf.is_empty() {
            None
        } else {
            Some(buf[0] as char)
        }
    }

    fn eof(&mut self) -> bool {
        self.remaining_slice().is_empty()
    }
    fn must_eof(&mut self) -> Result<()> {
        if !self.remaining_slice().is_empty() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Must reach the buffer end",
            ));
        }
        Ok(())
    }

    fn ignore(&mut self, f: impl Fn(u8) -> bool) -> bool {
        let available = self.remaining_slice();
        if available.is_empty() {
            false
        } else if f(available[0]) {
            self.consume(1);
            true
        } else {
            false
        }
    }

    fn ignores(&mut self, f: impl Fn(u8) -> bool) -> usize {
        let available = self.remaining_slice();
        if available.is_empty() {
            return 0;
        }
        for (index, byt) in available.iter().enumerate() {
            if !f(*byt) {
                self.consume(index);
                return index;
            }
        }
        let len = available.len();
        self.consume(len);
        len
    }

    fn ignore_byte(&mut self, b: u8) -> bool {
        self.ignore(|c| c == b)
    }

    fn ignore_bytes(&mut self, bs: &[u8]) -> bool {
        let available = self.remaining_slice();
        let len = bs.len();
        if available.len() < len {
            return false;
        }
        let eq = available[..len].iter().zip(bs).all(|(x, y)| x == y);
        if eq {
            BufRead::consume(self, len);
        }
        eq
    }

    fn must_ignore_byte(&mut self, b: u8) -> Result<()> {
        if !self.ignore_byte(b) {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Expected to have char '{}', got '{:?}' at pos {}",
                    b as char,
                    self.peek(),
                    self.position()
                ),
            ));
        }
        Ok(())
    }

    fn ignore_insensitive_bytes(&mut self, bs: &[u8]) -> bool {
        let available = self.remaining_slice();
        let len = bs.len();
        if available.len() < len {
            return false;
        }
        let eq = available[..len]
            .iter()
            .zip(bs)
            .all(|(x, y)| x.eq_ignore_ascii_case(y));
        if eq {
            BufRead::consume(self, len);
        }
        eq
    }

    fn ignore_white_spaces(&mut self) -> bool {
        self.ignores(|c| c.is_ascii_whitespace()) > 0
    }

    fn until(&mut self, delim: u8, buf: &mut Vec<u8>) -> usize {
        let remaining_slice = self.remaining_slice();
        let to_read =
            core::slice::memchr::memchr(delim, remaining_slice).map_or(buf.len(), |n| n + 1);
        buf.extend_from_slice(&remaining_slice[..to_read]);
        self.consume(to_read);
        to_read
    }

    fn keep_read(&mut self, buf: &mut Vec<u8>, f: impl Fn(u8) -> bool) -> usize {
        let remaining_slice = self.remaining_slice();
        let mut to_read = remaining_slice.len();
        for (i, b) in remaining_slice.iter().enumerate() {
            if !f(*b) {
                to_read = i;
                break;
            }
        }
        buf.extend_from_slice(&remaining_slice[..to_read]);
        self.consume(to_read);
        to_read
    }
}
