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

use memchr::memchr;
use std::io::BufRead;
use std::io::Cursor;
use std::io::ErrorKind;
use std::io::Result;

#[allow(dead_code)]
pub trait ReadBytesExt {
    fn peek(&mut self) -> Option<char>;
    fn peek_byte(&mut self) -> Option<u8>;
    fn ignore(&mut self, f: impl Fn(u8) -> bool) -> bool;
    fn ignores(&mut self, f: impl Fn(u8) -> bool) -> usize;
    fn ignore_byte(&mut self, b: u8) -> bool;
    fn ignore_bytes(&mut self, bs: &[u8]) -> bool;
    fn ignore_white_spaces(&mut self) -> bool;
    fn until(&mut self, delim: u8, buf: &mut Vec<u8>) -> usize;
    fn keep_read(&mut self, buf: &mut Vec<u8>, f: impl Fn(u8) -> bool) -> usize;
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
}

impl<T> ReadBytesExt for Cursor<T>
where
    T: AsRef<[u8]>,
{
    fn peek(&mut self) -> Option<char> {
        let buf = self.fill_buf().ok()?;
        if buf.is_empty() {
            None
        } else {
            Some(buf[0] as char)
        }
    }

    fn peek_byte(&mut self) -> Option<u8> {
        let buf = self.fill_buf().ok()?;
        if buf.is_empty() {
            None
        } else {
            Some(buf[0])
        }
    }

    fn ignore(&mut self, f: impl Fn(u8) -> bool) -> bool {
        match self.fill_buf() {
            Ok(available) => {
                if available.is_empty() {
                    false
                } else if f(available[0]) {
                    self.consume(1);
                    true
                } else {
                    false
                }
            }
            Err(_) => false,
        }
    }

    fn ignores(&mut self, f: impl Fn(u8) -> bool) -> usize {
        match self.fill_buf() {
            Ok(available) => {
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
            Err(_) => 0,
        }
    }

    fn ignore_byte(&mut self, b: u8) -> bool {
        self.ignore(|c| c == b)
    }

    fn ignore_bytes(&mut self, bs: &[u8]) -> bool {
        match self.fill_buf() {
            Ok(available) => {
                let len = bs.len();
                if available.len() < len {
                    return false;
                }
                let eq = available[..len].iter().zip(bs).all(|(x, y)| x == y);
                if eq {
                    self.consume(len);
                }
                eq
            }
            Err(_) => false,
        }
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

    fn ignore_white_spaces(&mut self) -> bool {
        self.ignores(|c| c.is_ascii_whitespace()) > 0
    }

    fn until(&mut self, delim: u8, buf: &mut Vec<u8>) -> usize {
        match self.fill_buf() {
            Ok(remaining_slice) => {
                let to_read = memchr(delim, remaining_slice).map_or(buf.len(), |n| n + 1);
                buf.extend_from_slice(&remaining_slice[..to_read]);
                self.consume(to_read);
                to_read
            }
            Err(_) => 0,
        }
    }

    fn keep_read(&mut self, buf: &mut Vec<u8>, f: impl Fn(u8) -> bool) -> usize {
        match self.fill_buf() {
            Ok(remaining_slice) => {
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
            Err(_) => 0,
        }
    }
}
