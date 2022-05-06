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

use super::BufferRead;

pub trait BufferReadExt: BufferRead {
    fn ignores(&mut self, f: impl Fn(u8) -> bool) -> Result<usize>;
    fn ignore(&mut self, f: impl Fn(u8) -> bool) -> Result<bool>;
    fn ignore_byte(&mut self, b: u8) -> Result<bool>;
    fn ignore_bytes(&mut self, bs: &[u8]) -> Result<bool>;
    fn ignore_insensitive_bytes(&mut self, bs: &[u8]) -> Result<bool>;
    fn ignore_white_spaces(&mut self) -> Result<bool>;
    fn until(&mut self, delim: u8, buf: &mut Vec<u8>) -> Result<usize>;

    fn keep_read(&mut self, buf: &mut Vec<u8>, f: impl Fn(u8) -> bool) -> Result<usize>;

    fn drain(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let mut bytes = 0;
        loop {
            let available = self.fill_buf()?;
            if available.is_empty() {
                break;
            }
            let size = available.len();
            buf.extend_from_slice(available);
            bytes += size;
            self.consume(size);
        }
        Ok(bytes)
    }

    fn read_quoted_text(&mut self, buf: &mut Vec<u8>, quota: u8) -> Result<()> {
        self.must_ignore_byte(quota)?;
        self.keep_read(buf, |b| b != quota)?;
        self.must_ignore_byte(quota)
    }

    fn read_escaped_string_text(&mut self, buf: &mut Vec<u8>) -> Result<()> {
        self.keep_read(buf, |f| f != b'\t' && f != b'\n' && f != b'\\')?;
        // TODO judge escaped '\\'
        Ok(())
    }

    fn eof(&mut self) -> Result<bool> {
        let buffer = self.fill_buf()?;
        Ok(buffer.is_empty())
    }

    fn must_eof(&mut self) -> Result<()> {
        let buffer = self.fill_buf()?;
        if !buffer.is_empty() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Must reach the buffer end",
            ));
        }
        Ok(())
    }

    fn must_ignore(&mut self, f: impl Fn(u8) -> bool) -> Result<()> {
        if !self.ignore(f)? {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Expected to ignore a byte",
            ));
        }
        Ok(())
    }

    fn must_ignore_byte(&mut self, b: u8) -> Result<()> {
        if !self.ignore_byte(b)? {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("Expected to have char {}", b as char),
            ));
        }
        Ok(())
    }

    fn must_ignore_bytes(&mut self, bs: &[u8]) -> Result<()> {
        if !self.ignore_bytes(bs)? {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("Expected to have bytes {:?}", bs),
            ));
        }
        Ok(())
    }

    fn must_ignore_insensitive_bytes(&mut self, bs: &[u8]) -> Result<()> {
        if !self.ignore_insensitive_bytes(bs)? {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("Expected to have insensitive bytes {:?}", bs),
            ));
        }
        Ok(())
    }
}

impl<R> BufferReadExt for R
where R: BufferRead
{
    fn ignores(&mut self, f: impl Fn(u8) -> bool) -> Result<usize> {
        let mut bytes = 0;
        loop {
            let available = self.fill_buf()?;
            if available.is_empty() || !f(available[0]) {
                break;
            }
            bytes += 1;
            self.consume(1);
        }
        Ok(bytes)
    }

    fn ignore(&mut self, f: impl Fn(u8) -> bool) -> Result<bool> {
        let available = self.fill_buf()?;
        if available.is_empty() {
            return Ok(false);
        }
        if f(available[0]) {
            self.consume(1);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn ignore_byte(&mut self, b: u8) -> Result<bool> {
        let f = |c: u8| c == b;
        self.ignore(f)
    }

    fn ignore_bytes(&mut self, bs: &[u8]) -> Result<bool> {
        for b in bs {
            let available = self.fill_buf()?;
            if available.is_empty() || *b != available[0] {
                return Ok(false);
            }
            self.consume(1);
        }
        Ok(true)
    }

    fn ignore_insensitive_bytes(&mut self, bs: &[u8]) -> Result<bool> {
        for b in bs {
            let available = self.fill_buf()?;
            if available.is_empty() || !b.eq_ignore_ascii_case(&available[0]) {
                return Ok(false);
            }
            self.consume(1);
        }
        Ok(true)
    }

    fn ignore_white_spaces(&mut self) -> Result<bool> {
        let mut cnt = 0;
        let f = |c: u8| c.is_ascii_whitespace();
        while self.ignore(f)? {
            cnt += 1;
        }
        Ok(cnt > 0)
    }

    fn until(&mut self, delim: u8, buf: &mut Vec<u8>) -> Result<usize> {
        self.read_until(delim, buf)
    }

    fn keep_read(&mut self, buf: &mut Vec<u8>, f: impl Fn(u8) -> bool) -> Result<usize> {
        let mut bytes = 0;
        loop {
            let available = self.fill_buf()?;
            if available.is_empty() || !f(available[0]) {
                break;
            }

            buf.push(available[0]);
            bytes += 1;
            self.consume(1);
        }
        Ok(bytes)
    }
}
