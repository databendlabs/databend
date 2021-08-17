// Copyright 2020 Datafuse Labs.
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
use std::io::BufReader;

use common_exception::Result;

pub trait BufReadExt {
    fn working_buffer(&mut self) -> Result<&[u8]>;
    fn ignore(&mut self, f: impl Fn(u8) -> bool) -> Result<bool>;
    fn ignore_byte(&mut self, b: u8) -> Result<bool>;
    fn ignore_bytes(&mut self, bs: &[u8]) -> Result<bool>;
    fn ignore_spaces(&mut self) -> Result<bool>;
    fn util(&mut self, delim: u8, buf: &mut Vec<u8>) -> Result<usize>;
}

impl<R> BufReadExt for BufReader<R>
where R: std::io::Read
{
    fn working_buffer(&mut self) -> Result<&[u8]> {
        let buf = self.fill_buf()?;
        Ok(buf)
    }

    fn ignore(&mut self, f: impl Fn(u8) -> bool) -> Result<bool> {
        let available = self.fill_buf()?;
        if available.len() == 0 {
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
        if bs.len() == 0 {
            return Ok(true);
        }

        let res = self.ignore_byte(bs[0])? && self.ignore_bytes(&bs[1..])?;
        Ok(res)
    }

    fn ignore_spaces(&mut self) -> Result<bool> {
        let mut cnt = 0;
        let f = |c: u8| c.is_ascii_whitespace();
        while self.ignore(f)? {
            cnt += 1;
        }
        Ok(cnt > 0)
    }

    fn util(&mut self, delim: u8, buf: &mut Vec<u8>) -> Result<usize> {
        Ok(self.read_until(delim, buf)?)
    }
}
