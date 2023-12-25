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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use lexical_core::FromLexical;

pub trait ReadNumberExt {
    fn read_int_text<T: FromLexical>(&mut self) -> Result<T>;
    fn read_float_text<T: FromLexical>(&mut self) -> Result<T>;
    fn read_num_text_exact<T: FromLexical>(&mut self) -> Result<T>;
}

pub fn collect_number(buffer: &[u8]) -> (usize, usize) {
    let mut has_number = false;
    let mut index = 0;
    let len = buffer.len();
    let mut point_pos = len;

    for _ in 0..len {
        match buffer[index] {
            b'0'..=b'9' => {
                has_number = true;
            }

            b'-' | b'+' => {
                if has_number {
                    break;
                }
            }
            b'.' => {
                point_pos = index;
                index += 1;
                break;
            }
            _ => {
                break;
            }
        }
        index += 1;
    }
    if point_pos < len {
        while index < len && buffer[index].is_ascii_digit() {
            index += 1;
        }
    }

    let mut is_scientific = false;
    if has_number && index < len && (buffer[index] == b'e' || buffer[index] == b'E') {
        is_scientific = true;
        index += 1;
        if index < len && (buffer[index] == b'-' || buffer[index] == b'+') {
            index += 1
        }
        while index < len && buffer[index].is_ascii_digit() {
            index += 1;
        }
    }

    let effective = if !is_scientific
        && point_pos < len
        && buffer[point_pos + 1..index].iter().all(|x| *x == b'0')
    {
        point_pos
    } else {
        index
    };
    (index, effective)
}

#[inline]
pub fn read_num_text_exact<T: FromLexical>(buf: &[u8]) -> Result<T> {
    match buf.is_empty() {
        true => Ok(T::default()),
        false => match FromLexical::from_lexical(buf) {
            Ok(value) => Ok(value),
            Err(cause) => Err(ErrorCode::BadBytes(format!(
                "Cannot parse value:{:?} to number type, cause: {:?}",
                String::from_utf8_lossy(buf),
                cause
            ))),
        },
    }
}

impl<B> ReadNumberExt for Cursor<B>
where B: AsRef<[u8]>
{
    fn read_int_text<T: FromLexical>(&mut self) -> Result<T> {
        let buf = self.remaining_slice();
        let (n_in, n_out) = collect_number(buf);
        if n_in == 0 {
            return Err(ErrorCode::BadBytes(
                "Failed to parse number from text: input does not contain a valid numeric format.",
            ));
        }
        let n = read_num_text_exact(&buf[..n_out])?;
        self.consume(n_in);
        Ok(n)
    }

    fn read_float_text<T: FromLexical>(&mut self) -> Result<T> {
        let (n_in, n_out) = collect_number(self.remaining_slice());
        if n_in == 0 {
            return Err(ErrorCode::BadBytes(
                "Unable to parse float: provided text is not in a recognizable floating-point format.",
            ));
        }
        let n = read_num_text_exact(&self.remaining_slice()[..n_out])?;
        self.consume(n_in);
        Ok(n)
    }

    fn read_num_text_exact<T: FromLexical>(&mut self) -> Result<T> {
        let buf = self.remaining_slice();
        read_num_text_exact(buf)
    }
}
