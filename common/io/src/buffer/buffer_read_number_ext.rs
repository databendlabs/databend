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

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use lexical_core::FromLexical;

use super::BufferRead;
use super::BufferReadExt;

pub trait BufferReadNumberExt: BufferRead {
    fn read_int_text<T: FromLexical>(&mut self) -> Result<T>;
    fn read_float_text<T: FromLexical>(&mut self) -> Result<T>;
}

impl<R> BufferReadNumberExt for R
where R: BufferRead
{
    fn read_int_text<T: FromLexical>(&mut self) -> Result<T> {
        // TODO: reuse the buf
        let mut buf = vec![];
        let mut has_point = false;
        let mut has_number = false;
        'L: loop {
            let buffer = self.fill_buf()?;
            if buffer.is_empty() {
                break;
            }

            match buffer[0] {
                b'-' | b'+' => {
                    if has_number {
                        break;
                    }
                }

                b'0'..=b'9' => {
                    has_number = true;
                }

                b'.' => {
                    has_point = true;
                    self.consume(1);
                    break;
                }
                _ => break 'L,
            }

            buf.push(buffer[0]);
            self.consume(1);
        }

        if has_point {
            let _ = self.ignores(|f| (b'0'..=b'9').contains(&f))?;
        }

        match buf.is_empty() {
            true => Ok(T::default()),
            false => match FromLexical::from_lexical(buf.as_slice()) {
                Ok(value) => Ok(value),
                Err(cause) => Err(ErrorCode::BadBytes(format!(
                    "Cannot parse value:{:?} to number type, cause: {:?}",
                    String::from_utf8(buf),
                    cause
                ))),
            },
        }
    }

    fn read_float_text<T: FromLexical>(&mut self) -> Result<T> {
        // TODO: reuse the buf
        let mut buf = vec![];
        let mut has_point = false;
        let mut has_number = false;
        'L: loop {
            let buffer = self.fill_buf()?;
            if buffer.is_empty() {
                break;
            }
            match buffer[0] {
                b'-' | b'+' => {
                    if has_number {
                        break;
                    }
                }

                b'0'..=b'9' => {
                    has_number = true;
                }

                b'.' => {
                    has_point = true;
                    self.consume(1);
                    break;
                }
                _ => break 'L,
            }

            buf.push(buffer[0]);
            self.consume(1);
        }

        if has_point {
            buf.push(b'.');
            let _ = self.keep_read(&mut buf, |f| (b'0'..=b'9').contains(&f))?;
        }

        FromLexical::from_lexical(buf.as_slice()).map_err_to_code(ErrorCode::BadBytes, || {
            format!("Cannot parse value:{:?} to number type", buf)
        })
    }
}
