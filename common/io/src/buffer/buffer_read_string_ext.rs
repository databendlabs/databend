// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;

use super::BufferRead;
use crate::prelude::FormatSettings;

pub trait BufferReadStringExt: BufferRead {
    fn read_csv_string(&mut self, buf: &mut Vec<u8>, settings: &FormatSettings) -> Result<()>;
}

impl<R> BufferReadStringExt for R
where R: BufferRead
{
    fn read_csv_string(&mut self, buf: &mut Vec<u8>, settings: &FormatSettings) -> Result<()> {
        let read_buffer = self.fill_buf()?;
        if read_buffer.is_empty() {
            return Ok(());
        }
        let maybe_quote = read_buffer[0];
        if maybe_quote == b'\'' || maybe_quote == b'"' {
            self.consume(1);
            let mut maybe_end = false;
            loop {
                let read_buffer = self.fill_buf()?;
                if read_buffer.is_empty() {
                    // TODO(youngsofun): to be strict, the string should ends with maybe_quote too
                    return Ok(());
                } else {
                    let mut index = 0;
                    if maybe_end {
                        if read_buffer[0] == maybe_quote {
                            index = 1;
                        } else {
                            return Ok(());
                        }
                    }
                    while index < read_buffer.len() && read_buffer[index] != maybe_quote {
                        index += 1;
                    }
                    buf.extend_from_slice(&read_buffer[0..index]);
                    if index == read_buffer.len() {
                        self.consume(index);
                        maybe_end = false;
                    } else {
                        self.consume(index + 1);
                        maybe_end = true;
                    }
                }
            }
        } else {
            // Unquoted case. Look for field_delimiter or record_delimiter.
            let mut field_delimiter = b',';

            if !settings.field_delimiter.is_empty() {
                field_delimiter = settings.field_delimiter[0];
            }
            if maybe_quote == field_delimiter {
                return Ok(());
            }

            if settings.record_delimiter.is_empty()
                || settings.record_delimiter[0] == b'\r'
                || settings.record_delimiter[0] == b'\n'
            {
                let mut index = 0;
                let mut read_buffer = self.fill_buf()?;

                'outer1: loop {
                    while index < read_buffer.len() {
                        if read_buffer[index] == field_delimiter
                            || read_buffer[index] == b'\r'
                            || read_buffer[index] == b'\n'
                        {
                            break 'outer1;
                        }
                        index += 1;
                    }

                    buf.extend_from_slice(&read_buffer[..index]);
                    self.consume(index);

                    index = 0;
                    read_buffer = self.fill_buf()?;

                    if read_buffer.is_empty() {
                        break 'outer1;
                    }
                }

                buf.extend_from_slice(&read_buffer[..index]);
                self.consume(index);
            } else {
                let record_delimiter = settings.record_delimiter[0];
                let mut read_buffer = self.fill_buf()?;

                let mut index = 0;

                'outer2: loop {
                    while index < read_buffer.len() {
                        if read_buffer[index] == field_delimiter
                            || read_buffer[index] == record_delimiter
                        {
                            break 'outer2;
                        }
                        index += 1;
                    }

                    buf.extend_from_slice(&read_buffer[..index]);
                    self.consume(index);

                    index = 0;
                    read_buffer = self.fill_buf()?;

                    if read_buffer.is_empty() {
                        break 'outer2;
                    }
                }

                buf.extend_from_slice(&read_buffer[..index]);
                self.consume(index);
            }
            Ok(())
        }
    }
}
