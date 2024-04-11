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

use crate::cursor_ext::cursor_read_bytes_ext::ReadBytesExt;

pub trait BufferReadStringExt {
    fn read_quoted_text(&mut self, buf: &mut Vec<u8>, quota: u8) -> Result<()>;
}

impl<T> BufferReadStringExt for Cursor<T>
where
    T: AsRef<[u8]>,
{
    fn read_quoted_text(&mut self, buf: &mut Vec<u8>, quote: u8) -> Result<()> {
        self.must_ignore_byte(quote)?;

        loop {
            self.keep_read(buf, |b| b != quote && b != b'\\');
            if self.ignore_byte(quote) {
                if self.peek_byte() == Some(quote) {
                    buf.push(quote);
                    self.consume(1);
                } else {
                    return Ok(());
                }
            } else if self.ignore_byte(b'\\') {
                let b = self.fill_buf()?;
                if b.is_empty() {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidData,
                        "Expected to have terminated string literal after escaped char '\' ."
                            .to_string(),
                    ));
                }
                let c = b[0];
                self.ignore_byte(c);

                match c {
                    b'n' => buf.push(b'\n'),
                    b't' => buf.push(b'\t'),
                    b'r' => buf.push(b'\r'),
                    b'0' => buf.push(b'\0'),
                    b'\'' => buf.push(b'\''),
                    b'\\' => buf.push(b'\\'),
                    _ => {
                        buf.push(b'\\');
                        buf.push(c);
                    }
                }
            } else {
                break;
            }
        }
        Err(std::io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "Expected to have terminated string literal after quota {:?}, while consumed buf: {:?}",
                quote as char, buf
            ),
        ))
    }
}
