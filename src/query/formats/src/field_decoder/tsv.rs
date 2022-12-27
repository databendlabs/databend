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

use std::any::Any;
use std::io::Cursor;

use common_exception::Result;
use common_expression::ArrayDeserializer;
use common_expression::StringDeserializer;
use common_expression::StructDeserializer;
use common_io::consts::FALSE_BYTES_NUM;
use common_io::consts::INF_BYTES_LOWER;
use common_io::consts::NAN_BYTES_LOWER;
use common_io::consts::NULL_BYTES_ESCAPE;
use common_io::consts::TRUE_BYTES_NUM;
use common_io::cursor_ext::BufferReadStringExt;
use common_io::cursor_ext::ReadBytesExt;

use crate::field_decoder::row_based::FieldDecoderRowBased;
use crate::CommonSettings;
use crate::FieldDecoder;
use crate::FileFormatOptionsExt;

#[derive(Clone)]
pub struct FieldDecoderTSV {
    pub common_settings: CommonSettings,
    pub quote_char: u8,
}

impl FieldDecoderTSV {
    pub fn create(options: &FileFormatOptionsExt) -> Self {
        FieldDecoderTSV {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone: options.timezone,
            },
            quote_char: options.get_quote_char(),
        }
    }
}

impl FieldDecoder for FieldDecoderTSV {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FieldDecoderRowBased for FieldDecoderTSV {
    fn common_settings(&self) -> &CommonSettings {
        &self.common_settings
    }

    fn ignore_field_end<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> bool {
        reader.eof()
    }

    fn read_string_inner<R: AsRef<[u8]>>(
        &self,
        reader: &mut Cursor<R>,
        tmp_buf: &mut Vec<u8>,
        raw: bool,
    ) -> Result<()> {
        tmp_buf.clear();
        if raw {
            reader.read_escaped_string_text(tmp_buf)
        } else {
            reader.read_quoted_text(tmp_buf, self.quote_char)
        }?;
        Ok(())
    }

    fn read_string<R: AsRef<[u8]>>(
        &self,
        column: &mut StringDeserializer,
        reader: &mut Cursor<R>,
        raw: bool,
    ) -> Result<()> {
        self.read_string_inner(reader, &mut column.data, raw)?;
        column.commit_row();
        Ok(())
    }

    fn read_array<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayDeserializer,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        reader.must_ignore_byte(b'[')?;
        let mut idx = 0;
        loop {
            let _ = reader.ignore_white_spaces();
            if reader.ignore_byte(b']') {
                break;
            }
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            self.read_field(column.inner.as_mut(), reader, false)?;
            idx += 1;
        }
        column.add_offset(idx);
        Ok(())
    }

    fn read_struct<R: AsRef<[u8]>>(
        &self,
        column: &mut StructDeserializer,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        reader.must_ignore_byte(b'(')?;
        for (idx, inner) in column.inners.iter_mut().enumerate() {
            let _ = reader.ignore_white_spaces();
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            self.read_field(inner, reader, false)?;
        }
        reader.must_ignore_byte(b')')?;
        Ok(())
    }
}
