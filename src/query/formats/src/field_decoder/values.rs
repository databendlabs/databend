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
use std::collections::HashSet;
use std::io::Cursor;

use chrono_tz::Tz;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ArrayDeserializer;
use common_expression::MapDeserializer;
use common_expression::NullableDeserializer;
use common_expression::StringDeserializer;
use common_expression::StructDeserializer;
use common_expression::TypeDeserializer;
use common_io::constants::FALSE_BYTES_LOWER;
use common_io::constants::INF_BYTES_LOWER;
use common_io::constants::NAN_BYTES_LOWER;
use common_io::constants::NULL_BYTES_UPPER;
use common_io::constants::TRUE_BYTES_LOWER;
use common_io::cursor_ext::BufferReadStringExt;
use common_io::cursor_ext::ReadBytesExt;
use common_io::prelude::FormatSettings;

use crate::field_decoder::row_based::FieldDecoderRowBased;
use crate::CommonSettings;
use crate::FieldDecoder;
use crate::FileFormatOptionsExt;

#[derive(Clone)]
pub struct FieldDecoderValues {
    pub common_settings: CommonSettings,
    format: FormatSettings,
}

impl FieldDecoderValues {
    pub fn create(options: &FileFormatOptionsExt) -> Self {
        FieldDecoderValues {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone: options.timezone,
            },
            format: FormatSettings {
                timezone: options.timezone,
            },
        }
    }

    pub fn create_for_insert(timezone: Tz) -> Self {
        FieldDecoderValues {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone,
            },
            format: FormatSettings { timezone },
        }
    }
}

impl FieldDecoder for FieldDecoderValues {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FieldDecoderRowBased for FieldDecoderValues {
    fn common_settings(&self) -> &CommonSettings {
        &self.common_settings
    }

    fn ignore_field_end<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> bool {
        reader.ignore_white_spaces();
        matches!(reader.peek(), None | Some(',') | Some(')'))
    }

    fn read_nullable<R: AsRef<[u8]>>(
        &self,
        column: &mut NullableDeserializer,
        reader: &mut Cursor<R>,
        raw: bool,
    ) -> Result<()> {
        if reader.eof() {
            column.de_default();
        } else if (raw && (self.match_bytes(reader, b"NULL") || self.match_bytes(reader, b"null")))
            || (!raw && (reader.ignore_bytes(b"NULL") || reader.ignore_bytes(b"null")))
        {
            column.de_default();
            return Ok(());
        } else {
            self.read_field(column.inner.as_mut(), reader, raw)?;
            column.validity.push(true);
        }
        Ok(())
    }

    fn read_string_inner<R: AsRef<[u8]>>(
        &self,
        reader: &mut Cursor<R>,
        out_buf: &mut Vec<u8>,
        _raw: bool,
    ) -> Result<()> {
        reader.read_quoted_text(out_buf, b'\'')?;
        Ok(())
    }

    fn read_string<R: AsRef<[u8]>>(
        &self,
        column: &mut StringDeserializer,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        reader.read_quoted_text(&mut column.data, b'\'')?;
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

    fn read_map<R: AsRef<[u8]>>(
        &self,
        column: &mut MapDeserializer,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        reader.must_ignore_byte(b'{')?;
        let mut idx = 0;
        let mut set = HashSet::new();
        loop {
            let _ = reader.ignore_white_spaces();
            if reader.ignore_byte(b'}') {
                break;
            }
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            self.read_field(column.key.as_mut(), reader, false)?;
            // check duplicate map keys
            let key = column.key.pop_data_value().unwrap();
            if set.contains(&key) {
                column.add_offset(idx);
                return Err(ErrorCode::BadBytes(
                    "map keys have to be unique".to_string(),
                ));
            }
            set.insert(key.clone());
            column.key.append_data_value(key, &self.format)?;
            let _ = reader.ignore_white_spaces();
            reader.must_ignore_byte(b':')?;
            let _ = reader.ignore_white_spaces();
            self.read_field(column.value.as_mut(), reader, false)?;
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
