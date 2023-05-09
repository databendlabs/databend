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

use std::any::Any;
use std::collections::HashSet;
use std::io::Cursor;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::array::ArrayColumnBuilder;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::AnyType;
use common_expression::ColumnBuilder;
use common_io::constants::FALSE_BYTES_NUM;
use common_io::constants::INF_BYTES_LOWER;
use common_io::constants::NAN_BYTES_LOWER;
use common_io::constants::NULL_BYTES_ESCAPE;
use common_io::constants::TRUE_BYTES_NUM;
use common_io::cursor_ext::BufferReadStringExt;
use common_io::cursor_ext::ReadBytesExt;
use common_meta_app::principal::TsvFileFormatParams;

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
    pub fn create(params: &TsvFileFormatParams, options_ext: &FileFormatOptionsExt) -> Self {
        FieldDecoderTSV {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone: options_ext.timezone,
                disable_variant_check: options_ext.disable_variant_check,
            },
            quote_char: params.quote.as_bytes()[0],
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
        output: &mut Vec<u8>,
        raw: bool,
    ) -> Result<()> {
        if raw {
            reader.read_escaped_string_text(output)
        } else {
            reader.read_quoted_text(output, self.quote_char)
        }?;
        Ok(())
    }

    fn read_string<R: AsRef<[u8]>>(
        &self,
        column: &mut StringColumnBuilder,
        reader: &mut Cursor<R>,
        raw: bool,
    ) -> Result<()> {
        self.read_string_inner(reader, &mut column.data, raw)?;
        column.commit_row();
        Ok(())
    }

    fn read_array<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        reader.must_ignore_byte(b'[')?;
        for idx in 0.. {
            let _ = reader.ignore_white_spaces();
            if reader.ignore_byte(b']') {
                break;
            }
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            self.read_field(&mut column.builder, reader, false)?;
        }
        column.commit_row();
        Ok(())
    }

    fn read_map<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        const KEY: usize = 0;
        const VALUE: usize = 1;
        reader.must_ignore_byte(b'{')?;
        let mut set = HashSet::new();
        let map_builder = column.builder.as_tuple_mut().unwrap();
        for idx in 0.. {
            let _ = reader.ignore_white_spaces();
            if reader.ignore_byte(b'}') {
                break;
            }
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            self.read_field(&mut map_builder[KEY], reader, false)?;
            // check duplicate map keys
            let key = map_builder[KEY].pop().unwrap();
            if set.contains(&key) {
                return Err(ErrorCode::BadBytes(
                    "map keys have to be unique".to_string(),
                ));
            }
            map_builder[KEY].push(key.as_ref());
            set.insert(key);
            let _ = reader.ignore_white_spaces();
            reader.must_ignore_byte(b':')?;
            let _ = reader.ignore_white_spaces();
            self.read_field(&mut map_builder[VALUE], reader, false)?;
        }
        column.commit_row();
        Ok(())
    }

    fn read_tuple<R: AsRef<[u8]>>(
        &self,
        fields: &mut Vec<ColumnBuilder>,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        reader.must_ignore_byte(b'(')?;
        for (idx, field) in fields.iter_mut().enumerate() {
            let _ = reader.ignore_white_spaces();
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces();
            self.read_field(field, reader, false)?;
        }
        reader.must_ignore_byte(b')')?;
        Ok(())
    }
}
