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
use std::io::BufRead;
use std::io::Cursor;

use common_datavalues::ArrayDeserializer;
use common_datavalues::StringDeserializer;
use common_datavalues::StructDeserializer;
use common_datavalues::VariantDeserializer;
use common_exception::Result;
use common_io::consts::FALSE_BYTES_LOWER;
use common_io::consts::INF_BYTES_LOWER;
use common_io::consts::NULL_BYTES_ESCAPE;
use common_io::consts::TRUE_BYTES_LOWER;
use common_io::cursor_ext::ReadBytesExt;

use crate::field_decoder::row_based::FieldDecoderRowBased;
use crate::field_decoder::values::FieldDecoderValues;
use crate::CommonSettings;
use crate::FieldDecoder;
use crate::FileFormatOptionsExt;

#[derive(Clone)]
pub struct FieldDecoderCSV {
    pub nested: FieldDecoderValues,
    pub common_settings: CommonSettings,
}

impl FieldDecoderCSV {
    pub fn create(options: &FileFormatOptionsExt) -> Self {
        FieldDecoderCSV {
            nested: FieldDecoderValues::create(options),
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
                nan_bytes: options.stage.nan_display.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone: options.timezone,
            },
        }
    }
}

impl FieldDecoder for FieldDecoderCSV {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FieldDecoderRowBased for FieldDecoderCSV {
    fn common_settings(&self) -> &CommonSettings {
        &self.common_settings
    }

    fn ignore_field_end<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> bool {
        reader.eof()
    }

    fn read_string_inner<R: AsRef<[u8]>>(
        &self,
        reader: &mut Cursor<R>,
        out_buf: &mut Vec<u8>,
        _raw: bool,
    ) -> Result<()> {
        let buf = reader.remaining_slice();
        out_buf.extend_from_slice(buf);
        reader.consume(buf.len());
        Ok(())
    }

    fn read_string<R: AsRef<[u8]>>(
        &self,
        column: &mut StringDeserializer,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        let buf = reader.remaining_slice();
        column.builder.append_value(buf);
        reader.consume(buf.len());
        Ok(())
    }

    fn read_variant<R: AsRef<[u8]>>(
        &self,
        column: &mut VariantDeserializer,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        let buf = reader.remaining_slice();
        let len = buf.len();
        let val = serde_json::from_slice(buf)?;
        reader.consume(len);
        column.builder.append_value(val);
        column.memory_size += len;
        Ok(())
    }

    fn read_array<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayDeserializer,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        self.nested.read_array(column, reader, false)?;
        Ok(())
    }

    fn read_struct<R: AsRef<[u8]>>(
        &self,
        column: &mut StructDeserializer,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        self.nested.read_struct(column, reader, false)?;
        Ok(())
    }
}
