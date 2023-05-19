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
use std::io::BufRead;
use std::io::Cursor;

use common_exception::Result;
use common_expression::types::array::ArrayColumnBuilder;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::AnyType;
use common_expression::ColumnBuilder;
use common_io::constants::FALSE_BYTES_LOWER;
use common_io::constants::INF_BYTES_LOWER;
use common_io::constants::NAN_BYTES_LOWER;
use common_io::constants::NULL_BYTES_LOWER;
use common_io::constants::TRUE_BYTES_LOWER;
use common_io::cursor_ext::ReadBytesExt;
use common_meta_app::principal::XmlFileFormatParams;

use crate::field_decoder::row_based::FieldDecoderRowBased;
use crate::field_decoder::values::FieldDecoderValues;
use crate::CommonSettings;
use crate::FieldDecoder;
use crate::FileFormatOptionsExt;

#[derive(Clone)]
pub struct FieldDecoderXML {
    pub nested: FieldDecoderValues,
    pub common_settings: CommonSettings,
    pub ident_case_sensitive: bool,
}

impl FieldDecoderXML {
    pub fn create(_params: XmlFileFormatParams, options_ext: &FileFormatOptionsExt) -> Self {
        FieldDecoderXML {
            nested: FieldDecoderValues::create(options_ext),
            ident_case_sensitive: options_ext.ident_case_sensitive,
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_LOWER.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone: options_ext.timezone,
                disable_variant_check: options_ext.disable_variant_check,
            },
        }
    }
}

impl FieldDecoder for FieldDecoderXML {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FieldDecoderRowBased for FieldDecoderXML {
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
        column: &mut StringColumnBuilder,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        let buf = reader.remaining_slice();
        column.put_slice(buf);
        column.commit_row();
        reader.consume(buf.len());
        Ok(())
    }

    fn read_array<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        self.nested.read_array(column, reader, false)?;
        Ok(())
    }

    fn read_map<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        self.nested.read_map(column, reader, false)?;
        Ok(())
    }

    fn read_tuple<R: AsRef<[u8]>>(
        &self,
        fields: &mut Vec<ColumnBuilder>,
        reader: &mut Cursor<R>,
        _raw: bool,
    ) -> Result<()> {
        self.nested.read_tuple(fields, reader, false)?;
        Ok(())
    }
}
