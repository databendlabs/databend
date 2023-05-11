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

use common_expression::types::array::ArrayColumn;
use common_expression::types::ValueType;
use common_expression::Column;
use common_io::constants::FALSE_BYTES_NUM;
use common_io::constants::INF_BYTES_LOWER;
use common_io::constants::NULL_BYTES_ESCAPE;
use common_io::constants::TRUE_BYTES_NUM;
use common_meta_app::principal::TsvFileFormatParams;

use super::helpers::write_escaped_string;
use crate::field_encoder::FieldEncoderRowBased;
use crate::CommonSettings;
use crate::FileFormatOptionsExt;

pub struct FieldEncoderTSV {
    pub common_settings: CommonSettings,
    pub quote_char: u8,
}

impl FieldEncoderTSV {
    pub fn create(params: &TsvFileFormatParams, options_ext: &FileFormatOptionsExt) -> Self {
        FieldEncoderTSV {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
                nan_bytes: params.nan_display.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone: options_ext.timezone,
                disable_variant_check: options_ext.disable_variant_check,
            },
            quote_char: params.quote.as_bytes().to_vec()[0],
        }
    }
}

impl FieldEncoderRowBased for FieldEncoderTSV {
    fn common_settings(&self) -> &CommonSettings {
        &self.common_settings
    }

    fn write_string_inner(&self, in_buf: &[u8], out_buf: &mut Vec<u8>, raw: bool) {
        if raw {
            write_escaped_string(in_buf, out_buf, self.quote_char);
        } else {
            out_buf.push(self.quote_char);
            write_escaped_string(in_buf, out_buf, self.quote_char);
            out_buf.push(self.quote_char);
        }
    }

    fn write_array<T: ValueType>(
        &self,
        column: &ArrayColumn<T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        _raw: bool,
    ) {
        let start = unsafe { *column.offsets.get_unchecked(row_index) as usize };
        let end = unsafe { *column.offsets.get_unchecked(row_index + 1) as usize };
        out_buf.push(b'[');
        let inner = &T::upcast_column(column.values.clone());
        for i in start..end {
            if i != start {
                out_buf.extend_from_slice(b",");
            }
            self.write_field(inner, i, out_buf, false);
        }
        out_buf.push(b']');
    }

    fn write_map<T: ValueType>(
        &self,
        column: &ArrayColumn<T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        _raw: bool,
    ) {
        let start = unsafe { *column.offsets.get_unchecked(row_index) as usize };
        let end = unsafe { *column.offsets.get_unchecked(row_index + 1) as usize };
        out_buf.push(b'{');
        let inner = &T::upcast_column(column.values.clone());
        match inner {
            Column::Tuple(fields) => {
                for i in start..end {
                    if i != start {
                        out_buf.extend_from_slice(b",");
                    }
                    self.write_field(&fields[0], i, out_buf, false);
                    out_buf.extend_from_slice(b":");
                    self.write_field(&fields[1], i, out_buf, false);
                }
            }
            _ => unreachable!(),
        }
        out_buf.push(b'}');
    }

    fn write_tuple(&self, columns: &[Column], row_index: usize, out_buf: &mut Vec<u8>, _raw: bool) {
        out_buf.push(b'(');
        for (i, inner) in columns.iter().enumerate() {
            if i > 0 {
                out_buf.extend_from_slice(b",");
            }
            self.write_field(inner, row_index, out_buf, false);
        }
        out_buf.push(b')');
    }
}
