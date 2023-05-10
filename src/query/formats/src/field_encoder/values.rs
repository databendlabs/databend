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

use chrono_tz::Tz;
use common_expression::types::array::ArrayColumn;
use common_expression::types::ValueType;
use common_expression::Column;
use common_io::constants::FALSE_BYTES_NUM;
use common_io::constants::INF_BYTES_LONG;
use common_io::constants::INF_BYTES_LOWER;
use common_io::constants::NAN_BYTES_LOWER;
use common_io::constants::NAN_BYTES_SNAKE;
use common_io::constants::NULL_BYTES_UPPER;
use common_io::constants::TRUE_BYTES_NUM;

use super::helpers::write_escaped_string;
use crate::field_encoder::FieldEncoderRowBased;
use crate::CommonSettings;
use crate::FileFormatOptionsExt;

pub struct FieldEncoderValues {
    pub common_settings: CommonSettings,
    pub quote_char: u8,
}

impl FieldEncoderValues {
    pub fn create(options: &FileFormatOptionsExt) -> Self {
        FieldEncoderValues {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone: options.timezone,
                disable_variant_check: false,
            },
            quote_char: b'\'',
        }
    }

    pub fn create_for_http_handler(timezone: Tz) -> Self {
        FieldEncoderValues {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone,
                disable_variant_check: false,
            },
            quote_char: b'\'',
        }
    }

    // JDBC only accept "NaN" and "Infinity".
    // mysql python client will decode to python float, which is printed as 'nan' and 'inf'
    // so we still use 'nan' and 'inf' in logic test.
    // https://github.com/datafuselabs/databend/discussions/8941
    pub fn create_for_mysql_handler(timezone: Tz) -> Self {
        FieldEncoderValues {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_SNAKE.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LONG.as_bytes().to_vec(),
                timezone,
                disable_variant_check: false,
            },
            quote_char: b'\'',
        }
    }
}

impl FieldEncoderRowBased for FieldEncoderValues {
    fn common_settings(&self) -> &CommonSettings {
        &self.common_settings
    }

    fn write_string_inner(&self, in_buf: &[u8], out_buf: &mut Vec<u8>, raw: bool) {
        if raw {
            out_buf.extend_from_slice(in_buf);
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
