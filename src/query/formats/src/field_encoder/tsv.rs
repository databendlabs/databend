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

use common_datavalues::serializations::ArraySerializer;
use common_datavalues::serializations::StructSerializer;
use common_io::consts::FALSE_BYTES_NUM;
use common_io::consts::INF_BYTES_LOWER;
use common_io::consts::NULL_BYTES_ESCAPE;
use common_io::consts::TRUE_BYTES_NUM;

use super::helpers::write_escaped_string;
use crate::field_encoder::FieldEncoderRowBased;
use crate::CommonSettings;
use crate::FileFormatOptionsExt;

pub struct FieldEncoderTSV {
    pub common_settings: CommonSettings,
    pub quote_char: u8,
}

impl FieldEncoderTSV {
    pub fn create(options: &FileFormatOptionsExt) -> Self {
        FieldEncoderTSV {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
                nan_bytes: options.stage.nan_display.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone: options.timezone,
            },
            quote_char: options.get_quote_char(),
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

    fn write_array<'a>(
        &self,
        column: &ArraySerializer<'a>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        _raw: bool,
    ) {
        let start = column.offsets[row_index] as usize;
        let end = column.offsets[row_index + 1] as usize;
        out_buf.push(b'[');
        let inner = &column.inner;
        for i in start..end {
            if i != start {
                out_buf.extend_from_slice(b",");
            }
            self.write_field(inner, i, out_buf, false);
        }
        out_buf.push(b']');
    }

    fn write_struct<'a>(
        &self,
        column: &StructSerializer<'a>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
        _raw: bool,
    ) {
        out_buf.push(b'(');
        let mut first = true;

        for inner in &column.inners {
            if !first {
                out_buf.extend_from_slice(b",");
            }
            first = false;
            self.write_field(inner, row_index, out_buf, false);
        }
        out_buf.push(b')');
    }
}
