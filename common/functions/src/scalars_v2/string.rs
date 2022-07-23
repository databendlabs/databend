// Copyright 2021 Datafuse Labs.
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

use bstr::ByteSlice;
use common_expression::types::StringType;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_with_writer_1_arg::<StringType, StringType, _, _>(
        "upper",
        FunctionProperty::default(),
        |_| None,
        |val, writer| {
            for (start, end, ch) in val.char_indices() {
                if ch == '\u{FFFD}' {
                    // If char is invalid, just copy it.
                    writer.put_slice(&val.as_bytes()[start..end]);
                } else if ch.is_ascii() {
                    writer.put_u8(ch.to_ascii_uppercase() as u8);
                } else {
                    for x in ch.to_uppercase() {
                        writer.put_char(x);
                    }
                }
            }
            writer.commit_row();
            Ok(())
        },
    );
    registry.register_aliases("upper", &["ucase"]);

    registry.register_string_2_string(
        "to_base64",
        FunctionProperty::default(),
        |_| None,
        |val, buf| Ok(base64::encode_config_slice(val, base64::STANDARD, buf)),
        |(data, offsets)| data.len() * 4 / 3 + (offsets.len() - 1) * 4,
    );

    registry.register_string_2_string(
        "from_base64",
        FunctionProperty::default(),
        |_| None,
        |val, buf| {
            base64::decode_config_slice(val, base64::STANDARD, buf).map_err(|e| e.to_string())
        },
        |(data, offsets)| data.len() * 4 / 3 + (offsets.len() - 1) * 4,
    );
}
