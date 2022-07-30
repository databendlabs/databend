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

use std::io::Write;

use bstr::ByteSlice;
use common_expression::types::string::StringColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::GenericMap;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::Value;
use common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "upper",
        FunctionProperty::default(),
        |_| None,
        vectorize_string_to_string(
            |col| col.data.len(),
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
        ),
    );
    registry.register_aliases("upper", &["ucase"]);

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "lower",
        FunctionProperty::default(),
        |_| None,
        vectorize_string_to_string(
            |col| col.data.len(),
            |val, writer| {
                for (start, end, ch) in val.char_indices() {
                    if ch == '\u{FFFD}' {
                        // If char is invalid, just copy it.
                        writer.put_slice(&val.as_bytes()[start..end]);
                    } else if ch.is_ascii() {
                        writer.put_u8(ch.to_ascii_lowercase() as u8);
                    } else {
                        for x in ch.to_lowercase() {
                            writer.put_char(x);
                        }
                    }
                }
                writer.commit_row();
                Ok(())
            },
        ),
    );
    registry.register_aliases("lower", &["lcase"]);

    registry.register_1_arg::<StringType, NumberType<u64>, _, _>(
        "bit_length",
        FunctionProperty::default(),
        |_| None,
        |val| 8 * val.len() as u64,
    );

    registry.register_1_arg::<StringType, NumberType<u64>, _, _>(
        "octet_length",
        FunctionProperty::default(),
        |_| None,
        |val| val.len() as u64,
    );
    registry.register_aliases("octet_length", &["length"]);

    registry.register_1_arg::<StringType, NumberType<u64>, _, _>(
        "char_length",
        FunctionProperty::default(),
        |_| None,
        |val| match std::str::from_utf8(val) {
            Ok(s) => s.chars().count() as u64,
            Err(_) => val.len() as u64,
        },
    );
    registry.register_aliases("char_length", &["character_length"]);

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "to_base64",
        FunctionProperty::default(),
        |_| None,
        vectorize_string_to_string(
            |col| col.data.len() * 4 / 3 + col.len() * 4,
            |val, writer| {
                base64::write::EncoderWriter::new(&mut writer.data, base64::STANDARD)
                    .write_all(val)
                    .unwrap();
                writer.commit_row();
                Ok(())
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "from_base64",
        FunctionProperty::default(),
        |_| None,
        vectorize_string_to_string(
            |col| col.data.len() * 4 / 3 + col.len() * 4,
            |val, writer| {
                base64::decode_config_buf(val, base64::STANDARD, &mut writer.data).unwrap();
                writer.commit_row();
                Ok(())
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "quote",
        FunctionProperty::default(),
        |_| None,
        vectorize_string_to_string(
            |col| col.data.len() * 2,
            |val, writer| {
                for ch in val {
                    match ch {
                        0 => writer.put_slice(&[b'\\', b'0']),
                        b'\'' => writer.put_slice(&[b'\\', b'\'']),
                        b'\"' => writer.put_slice(&[b'\\', b'\"']),
                        8 => writer.put_slice(&[b'\\', b'b']),
                        b'\n' => writer.put_slice(&[b'\\', b'n']),
                        b'\r' => writer.put_slice(&[b'\\', b'r']),
                        b'\t' => writer.put_slice(&[b'\\', b't']),
                        b'\\' => writer.put_slice(&[b'\\', b'\\']),
                        c => writer.put_u8(*c),
                    }
                }
                writer.commit_row();
                Ok(())
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "reverse",
        FunctionProperty::default(),
        |_| None,
        vectorize_string_to_string(
            |col| col.data.len(),
            |val, writer| {
                let start = writer.data.len();
                writer.put_slice(val);
                let buf = &mut writer.data[start..];
                buf.reverse();
                writer.commit_row();
                Ok(())
            },
        ),
    );

    registry.register_1_arg::<StringType, NumberType<u8>, _, _>(
        "ascii",
        FunctionProperty::default(),
        |_| None,
        |val| val.first().cloned().unwrap_or_default(),
    );
}

// Vectorize string to string function with customer estimate_bytes.
fn vectorize_string_to_string(
    estimate_bytes: impl Fn(&StringColumn) -> usize + Copy,
    func: impl Fn(&[u8], &mut StringColumnBuilder) -> Result<(), String> + Copy,
) -> impl Fn(ValueRef<StringType>, &GenericMap) -> Result<Value<StringType>, String> + Copy {
    move |arg1, _| match arg1 {
        ValueRef::Scalar(val) => {
            let mut builder = StringColumnBuilder::with_capacity(1, 0);
            func(val, &mut builder)?;
            Ok(Value::Scalar(builder.build_scalar()))
        }
        ValueRef::Column(col) => {
            let data_capacity = estimate_bytes(&col);
            let mut builder = StringColumnBuilder::with_capacity(col.len(), data_capacity);
            for val in col.iter() {
                func(val, &mut builder)?;
            }

            Ok(Value::Column(builder.build()))
        }
    }
}
