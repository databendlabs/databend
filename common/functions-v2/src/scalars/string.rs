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
use bytes::BufMut;
use common_arrow::arrow::buffer::Buffer;
use common_expression::types::string::try_transform_scalar;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::ArgType;
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
            |(data, _)| data.len(),
            |val, mut writer| {
                for (start, end, ch) in val.char_indices() {
                    if ch == '\u{FFFD}' {
                        // If char is invalid, just copy it.
                        writer.put_slice(&val.as_bytes()[start..end]);
                    } else if ch.is_ascii() {
                        writer.put_u8(ch.to_ascii_uppercase() as u8);
                    } else {
                        for x in ch.to_uppercase() {
                            writer.put_slice(x.encode_utf8(&mut [0; 4]).as_bytes());
                        }
                    }
                }
                Ok(val.len())
            },
        ),
    );
    registry.register_aliases("upper", &["ucase"]);

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "lower",
        FunctionProperty::default(),
        |_| None,
        vectorize_string_to_string(
            |(data, _)| data.len(),
            |val, mut writer| {
                for (start, end, ch) in val.char_indices() {
                    if ch == '\u{FFFD}' {
                        // If char is invalid, just copy it.
                        writer.put_slice(&val.as_bytes()[start..end]);
                    } else if ch.is_ascii() {
                        writer.put_u8(ch.to_ascii_lowercase() as u8);
                    } else {
                        for x in ch.to_lowercase() {
                            writer.put_slice(x.encode_utf8(&mut [0; 4]).as_bytes());
                        }
                    }
                }
                Ok(val.len())
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
            |(data, offsets)| data.len() * 4 / 3 + (offsets.len() - 1) * 4,
            |val, buf| Ok(base64::encode_config_slice(val, base64::STANDARD, buf)),
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "from_base64",
        FunctionProperty::default(),
        |_| None,
        vectorize_string_to_string(
            |(data, offsets)| data.len() * 4 / 3 + (offsets.len() - 1) * 4,
            |val, buf| {
                base64::decode_config_slice(val, base64::STANDARD, buf).map_err(|e| e.to_string())
            },
        ),
    );
}

// Vectorize string to string function with customer estimate_bytes.
fn vectorize_string_to_string(
    estimate_bytes: impl Fn((&[u8], Buffer<u64>)) -> usize + Copy,
    func: impl Fn(&[u8], &mut [u8]) -> Result<usize, String> + Copy,
) -> impl Fn(ValueRef<StringType>, &GenericMap) -> Result<Value<StringType>, String> + Copy {
    move |arg1, _| match arg1 {
        ValueRef::Scalar(val) => {
            let offsets = vec![0, val.len() as u64];
            let data_len = estimate_bytes((val, offsets.into()));
            let scalar = try_transform_scalar(val, data_len, |val, buf| func(val, buf))?;
            Ok(Value::Scalar(scalar))
        }
        ValueRef::Column(col) => {
            let iter = StringType::iter_column(&col);
            let (data, offsets) = (&col.data, col.offsets.clone());
            let data_len = estimate_bytes((data, offsets));
            let builder =
                StringColumnBuilder::try_from_transform(iter, data_len, |val, buf| func(val, buf))?;
            Ok(Value::Column(builder.build()))
        }
    }
}
