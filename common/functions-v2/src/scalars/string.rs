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
            |_| 0,
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
}

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
