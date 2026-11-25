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

//! Text conversions of `TIMESTAMP_NTZ`.
//!
//! None of these functions read the session timezone, so they are registered
//! as context independent and can be folded at planning time.

use std::io::Write;

use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::error_to_null;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampNtzType;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::types::timestamp_ntz::string_to_timestamp_ntz;
use databend_common_expression::types::timestamp_ntz::timestamp_ntz_to_string;
use databend_common_expression::vectorize_with_builder_1_arg;

pub(crate) fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, TimestampNtzType, _>(
        "to_timestamp_ntz",
        |_, _| FunctionDomain::MayThrow,
        eval_string_to_timestamp_ntz,
    );
    registry.register_combine_nullable_1_arg::<StringType, TimestampNtzType, _, _>(
        "try_to_timestamp_ntz",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_string_to_timestamp_ntz),
    );

    registry.register_passthrough_nullable_1_arg::<TimestampNtzType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<TimestampNtzType, StringType>(|val, output, _| {
            write!(output.row_buffer, "{}", timestamp_ntz_to_string(val)).unwrap();
            output.commit_row();
        }),
    );
    registry.register_combine_nullable_1_arg::<TimestampNtzType, StringType, _, _>(
        "try_to_string",
        |_, _| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(StringDomain {
                    min: "".to_string(),
                    max: None,
                })),
            })
        },
        vectorize_with_builder_1_arg::<TimestampNtzType, NullableType<StringType>>(
            |val, output, _| {
                write!(
                    output.builder.row_buffer,
                    "{}",
                    timestamp_ntz_to_string(val)
                )
                .unwrap();
                output.builder.commit_row();
                output.validity.push(true);
            },
        ),
    );
}

fn eval_string_to_timestamp_ntz(
    val: Value<StringType>,
    ctx: &mut EvalContext,
) -> Value<TimestampNtzType> {
    vectorize_with_builder_1_arg::<StringType, TimestampNtzType>(|val, output, ctx| {
        match string_to_timestamp_ntz(val) {
            Ok(micros) => output.push(micros),
            Err(e) => {
                ctx.set_error(
                    output.len(),
                    format!("cannot parse to type `TIMESTAMP_NTZ`. {}", e.message()),
                );
                output.push(0);
            }
        }
    })(val, ctx)
}
