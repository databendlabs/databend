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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_expression::error_to_null;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::StringType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<BinaryType, NumberType<u64>, _, _>(
        "length",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            ValueRef::Scalar(s) => Value::Scalar(s.len() as u64),
            ValueRef::Column(c) => {
                let diffs = c
                    .offsets()
                    .iter()
                    .zip(c.offsets().iter().skip(1))
                    .map(|(a, b)| b - a)
                    .collect::<Vec<_>>();

                Value::Column(diffs.into())
            }
        },
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        eval_binary_to_string,
    );

    registry.register_combine_nullable_1_arg::<BinaryType, StringType, _, _>(
        "try_to_string",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_binary_to_string),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, BinaryType, _, _>(
        "to_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            ValueRef::Scalar(val) => Value::Scalar(val.to_vec()),
            ValueRef::Column(col) => Value::Column(col),
        },
    );

    registry.register_combine_nullable_1_arg::<StringType, BinaryType, _, _>(
        "try_to_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            ValueRef::Scalar(val) => Value::Scalar(Some(val.to_vec())),
            ValueRef::Column(col) => Value::Column(NullableColumn {
                validity: Bitmap::new_constant(true, col.len()),
                column: col,
            }),
        },
    );
}

fn eval_binary_to_string(val: ValueRef<BinaryType>, ctx: &mut EvalContext) -> Value<StringType> {
    vectorize_with_builder_1_arg::<BinaryType, StringType>(|val, output, _| {
        output.put_slice(val);
        output.commit_row();
    })(val, ctx)
}
