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

use std::io::Write;

use databend_common_expression::error_to_null;
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::string::StringColumn;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BitmapType;
use databend_common_expression::types::GeographyType;
use databend_common_expression::types::GeometryType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("to_hex", &["hex"]);
    registry.register_aliases("from_hex", &["unhex"]);

    registry.register_passthrough_nullable_1_arg::<BinaryType, NumberType<u64>, _, _>(
        "length",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<BinaryType, NumberType<u64>>(|val, _| val.len() as u64),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        eval_binary_to_string,
    );

    registry.register_combine_nullable_1_arg::<BinaryType, StringType, _, _>(
        "try_to_string",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_binary_to_string),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BinaryType, _, _>(
        "to_jsonb_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            Value::Scalar(val) => Value::Scalar(val.to_vec()),
            Value::Column(col) => Value::Column(col),
        },
    );

    registry.register_passthrough_nullable_1_arg::<BitmapType, BinaryType, _, _>(
        "to_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            Value::Scalar(val) => Value::Scalar(val.to_vec()),
            Value::Column(col) => Value::Column(col),
        },
    );

    registry.register_combine_nullable_1_arg::<BitmapType, BinaryType, _, _>(
        "try_to_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            Value::Scalar(val) => Value::Scalar(Some(val.to_vec())),
            Value::Column(col) => {
                let validity = Bitmap::new_constant(true, col.len());
                Value::Column(NullableColumn::new(col, validity))
            }
        },
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, BinaryType, _, _>(
        "to_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            Value::Scalar(val) => Value::Scalar(val.to_vec()),
            Value::Column(col) => Value::Column(col),
        },
    );

    registry.register_combine_nullable_1_arg::<GeometryType, BinaryType, _, _>(
        "try_to_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            Value::Scalar(val) => Value::Scalar(Some(val.to_vec())),
            Value::Column(col) => {
                let validity = Bitmap::new_constant(true, col.len());
                Value::Column(NullableColumn::new(col, validity))
            }
        },
    );

    registry.register_passthrough_nullable_1_arg::<GeographyType, BinaryType, _, _>(
        "to_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            Value::Scalar(val) => Value::Scalar(val.0.to_vec()),
            Value::Column(col) => Value::Column(col.0),
        },
    );

    registry.register_combine_nullable_1_arg::<GeographyType, BinaryType, _, _>(
        "try_to_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            Value::Scalar(val) => Value::Scalar(Some(val.0.to_vec())),
            Value::Column(col) => {
                let validity = Bitmap::new_constant(true, col.len());
                Value::Column(NullableColumn::new(col.0, validity))
            }
        },
    );

    registry.register_passthrough_nullable_1_arg::<StringType, BinaryType, _, _>(
        "to_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            Value::Scalar(val) => Value::Scalar(val.as_bytes().to_vec()),
            Value::Column(col) => Value::Column(col.into()),
        },
    );

    registry.register_combine_nullable_1_arg::<StringType, BinaryType, _, _>(
        "try_to_binary",
        |_, _| FunctionDomain::Full,
        |val, _| match val {
            Value::Scalar(val) => Value::Scalar(Some(val.as_bytes().to_vec())),
            Value::Column(col) => {
                let validity = Bitmap::new_constant(true, col.len());
                Value::Column(NullableColumn::new(col.into(), validity))
            }
        },
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, StringType, _, _>(
        "to_hex",
        |_, _| FunctionDomain::Full,
        vectorize_binary_to_string(
            |col| col.total_bytes_len() * 2,
            |val, output, _| {
                let extra_len = val.len() * 2;
                output.row_buffer.resize(extra_len, 0);
                hex::encode_to_slice(val, &mut output.row_buffer).unwrap();
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, BinaryType, _, _>(
        "from_hex",
        |_, _| FunctionDomain::MayThrow,
        eval_unhex,
    );

    registry.register_combine_nullable_1_arg::<StringType, BinaryType, _, _>(
        "try_from_hex",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_unhex),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, StringType, _, _>(
        "to_base64",
        |_, _| FunctionDomain::Full,
        vectorize_binary_to_string(
            |col| col.total_bytes_len() * 4 / 3 + col.len() * 4,
            |val, output, _| {
                base64::write::EncoderWriter::new(
                    &mut output.row_buffer,
                    &base64::engine::general_purpose::STANDARD,
                )
                .write_all(val)
                .unwrap();
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, BinaryType, _, _>(
        "from_base64",
        |_, _| FunctionDomain::MayThrow,
        eval_from_base64,
    );

    registry.register_combine_nullable_1_arg::<StringType, BinaryType, _, _>(
        "try_from_base64",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_from_base64),
    );
}

fn eval_binary_to_string(val: Value<BinaryType>, ctx: &mut EvalContext) -> Value<StringType> {
    vectorize_binary_to_string(
        |col| col.total_bytes_len(),
        |val, output, ctx| {
            if let Ok(val) = simdutf8::basic::from_utf8(val) {
                output.put_str(val);
            } else {
                ctx.set_error(output.len(), "invalid utf8 sequence");
            }
            output.commit_row();
        },
    )(val, ctx)
}

fn eval_unhex(val: Value<StringType>, ctx: &mut EvalContext) -> Value<BinaryType> {
    vectorize_string_to_binary(
        |col| col.total_bytes_len() / 2,
        |val, output, ctx| {
            let old_len = output.data.len();
            let extra_len = val.len() / 2;
            output.data.resize(old_len + extra_len, 0);
            if let Err(err) = hex::decode_to_slice(val, &mut output.data[old_len..]) {
                ctx.set_error(output.len(), err.to_string());
            }
            output.commit_row();
        },
    )(val, ctx)
}

fn eval_from_base64(val: Value<StringType>, ctx: &mut EvalContext) -> Value<BinaryType> {
    vectorize_string_to_binary(
        |col| col.total_bytes_len() * 4 / 3 + col.len() * 4,
        |val, output, ctx| {
            if let Err(err) = base64::Engine::decode_vec(
                &base64::engine::general_purpose::STANDARD,
                val,
                &mut output.data,
            ) {
                ctx.set_error(output.len(), err.to_string());
            }
            output.commit_row();
        },
    )(val, ctx)
}

/// Binary to String scalar function with estimated output column capacity.
pub fn vectorize_binary_to_string(
    _estimate_bytes: impl Fn(&BinaryColumn) -> usize + Copy,
    func: impl Fn(&[u8], &mut StringColumnBuilder, &mut EvalContext) + Copy,
) -> impl Fn(Value<BinaryType>, &mut EvalContext) -> Value<StringType> + Copy {
    move |arg1, ctx| match arg1 {
        Value::Scalar(val) => {
            let mut builder = StringColumnBuilder::with_capacity(1);
            func(&val, &mut builder, ctx);
            Value::Scalar(builder.build_scalar())
        }
        Value::Column(col) => {
            let mut builder = StringColumnBuilder::with_capacity(col.len());
            for val in col.iter() {
                func(val, &mut builder, ctx);
            }

            Value::Column(builder.build())
        }
    }
}

/// String to Binary scalar function with estimated output column capacity.
pub fn vectorize_string_to_binary(
    estimate_bytes: impl Fn(&StringColumn) -> usize + Copy,
    func: impl Fn(&str, &mut BinaryColumnBuilder, &mut EvalContext) + Copy,
) -> impl Fn(Value<StringType>, &mut EvalContext) -> Value<BinaryType> + Copy {
    move |arg1, ctx| match arg1 {
        Value::Scalar(val) => {
            let mut builder = BinaryColumnBuilder::with_capacity(1, 0);
            func(&val, &mut builder, ctx);
            Value::Scalar(builder.build_scalar())
        }
        Value::Column(col) => {
            let data_capacity = estimate_bytes(&col);
            let mut builder = BinaryColumnBuilder::with_capacity(col.len(), data_capacity);
            for val in col.iter() {
                func(val, &mut builder, ctx);
            }

            Value::Column(builder.build())
        }
    }
}
