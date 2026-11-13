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

use std::borrow::Cow;
use std::io::Write;

use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::error_to_null;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BitmapType;
use databend_common_expression::types::GeographyType;
use databend_common_expression::types::GeometryType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableType;
use databend_common_expression::types::string::StringColumn;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_io::prelude::BinaryDisplayFormat;
use encoding_rs::Encoding;
use encoding_rs::UTF_8;

pub fn register(registry: &mut FunctionRegistry) {
    registry
        .scalar_builder("length")
        .function()
        .typed_1_arg::<BinaryType, NumberType<u64>>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .each_row(|val, _| val.len() as u64)
        .register();

    registry.register_context_dependent(|registry| {
        registry
            .scalar_builder("to_string")
            .function()
            .typed_1_arg::<BinaryType, StringType>()
            .passthrough_nullable()
            .calc_domain(|_, _| FunctionDomain::MayThrow)
            .vectorized(eval_binary_to_string)
            .register();

        registry.register_combine_nullable_1_arg::<BinaryType, StringType, _, _>(
            "try_to_string",
            |_, _| FunctionDomain::Full,
            error_to_null(eval_binary_to_string),
        );
    });

    registry.register_passthrough_nullable_2_arg::<BinaryType, StringType, StringType, _, _>(
        "to_string",
        |_, _, _| FunctionDomain::MayThrow,
        |val, format, ctx| {
            let Some(format) = format.as_scalar() else {
                return conversion_error(
                    true,
                    ctx,
                    "`format` parameter must be a scalar constant, not a column or expression",
                );
            };
            eval_binary_to_string_with_format(val, format, ctx)
        },
    );

    registry.register_combine_nullable_2_arg::<BinaryType, StringType, StringType, _, _>(
        "try_to_string",
        |_, _, _| FunctionDomain::Full,
        |val, format, ctx| {
            let Some(format) = format.as_scalar() else {
                return Value::Scalar(None);
            };
            error_to_null(|val, ctx| eval_binary_to_string_with_format(val, format, ctx))(val, ctx)
        },
    );

    registry.register_passthrough_nullable_2_arg::<BinaryType, StringType, StringType, _, _>(
        "convert_from",
        |_, _, _| FunctionDomain::MayThrow,
        |val, charset, ctx| {
            let Some(charset) = charset.as_scalar() else {
                return conversion_error(
                    true,
                    ctx,
                    "`charset` parameter must be a scalar constant, not a column or expression",
                );
            };
            eval_decode_charset(val, charset, ctx)
        },
    );

    registry.register_combine_nullable_2_arg::<BinaryType, StringType, StringType, _, _>(
        "try_convert_from",
        |_, _, _| FunctionDomain::Full,
        |val, charset, ctx| {
            let Some(charset) = charset.as_scalar() else {
                return Value::Scalar(None);
            };
            error_to_null(|val, ctx| eval_decode_charset(val, charset, ctx))(val, ctx)
        },
    );

    registry
        .scalar_builder("to_jsonb_binary")
        .function()
        .typed_1_arg::<VariantType, BinaryType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(|val, _| match val {
            Value::Scalar(val) => Value::Scalar(val.to_vec()),
            Value::Column(col) => Value::Column(col),
        })
        .register();

    registry
        .scalar_builder("to_binary")
        .function()
        .typed_1_arg::<BitmapType, BinaryType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(|val, _| match val {
            Value::Scalar(val) => Value::Scalar(val.to_vec()),
            Value::Column(col) => Value::Column(col),
        })
        .register();

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

    registry
        .scalar_builder("to_binary")
        .function()
        .typed_1_arg::<GeometryType, BinaryType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(|val, _| match val {
            Value::Scalar(val) => Value::Scalar(val.to_vec()),
            Value::Column(col) => Value::Column(col),
        })
        .register();

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

    registry
        .scalar_builder("to_binary")
        .function()
        .typed_1_arg::<GeographyType, BinaryType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(|val, _| match val {
            Value::Scalar(val) => Value::Scalar(val.0.to_vec()),
            Value::Column(col) => Value::Column(col.0),
        })
        .register();

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

    registry.register_context_dependent(|registry| {
        registry
            .scalar_builder("to_binary")
            .function()
            .typed_1_arg::<StringType, BinaryType>()
            .passthrough_nullable()
            .calc_domain(|_, _| FunctionDomain::Full)
            .vectorized(|val, ctx| match ctx.func_ctx.binary_input_format {
                BinaryDisplayFormat::Hex => eval_unhex(val, ctx),
                BinaryDisplayFormat::Base64 => eval_from_base64(val, ctx),
                BinaryDisplayFormat::Utf8 | BinaryDisplayFormat::Utf8Lossy => eval_utf8_bytes(val),
            })
            .register();
    });

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BinaryType, _, _>(
        "to_binary",
        |_, _, _| FunctionDomain::MayThrow,
        |val, format, ctx| {
            let Some(format) = format.as_scalar() else {
                return conversion_error(
                    true,
                    ctx,
                    "`format` parameter must be a scalar constant, not a column or expression",
                );
            };
            eval_string_to_binary_with_format(val, format, ctx)
        },
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BinaryType, _, _>(
        "convert_to",
        |_, _, _| FunctionDomain::MayThrow,
        |val, charset, ctx| {
            let Some(charset) = charset.as_scalar() else {
                return conversion_error(
                    true,
                    ctx,
                    "`charset` parameter must be a scalar constant, not a column or expression",
                );
            };
            eval_encode_charset(val, charset, ctx)
        },
    );

    registry.register_context_dependent(|registry| {
        registry.register_combine_nullable_1_arg::<StringType, BinaryType, _, _>(
            "try_to_binary",
            |_, _| FunctionDomain::Full,
            |val, ctx| match ctx.func_ctx.binary_input_format {
                BinaryDisplayFormat::Hex => error_to_null(eval_unhex)(val, ctx),
                BinaryDisplayFormat::Base64 => error_to_null(eval_from_base64)(val, ctx),
                BinaryDisplayFormat::Utf8 | BinaryDisplayFormat::Utf8Lossy => {
                    eval_utf8_bytes_nullable(val)
                }
            },
        );
    });

    registry.register_combine_nullable_2_arg::<StringType, StringType, BinaryType, _, _>(
        "try_to_binary",
        |_, _, _| FunctionDomain::Full,
        |val, format, ctx| {
            let Some(format) = format.as_scalar() else {
                return Value::Scalar(None);
            };
            error_to_null(|val, ctx| eval_string_to_binary_with_format(val, format, ctx))(val, ctx)
        },
    );

    registry.register_combine_nullable_2_arg::<StringType, StringType, BinaryType, _, _>(
        "try_convert_to",
        |_, _, _| FunctionDomain::Full,
        |val, charset, ctx| {
            let Some(charset) = charset.as_scalar() else {
                return Value::Scalar(None);
            };
            error_to_null(|val, ctx| eval_encode_charset(val, charset, ctx))(val, ctx)
        },
    );

    registry
        .scalar_builder("to_hex")
        .aliases(&["hex_encode"])
        .function()
        .typed_1_arg::<BinaryType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_binary_to_string(
            |col| col.total_bytes_len() * 2,
            |val, output, _| {
                write_hex_lower(val, output);
                output.commit_row();
            },
        ))
        .register();

    registry
        .scalar_builder("hex")
        .function()
        .typed_1_arg::<BinaryType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_binary_to_string(
            |col| col.total_bytes_len() * 2,
            |val, output, _| {
                write_hex_lower(val, output);
                output.commit_row();
            },
        ))
        .register();

    registry
        .scalar_builder("from_hex")
        .aliases(&["unhex", "hex_decode_binary"])
        .function()
        .typed_1_arg::<StringType, BinaryType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(eval_unhex)
        .register();

    registry.register_aliases("try_from_hex", &["try_hex_decode_binary"]);
    registry.register_combine_nullable_1_arg::<StringType, BinaryType, _, _>(
        "try_from_hex",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_unhex),
    );

    registry
        .scalar_builder("to_base64")
        .aliases(&["base64_encode"])
        .function()
        .typed_1_arg::<BinaryType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_binary_to_string(
            |col| col.total_bytes_len() * 4 / 3 + col.len() * 4,
            |val, output, _| {
                write_base64(val, output);
                output.commit_row();
            },
        ))
        .register();

    registry
        .scalar_builder("from_base64")
        .aliases(&["base64_decode_binary"])
        .function()
        .typed_1_arg::<StringType, BinaryType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(eval_from_base64)
        .register();

    registry.register_aliases("try_from_base64", &["try_base64_decode_binary"]);
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
            let decoded = if matches!(
                ctx.func_ctx.binary_output_format,
                BinaryDisplayFormat::Utf8Lossy
            ) {
                Cow::Owned(String::from_utf8_lossy(val).into_owned())
            } else if let Ok(val) = simdutf8::basic::from_utf8(val) {
                Cow::Borrowed(val)
            } else {
                ctx.set_error(
                    output.len(),
                    "invalid utf8 sequence; consider setting binary_output_format to 'utf-8-lossy'",
                );
                output.commit_row();
                return;
            };
            output.put_str(&decoded);
            output.commit_row();
        },
    )(val, ctx)
}

fn eval_utf8_bytes(val: Value<StringType>) -> Value<BinaryType> {
    match val {
        Value::Scalar(val) => Value::Scalar(val.as_bytes().to_vec()),
        Value::Column(col) => Value::Column(col.into()),
    }
}

/// `to_binary(string, format)`: `format` is `hex`, `base64`, or a charset label such as
/// `utf-8`, `gbk`, `gb18030`, `big5`, `shift_jis`, `windows-1252`.
fn eval_string_to_binary_with_format(
    val: Value<StringType>,
    format: &str,
    ctx: &mut EvalContext,
) -> Value<BinaryType> {
    match format.to_ascii_lowercase().as_str() {
        "hex" => eval_unhex(val, ctx),
        "base64" => eval_from_base64(val, ctx),
        _ => eval_encode_charset(val, format, ctx),
    }
}

/// `to_string(binary, format)`: the inverse of [`eval_string_to_binary_with_format`].
fn eval_binary_to_string_with_format(
    val: Value<BinaryType>,
    format: &str,
    ctx: &mut EvalContext,
) -> Value<StringType> {
    match format.to_ascii_lowercase().as_str() {
        "hex" => vectorize_binary_to_string(
            |col| col.total_bytes_len() * 2,
            |val, output, _| {
                write_hex_lower(val, output);
                output.commit_row();
            },
        )(val, ctx),
        "base64" => vectorize_binary_to_string(
            |col| col.total_bytes_len() * 4 / 3 + col.len() * 4,
            |val, output, _| {
                write_base64(val, output);
                output.commit_row();
            },
        )(val, ctx),
        _ => eval_decode_charset(val, format, ctx),
    }
}

/// Resolve a charset label (WHATWG encoding label, case-insensitive) such as `utf-8`, `gbk`,
/// `gb18030`, `big5`, `shift_jis`, `euc-kr`, `windows-1252`, `utf-16le`.
fn resolve_charset(label: &str) -> Result<&'static Encoding, String> {
    Encoding::for_label(label.as_bytes()).ok_or_else(|| format!("unsupported charset '{label}'"))
}

/// Validation failures apply to every active row, not just row 0 (which may be NULL).
/// Retain column shape so nullable adapters can preserve the input validity bitmap.
fn conversion_error<O: ReturnType>(
    is_column: bool,
    ctx: &mut EvalContext,
    message: &str,
) -> Value<O> {
    let rows = if is_column { ctx.num_rows } else { 1 };
    let mut builder = O::create_builder(rows, ctx.generics);
    for _ in 0..rows {
        O::push_default(&mut builder);
    }
    for row in 0..if is_column { rows } else { ctx.num_rows.max(1) } {
        ctx.set_error(row, message);
    }
    if is_column {
        Value::Column(O::build_column(builder))
    } else {
        Value::Scalar(O::build_scalar(builder))
    }
}

/// Decode bytes in `charset` to a UTF-8 string. Malformed input is an error, not a U+FFFD.
fn eval_decode_charset(
    val: Value<BinaryType>,
    charset: &str,
    ctx: &mut EvalContext,
) -> Value<StringType> {
    let encoding = match resolve_charset(charset) {
        Ok(encoding) => encoding,
        Err(message) => return conversion_error(matches!(val, Value::Column(_)), ctx, &message),
    };
    vectorize_binary_to_string(
        |col| col.total_bytes_len(),
        |val, output, ctx| {
            match encoding.decode_without_bom_handling_and_without_replacement(val) {
                Some(decoded) => output.put_str(&decoded),
                None => ctx.set_error(
                    output.len(),
                    format!("invalid {} byte sequence", encoding.name()),
                ),
            }
            output.commit_row();
        },
    )(val, ctx)
}

/// Encode a UTF-8 string into bytes in `charset`. Characters that the charset cannot represent
/// are an error, not a `?`/numeric reference substitution.
fn eval_encode_charset(
    val: Value<StringType>,
    charset: &str,
    ctx: &mut EvalContext,
) -> Value<BinaryType> {
    let encoding = match resolve_charset(charset) {
        Ok(encoding) => encoding,
        Err(message) => return conversion_error(matches!(val, Value::Column(_)), ctx, &message),
    };
    if encoding == UTF_8 {
        return eval_utf8_bytes(val);
    }
    // encoding_rs only decodes UTF-16 and `replacement`; encoding them falls back to UTF-8.
    if encoding.output_encoding() != encoding {
        return conversion_error(
            matches!(val, Value::Column(_)),
            ctx,
            &format!(
                "charset '{}' is not supported for encoding",
                encoding.name()
            ),
        );
    }
    vectorize_string_to_binary(
        |col| col.total_bytes_len(),
        |val, output, ctx| {
            let (encoded, _, had_unmappable) = encoding.encode(val);
            if had_unmappable {
                ctx.set_error(
                    output.len(),
                    format!(
                        "string contains characters that cannot be encoded in {}",
                        encoding.name()
                    ),
                );
            }
            output.put_slice(&encoded);
            output.commit_row();
        },
    )(val, ctx)
}

fn eval_utf8_bytes_nullable(val: Value<StringType>) -> Value<NullableType<BinaryType>> {
    match val {
        Value::Scalar(val) => Value::Scalar(Some(val.as_bytes().to_vec())),
        Value::Column(col) => {
            let validity = Bitmap::new_constant(true, col.len());
            Value::Column(NullableColumn::new(col.into(), validity))
        }
    }
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

pub fn write_hex_lower(bytes: &[u8], output: &mut StringColumnBuilder) {
    let old_len = output.row_buffer.len();
    output.row_buffer.resize(old_len + bytes.len() * 2, 0);
    hex::encode_to_slice(bytes, &mut output.row_buffer[old_len..]).unwrap();
}

fn write_base64(bytes: &[u8], output: &mut StringColumnBuilder) {
    base64::write::EncoderWriter::new(
        &mut output.row_buffer,
        &base64::engine::general_purpose::STANDARD,
    )
    .write_all(bytes)
    .unwrap();
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
