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

use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VectorDataType;
use databend_common_expression::types::VectorScalarRef;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::Column;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_openai::OpenAI;
use databend_common_vector::cosine_distance;
use databend_common_vector::cosine_distance_64;
use databend_common_vector::inner_product;
use databend_common_vector::inner_product_64;
use databend_common_vector::l1_distance;
use databend_common_vector::l1_distance_64;
use databend_common_vector::l2_distance;
use databend_common_vector::l2_distance_64;
use databend_common_vector::vector_norm;

pub fn register(registry: &mut FunctionRegistry) {
    // cosine_distance
    // This function takes two Float32 arrays as input and computes the cosine distance between them.
    registry.register_passthrough_nullable_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type, _, _>(
        "cosine_distance",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type>(
            |lhs, rhs, output, ctx| {
                let l =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(rhs) };

                match cosine_distance(l.as_slice(), r.as_slice()) {
                    Ok(dist) => {
                        output.push(F32::from(dist));
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(F32::from(0.0));
                    }
                }
            }
        ),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type, _, _>(
        "l1_distance",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type>(
            |lhs, rhs, output, ctx| {
                let l =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(rhs) };

                match l1_distance(l.as_slice(), r.as_slice()) {
                    Ok(dist) => {
                        output.push(F32::from(dist));
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(F32::from(0.0));
                    }
                }
            }
        ),
    );

    // L2 distance
    // This function takes two Float32 arrays as input and computes the l2 distance between them.
    registry.register_passthrough_nullable_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type, _, _>(
        "l2_distance",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type>(
            |lhs, rhs, output, ctx| {
                let l =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(rhs) };

                match l2_distance(l.as_slice(), r.as_slice()) {
                    Ok(dist) => {
                        output.push(F32::from(dist));
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(F32::from(0.0));
                    }
                }
            }
        ),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type, _, _>(
        "inner_product",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type>(
            |lhs, rhs, output, ctx| {
                let l =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(rhs) };

                match inner_product(l.as_slice(), r.as_slice()) {
                    Ok(dist) => {
                        output.push(F32::from(dist));
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(F32::from(0.0));
                    }
                }
            }
        ),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>, Float64Type, _, _>(
        "cosine_distance",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>, Float64Type>(
            |lhs, rhs, output, ctx| {
                let l =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(rhs) };

                match cosine_distance_64(l.as_slice(), r.as_slice()) {
                    Ok(dist) => {
                        output.push(F64::from(dist));
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(F64::from(0.0));
                    }
                }
            }
        ),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>, Float64Type, _, _>(
        "l1_distance",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>, Float64Type>(
            |lhs, rhs, output, ctx| {
                let l =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(rhs) };

                match l1_distance_64(l.as_slice(), r.as_slice()) {
                    Ok(dist) => {
                        output.push(F64::from(dist));
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(F64::from(0.0));
                    }
                }
            }
        ),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>, Float64Type, _, _>(
        "l2_distance",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>, Float64Type>(
            |lhs, rhs, output, ctx| {
                let l =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(rhs) };

                match l2_distance_64(l.as_slice(), r.as_slice()) {
                    Ok(dist) => {
                        output.push(F64::from(dist));
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(F64::from(0.0));
                    }
                }
            }
        ),
    );

    registry.register_passthrough_nullable_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>, Float64Type, _, _>(
        "inner_product",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>, Float64Type>(
            |lhs, rhs, output, ctx| {
                let l =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(rhs) };

                match inner_product_64(l.as_slice(), r.as_slice()) {
                    Ok(dist) => {
                        output.push(F64::from(dist));
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(F64::from(0.0));
                    }
                }
            }
        ),
    );

    // embedding_vector
    // This function takes two strings as input, sends an API request to OpenAI, and returns the Float32 array of embeddings.
    // The OpenAI API key is pre-configured during the binder phase, so we rewrite this function and set the API key.
    registry.register_passthrough_nullable_1_arg::<StringType, ArrayType<Float32Type>, _, _>(
        "ai_embedding_vector",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, ArrayType<Float32Type>>(|data, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(vec![F32::from(0.0)].into());
                    return;
                }
            }

            if ctx.func_ctx.openai_api_key.is_empty() {
                ctx.set_error(output.len(), "openai_api_key is empty".to_string());
                output.push(vec![F32::from(0.0)].into());
                return;
            }
            let api_base = ctx.func_ctx.openai_api_embedding_base_url.clone();
            let api_key = ctx.func_ctx.openai_api_key.clone();
            let api_version = ctx.func_ctx.openai_api_version.clone();
            let embedding_model = ctx.func_ctx.openai_api_embedding_model.clone();
            let completion_model = ctx.func_ctx.openai_api_completion_model.clone();

            let openai = OpenAI::create(
                api_base,
                api_key,
                api_version,
                embedding_model,
                completion_model,
            );
            let result = openai.embedding_request(&[data.to_string()]);
            match result {
                Ok((embeddings, _)) => {
                    let result = embeddings[0]
                        .iter()
                        .map(|x| F32::from(*x))
                        .collect::<Vec<F32>>();
                    output.push(result.into());
                }
                Err(e) => {
                    ctx.set_error(
                        output.len(),
                        format!("openai embedding request error:{:?}", e),
                    );
                    output.push(vec![F32::from(0.0)].into());
                }
            }
        }),
    );

    // text_completion
    // This function takes two strings as input, sends an API request to OpenAI, and returns the AI-generated completion as a string.
    // The OpenAI API key is pre-configured during the binder phase, so we rewrite this function and set the API key.
    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "ai_text_completion",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, StringType>(|data, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.put_and_commit("");
                    return;
                }
            }

            if ctx.func_ctx.openai_api_key.is_empty() {
                ctx.set_error(output.len(), "openai_api_key is empty".to_string());
                output.put_and_commit("");
                return;
            }
            let api_base = ctx.func_ctx.openai_api_chat_base_url.clone();
            let api_key = ctx.func_ctx.openai_api_key.clone();
            let api_version = ctx.func_ctx.openai_api_version.clone();
            let embedding_model = ctx.func_ctx.openai_api_embedding_model.clone();
            let completion_model = ctx.func_ctx.openai_api_completion_model.clone();

            let openai = OpenAI::create(
                api_base,
                api_key,
                api_version,
                embedding_model,
                completion_model,
            );
            let result = openai.completion_text_request(data.to_string());
            match result {
                Ok((resp, _)) => {
                    output.put_and_commit(resp);
                }
                Err(e) => {
                    ctx.set_error(
                        output.len(),
                        format!("openai completion request error:{:?}", e),
                    );
                    output.put_and_commit("");
                }
            }
        }),
    );

    let cosine_distance_factory =
        FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
            let return_type = check_args_type(args_type)?;
            let is_nullable = return_type.is_nullable_or_null();
            Some(Arc::new(Function {
                signature: FunctionSignature {
                    name: "cosine_distance".to_string(),
                    args_type: args_type.to_vec(),
                    return_type,
                },
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(|_, _| FunctionDomain::Full),
                    eval: Box::new(move |args, _ctx| {
                        calculate_distance(args, is_nullable, cosine_distance)
                    }),
                },
            }))
        }));
    registry.register_function_factory("cosine_distance", cosine_distance_factory);

    let l1_distance_factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        let return_type = check_args_type(args_type)?;
        let is_nullable = return_type.is_nullable_or_null();
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "l1_distance".to_string(),
                args_type: args_type.to_vec(),
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(move |args, _ctx| {
                    calculate_distance(args, is_nullable, l1_distance)
                }),
            },
        }))
    }));
    registry.register_function_factory("l1_distance", l1_distance_factory);

    let l2_distance_factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        let return_type = check_args_type(args_type)?;
        let is_nullable = return_type.is_nullable_or_null();
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "l2_distance".to_string(),
                args_type: args_type.to_vec(),
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(move |args, _ctx| {
                    calculate_distance(args, is_nullable, l2_distance)
                }),
            },
        }))
    }));
    registry.register_function_factory("l2_distance", l2_distance_factory);

    let inner_product_factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        let return_type = check_args_type(args_type)?;
        let is_nullable = return_type.is_nullable_or_null();
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "inner_product".to_string(),
                args_type: args_type.to_vec(),
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(move |args, _ctx| {
                    calculate_distance(args, is_nullable, inner_product)
                }),
            },
        }))
    }));
    registry.register_function_factory("inner_product", inner_product_factory);

    let vector_dims_factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.len() != 1 {
            return None;
        }
        let arg_type = args_type[0].remove_nullable();
        if !matches!(arg_type, DataType::Vector(_) | DataType::Null) {
            return None;
        }
        let return_type = if args_type[0].is_nullable_or_null() {
            DataType::Number(NumberDataType::Int32).wrap_nullable()
        } else {
            DataType::Number(NumberDataType::Int32)
        };
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "vector_dims".to_string(),
                args_type: args_type.to_vec(),
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(|args, _ctx| match &args[0] {
                    Value::Scalar(scalar) => match scalar {
                        Scalar::Null => Value::Scalar(Scalar::Null),
                        Scalar::Vector(vector) => Value::Scalar(Scalar::Number(
                            NumberScalar::Int32(vector.dimension() as i32),
                        )),
                        _ => unreachable!(),
                    },
                    Value::Column(column) => {
                        let len = column.len();
                        let (_, validity) = column.validity();
                        let column = column.remove_nullable();
                        let vector_column = column.as_vector().unwrap();
                        let dimension = vector_column.dimension() as i32;
                        let dimension_column = Value::Column(Column::Number(NumberColumn::Int32(
                            Buffer::from(vec![dimension; len]),
                        )));
                        if validity.is_some() {
                            dimension_column.wrap_nullable(validity.cloned())
                        } else {
                            dimension_column
                        }
                    }
                }),
            },
        }))
    }));
    registry.register_function_factory("vector_dims", vector_dims_factory);

    let vector_norm_factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.len() != 1 {
            return None;
        }
        let arg_type = args_type[0].remove_nullable();
        if !matches!(arg_type, DataType::Vector(_) | DataType::Null) {
            return None;
        }
        let return_type = if args_type[0].is_nullable_or_null() {
            DataType::Number(NumberDataType::Float32).wrap_nullable()
        } else {
            DataType::Number(NumberDataType::Float32)
        };
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "vector_norm".to_string(),
                args_type: args_type.to_vec(),
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(|args, _ctx| match &args[0] {
                    Value::Scalar(scalar) => match scalar {
                        Scalar::Null => Value::Scalar(Scalar::Null),
                        Scalar::Vector(vector) => {
                            let norm = calculate_norm(&vector.as_ref());
                            Value::Scalar(Scalar::Number(NumberScalar::Float32(norm.into())))
                        }
                        _ => unreachable!(),
                    },
                    Value::Column(column) => {
                        let len = column.len();
                        let (_, validity) = column.validity();
                        let mut builder = Vec::with_capacity(len);
                        for i in 0..len {
                            let scalar = unsafe { column.index_unchecked(i) };
                            match scalar {
                                ScalarRef::Null => {
                                    builder.push(F32::default());
                                }
                                ScalarRef::Vector(vector) => {
                                    let norm = calculate_norm(&vector);
                                    builder.push(F32::from(norm));
                                }
                                _ => unreachable!(),
                            }
                        }
                        let norm_column = Value::Column(Column::Number(NumberColumn::Float32(
                            Buffer::from(builder),
                        )));
                        if validity.is_some() {
                            norm_column.wrap_nullable(validity.cloned())
                        } else {
                            norm_column
                        }
                    }
                }),
            },
        }))
    }));
    registry.register_function_factory("vector_norm", vector_norm_factory);
}

fn check_args_type(args_type: &[DataType]) -> Option<DataType> {
    if args_type.len() != 2 {
        return None;
    }
    let args_type0 = args_type[0].remove_nullable();
    let args_type1 = args_type[1].remove_nullable();
    if !matches!(args_type0, DataType::Vector(_) | DataType::Null)
        || !matches!(args_type1, DataType::Vector(_) | DataType::Null)
    {
        return None;
    }
    match (args_type0, args_type1) {
        (
            DataType::Vector(VectorDataType::Int8(dim0)),
            DataType::Vector(VectorDataType::Int8(dim1)),
        ) => {
            if dim0 != dim1 {
                return None;
            }
        }
        (
            DataType::Vector(VectorDataType::Float32(dim0)),
            DataType::Vector(VectorDataType::Float32(dim1)),
        ) => {
            if dim0 != dim1 {
                return None;
            }
        }
        (_, _) => {}
    }
    let return_type = if args_type[0].is_nullable_or_null() || args_type[1].is_nullable_or_null() {
        DataType::Number(NumberDataType::Float32).wrap_nullable()
    } else {
        DataType::Number(NumberDataType::Float32)
    };

    Some(return_type)
}

fn calculate_distance<F>(
    args: &[Value<AnyType>],
    is_nullable: bool,
    distance_fn: F,
) -> Value<AnyType>
where
    F: Fn(&[f32], &[f32]) -> Result<f32>,
{
    let len_opt = args.iter().find_map(|arg| match arg {
        Value::Column(col) => Some(col.len()),
        _ => None,
    });
    let len = len_opt.unwrap_or(1);
    let mut validity = MutableBitmap::with_capacity(len);
    let mut builder = Vec::with_capacity(len);
    for i in 0..len {
        let lhs = unsafe { args[0].index_unchecked(i) };
        let rhs = unsafe { args[1].index_unchecked(i) };
        match (lhs, rhs) {
            (
                ScalarRef::Vector(VectorScalarRef::Int8(lhs)),
                ScalarRef::Vector(VectorScalarRef::Int8(rhs)),
            ) => {
                let l: Vec<_> = lhs.iter().map(|v| *v as f32).collect();
                let r: Vec<_> = rhs.iter().map(|v| *v as f32).collect();
                let dist = distance_fn(l.as_slice(), r.as_slice()).unwrap();
                validity.push(true);
                builder.push(F32::from(dist));
            }
            (
                ScalarRef::Vector(VectorScalarRef::Float32(lhs)),
                ScalarRef::Vector(VectorScalarRef::Float32(rhs)),
            ) => {
                let l = unsafe { std::mem::transmute::<&[F32], &[f32]>(lhs) };
                let r = unsafe { std::mem::transmute::<&[F32], &[f32]>(rhs) };
                let dist = distance_fn(l, r).unwrap();
                validity.push(true);
                builder.push(F32::from(dist));
            }
            (_, _) => {
                validity.push(false);
                builder.push(F32::default());
            }
        }
    }

    let validity: Bitmap = validity.into();
    if len_opt.is_some() {
        let column = Value::Column(Column::Number(NumberColumn::Float32(Buffer::from(builder))));
        if is_nullable {
            column.wrap_nullable(Some(validity))
        } else {
            column
        }
    } else if !validity.get_bit(0) {
        Value::Scalar(Scalar::Null)
    } else {
        Value::Scalar(Scalar::Number(NumberScalar::Float32(builder[0])))
    }
}

fn calculate_norm(value: &VectorScalarRef) -> f32 {
    match value {
        VectorScalarRef::Int8(vals) => {
            let v: Vec<_> = vals.iter().map(|v| *v as f32).collect();
            vector_norm(&v)
        }
        VectorScalarRef::Float32(vals) => {
            let v = unsafe { std::mem::transmute::<&[F32], &[f32]>(vals) };
            vector_norm(v)
        }
    }
}
