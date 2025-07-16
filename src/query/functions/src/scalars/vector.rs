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
use databend_common_expression::EvalContext;
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
use databend_common_vector::l1_distance;
use databend_common_vector::l1_distance_64;
use databend_common_vector::l2_distance;
use databend_common_vector::l2_distance_64;

pub fn register(registry: &mut FunctionRegistry) {
    // cosine_distance
    // This function takes two Float32 arrays as input and computes the cosine distance between them.
    registry.register_passthrough_nullable_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type, _, _>(
        "cosine_distance",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>,  Float32Type>(
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
        vectorize_with_builder_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>,  Float32Type>(
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
    // cosine_distance
    // This function takes two Float32 arrays as input and computes the l2 distance between them.
    registry.register_passthrough_nullable_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type, _, _>(
        "l2_distance",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>,  Float32Type>(
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

    registry.register_passthrough_nullable_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>, Float64Type, _, _>(
        "cosine_distance",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>,  Float64Type>(
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
        vectorize_with_builder_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>,  Float64Type>(
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
        vectorize_with_builder_2_arg::<ArrayType<Float64Type>, ArrayType<Float64Type>,  Float64Type>(
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
            let args_type = check_args_type(args_type)?;
            Some(Arc::new(Function {
                signature: FunctionSignature {
                    name: "cosine_distance".to_string(),
                    args_type: args_type.clone(),
                    return_type: DataType::Number(NumberDataType::Float32),
                },
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(|_, _| FunctionDomain::Full),
                    eval: Box::new(|args, ctx| calculate_distance(args, ctx, cosine_distance)),
                },
            }))
        }));
    registry.register_function_factory("cosine_distance", cosine_distance_factory);

    let l1_distance_factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        let args_type = check_args_type(args_type)?;
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "l1_distance".to_string(),
                args_type: args_type.clone(),
                return_type: DataType::Number(NumberDataType::Float32),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(|args, ctx| calculate_distance(args, ctx, l1_distance)),
            },
        }))
    }));
    registry.register_function_factory("l1_distance", l1_distance_factory);

    let l2_distance_factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        let args_type = check_args_type(args_type)?;
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "l2_distance".to_string(),
                args_type: args_type.clone(),
                return_type: DataType::Number(NumberDataType::Float32),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(|args, ctx| calculate_distance(args, ctx, l2_distance)),
            },
        }))
    }));
    registry.register_function_factory("l2_distance", l2_distance_factory);
}

fn check_args_type(args_type: &[DataType]) -> Option<Vec<DataType>> {
    if args_type.len() != 2 {
        return None;
    }
    let args_type0 = args_type[0].remove_nullable();
    let vector_type0 = args_type0.as_vector()?;
    let args_type1 = args_type[1].remove_nullable();
    let vector_type1 = args_type1.as_vector()?;
    match (vector_type0, vector_type1) {
        (VectorDataType::Int8(dim0), VectorDataType::Int8(dim1)) => {
            if dim0 != dim1 {
                return None;
            }
        }
        (VectorDataType::Float32(dim0), VectorDataType::Float32(dim1)) => {
            if dim0 != dim1 {
                return None;
            }
        }
        (_, _) => {
            return None;
        }
    }
    Some(args_type.to_vec())
}

fn calculate_distance<F>(
    args: &[Value<AnyType>],
    _ctx: &mut EvalContext,
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
                builder.push(F32::from(dist));
            }
            (
                ScalarRef::Vector(VectorScalarRef::Float32(lhs)),
                ScalarRef::Vector(VectorScalarRef::Float32(rhs)),
            ) => {
                let l = unsafe { std::mem::transmute::<&[F32], &[f32]>(lhs) };
                let r = unsafe { std::mem::transmute::<&[F32], &[f32]>(rhs) };
                let dist = distance_fn(l, r).unwrap();
                builder.push(F32::from(dist));
            }
            (_, _) => {
                builder.push(F32::from(f32::MAX));
            }
        }
    }
    if len_opt.is_some() {
        Value::Column(Column::Number(NumberColumn::Float32(Buffer::from(builder))))
    } else {
        Value::Scalar(Scalar::Number(NumberScalar::Float32(builder[0])))
    }
}
