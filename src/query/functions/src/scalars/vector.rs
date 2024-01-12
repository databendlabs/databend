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

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_openai::OpenAI;
use databend_common_vector::cosine_distance;
use databend_common_vector::cosine_distance_64;
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
                let l=
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(rhs) };

                match cosine_distance(l.as_slice(), r .as_slice()) {
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
                let l=
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(rhs) };

                match l2_distance(l.as_slice(), r .as_slice()) {
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

                match cosine_distance_64(l.as_slice(), r .as_slice()) {
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
                let l=
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(lhs) };
                let r =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(rhs) };

                match l2_distance_64(l.as_slice(), r .as_slice()) {
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

            let data = match std::str::from_utf8(data) {
                Ok(data) => data,
                Err(_) => {
                    ctx.set_error(
                        output.len(),
                        format!("Invalid data: {:?}", String::from_utf8_lossy(data)),
                    );
                    output.push(vec![F32::from(0.0)].into());
                    return;
                }
            };
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
                    output.put_str("");
                    output.commit_row();
                    return;
                }
            }

            let data = match std::str::from_utf8(data) {
                Ok(data) => data,
                Err(_) => {
                    ctx.set_error(
                        output.len(),
                        format!("Invalid data: {:?}", String::from_utf8_lossy(data)),
                    );
                    output.put_str("");
                    output.commit_row();
                    return;
                }
            };
            if ctx.func_ctx.openai_api_key.is_empty() {
                ctx.set_error(output.len(), "openai_api_key is empty".to_string());
                output.put_str("");
                output.commit_row();
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
                    output.put_str(&resp);
                }
                Err(e) => {
                    ctx.set_error(
                        output.len(),
                        format!("openai completion request error:{:?}", e),
                    );
                    output.put_str("");
                }
            }
            output.commit_row();
        }),
    );
}
