// Copyright 2023 Datafuse Labs.
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

use common_arrow::arrow::buffer::Buffer;
use common_expression::types::ArrayType;
use common_expression::types::Float32Type;
use common_expression::types::StringType;
use common_expression::types::F32;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::FunctionDomain;
use common_expression::FunctionRegistry;
use common_openai::AIModel;
use common_openai::OpenAI;
use common_vector::cosine_distance;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, Float32Type, _, _>(
        "cosine_distance",
        |_,  _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>,  Float32Type>(
            |lhs, rhs, output, ctx| {
                let l_f32=
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(lhs) };
                let r_f32=
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(rhs) };

                match cosine_distance(l_f32.as_slice(), r_f32.as_slice()) {
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

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, ArrayType<Float32Type>, _, _>(
        "embedding_vector",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, StringType, ArrayType<Float32Type>>(
            |data, api_key, output, ctx| {
                let data = std::str::from_utf8(data).unwrap();
                let api_key = std::str::from_utf8(api_key).unwrap();
                let openai = OpenAI::create(api_key.to_string(), AIModel::TextEmbeddingAda003);
                let result = openai.embedding_request(&[data.to_string()]);
                match result {
                    Ok((embeddings, _)) => {
                        let result = embeddings[0].iter().map(|x| F32::from(*x)).collect::<Vec<F32>>();
                        output.push(result.into());
                    }
                    Err(e) => {
                        ctx.set_error(output.len(), format!("openai request error:{:?}", e));
                        output.push(vec![F32::from(0.0)].into());
                    }
                }
            },
        ),
    );
}
