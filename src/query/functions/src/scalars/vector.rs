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
use common_expression::types::EmptyArrayType;
use common_expression::types::Float32Type;
use common_expression::types::NumberType;
use common_expression::types::UInt64Type;
use common_expression::types::ValueType;
use common_expression::types::F32;
use common_expression::vectorize_3_arg;
use common_expression::vectorize_with_builder_3_arg;
use common_expression::FromData;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_vector::cosine_distance;

use crate::aggregates::eval_aggr;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_3_arg::<EmptyArrayType, EmptyArrayType, UInt64Type, Float32Type, _, _>(
        "cosine_distance",
        FunctionProperty::default(),
        |_, _, _| FunctionDomain::Full,
        vectorize_3_arg::<EmptyArrayType, EmptyArrayType, UInt64Type, Float32Type>(|_, _, _, _| F32::from(0.0)),
    );

    registry.register_passthrough_nullable_3_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, UInt64Type, Float32Type, _, _>(
        "cosine_distance",
        FunctionProperty::default(),
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<ArrayType<Float32Type>, ArrayType<Float32Type>, UInt64Type, Float32Type>(
            |lhs, rhs, length, output, ctx| {
                let l_f32=
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(lhs) };

                let r_f32=
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(rhs) };
                match cosine_distance(l_f32.as_slice(), r_f32.as_slice(), length as usize) {
                    Ok(dist) => {
                        let len = dist.len();
                        let column = Float32Type::from_data(dist);
                        match eval_aggr("min", vec![], &[column], len) {
                            Ok((col, _)) => {
                                let v = unsafe { col.index_unchecked(0) };
                                let val = NumberType::<F32>::try_downcast_scalar(&v).unwrap();
                                output.push(val)
                            }
                            Err(err) => {
                                ctx.set_error(output.len(), err.to_string());
                                output.push(F32::from(0.0));
                            }
                        }
                    }
                    Err(err) => {
                        ctx.set_error(output.len(), err.to_string());
                        output.push(F32::from(0.0));
                    }
                }
            }
        ),
    );
}
