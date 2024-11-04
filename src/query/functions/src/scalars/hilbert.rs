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

use databend_common_expression::hilbert_index;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::FixedLengthEncoding;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, BinaryType, _, _>(
        "hilbert_key",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, BinaryType>(|val, builder, _| {
            let bytes = val.as_bytes();
            builder.put_slice(bytes);
            builder.commit_row();
        }),
    );

    for ty in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_passthrough_nullable_1_arg::<NumberType<NUM_TYPE>, BinaryType, _, _>(
                        "hilbert_key",
                        |_, _| FunctionDomain::Full,
                        vectorize_with_builder_1_arg::<NumberType<NUM_TYPE>, BinaryType>(
                            |val, builder, _| {
                                let encoded = val.encode();
                                builder.put_slice(&encoded);
                                builder.commit_row();
                            },
                        ),
                    );
            }
        })
    }

    registry.register_passthrough_nullable_2_arg::<ArrayType<BinaryType>, NumberType<u64>, BinaryType, _, _>(
        "hilbert_index",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<ArrayType<BinaryType>, NumberType<u64>, BinaryType>(
            |val, len, builder, ctx| {
                let points = val.iter().collect::<Vec<_>>();
                let dimension = points.len();

                if std::intrinsics::unlikely(len > 64) {
                    ctx.set_error(builder.len(), "Width must be less than or equal to 64");
                } else if std::intrinsics::unlikely(!(2..=5).contains(&dimension)) {
                    ctx.set_error(builder.len(), "Dimension must between 2 and 5");
                } else {
                    let slice = hilbert_index(&points, len as usize);
                    builder.put_slice(&slice);
                }

                builder.commit_row();
            },
        ),
    );
}
