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

use databend_common_expression::hilbert_index_u16;
use databend_common_expression::hilbert_index_u32;
use databend_common_expression::hilbert_index_u64;
use databend_common_expression::hilbert_index_u8;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt16Type;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_1_arg::<StringType, NumberType<u64>, _, _>(
        "hilbert_key",
        |_, _| FunctionDomain::Full,
        |s: &str, _| {
            let bytes = s.as_bytes();
            let mut slice = [0u8; 8];
            let len = bytes.len().min(8);
            slice[..len].copy_from_slice(&bytes[..len]);
            u64::from_be_bytes(slice)
        },
    );

    registry.register_1_arg::<NumberType<F32>, NumberType<u32>, _, _>(
        "hilbert_key",
        |_, _| FunctionDomain::Full,
        |f: F32, _| f.to_bits(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<u64>, _, _>(
        "hilbert_key",
        |_, _| FunctionDomain::Full,
        |f: F64, _| f.to_bits(),
    );

    macro_rules! hilbert_index {
        ($t:ty, $f: expr) => {
            registry.register_passthrough_nullable_2_arg::<ArrayType<NumberType<$t>>, ArrayType<UInt16Type>, BinaryType, _, _>(
                "hilbert_index",
                |_, _, _| FunctionDomain::Full,
                vectorize_with_builder_2_arg::<
                    ArrayType<NumberType<$t>>,
                    ArrayType<UInt16Type>,
                    BinaryType,
                >(|point, states, builder, _| {
                    let index = $f(point.as_slice(), states.as_slice());
                    builder.put_slice(&index);
                    builder.commit_row();
                }),
            );

            registry.register_passthrough_nullable_2_arg::<ArrayType<NullableType<NumberType<$t>>>, ArrayType<UInt16Type>, BinaryType, _, _>(
                "hilbert_index",
                |_, _, _| FunctionDomain::Full,
                vectorize_with_builder_2_arg::<
                    ArrayType<NullableType<NumberType<$t>>>,
                    ArrayType<UInt16Type>,
                    BinaryType,
                >(|point, states, builder, _| {
                    let index = $f(point.column.as_slice(), states.as_slice());
                    builder.put_slice(&index);
                    builder.commit_row();
                }),
            );
        };
    }

    hilbert_index!(u8, hilbert_index_u8);
    hilbert_index!(u16, hilbert_index_u16);
    hilbert_index!(u32, hilbert_index_u32);
    hilbert_index!(u64, hilbert_index_u64);
}
