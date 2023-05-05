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

use common_expression::types::bitmap::BitmapType;
use common_expression::types::ArrayType;
use common_expression::types::StringType;
use common_expression::types::UInt64Type;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::FunctionDomain;
use common_expression::FunctionRegistry;
use itertools::join;
use roaring::RoaringTreemap;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, BitmapType, _, _>(
        "to_bitmap",
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, BitmapType>(|s, builder, ctx| {
            match std::str::from_utf8(s)
                .map_err(|e| e.to_string())
                .and_then(|s| {
                    let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
                    let result: Result<Vec<u64>, String> = s
                        .split(',')
                        .map(|v| v.parse::<u64>().map_err(|e| e.to_string()))
                        .collect();
                    result
                }) {
                Ok(v) => {
                    let rb = RoaringTreemap::from_iter(v.iter());
                    rb.serialize_into(&mut builder.data).unwrap();
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, BitmapType, _, _>(
        "to_bitmap",
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, BitmapType>(|arg, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            let mut rb = RoaringTreemap::new();
            rb.insert(arg);

            rb.serialize_into(&mut builder.data).unwrap();
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<ArrayType<UInt64Type>, BitmapType, _, _>(
        "build_bitmap",
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<ArrayType<UInt64Type>, BitmapType>(|arg, builder, _ctx| {
            let mut rb = RoaringTreemap::new();
            for a in arg.iter() {
                rb.insert(*a);
            }

            rb.serialize_into(&mut builder.data).unwrap();
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BitmapType, UInt64Type, _, _>(
        "bitmap_count",
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<BitmapType, UInt64Type>(|arg, builder, ctx| {
            match RoaringTreemap::deserialize_from(arg) {
                Ok(rb) => {
                    builder.push(rb.len());
                }
                Err(e) => {
                    builder.push(0_u64);
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
        }),
    );

    registry.register_aliases("bitmap_count", &["bitmap_cardinality"]);

    registry.register_passthrough_nullable_1_arg::<BitmapType, StringType, _, _>(
        "to_string",
        |_| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BitmapType, StringType>(|b, builder, ctx| {
            match RoaringTreemap::deserialize_from(b) {
                Ok(rb) => {
                    let raw = rb.into_iter().collect::<Vec<_>>();
                    let s = join(raw.iter(), ",");
                    builder.put_str(&s);
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }

            builder.commit_row();
        }),
    );
}
