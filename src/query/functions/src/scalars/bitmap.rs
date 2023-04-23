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
use common_expression::types::bitmap::BitmapType;
use common_expression::types::bitmap::BitmapWrapper;
use common_expression::types::StringType;
use common_expression::types::UInt32Type;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::FunctionDomain;
use common_expression::FunctionRegistry;
use itertools::join;
use roaring::RoaringBitmap;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<UInt32Type, BitmapType, _, _>(
        "to_bitmap",
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt32Type, BitmapType>(|arg, builder, _ctx| {
            let mut rb = RoaringBitmap::new();
            rb.insert(arg);
            builder.push(BitmapWrapper { bitmap: rb })
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, BitmapType, _, _>(
        "bitmap_from_string",
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, BitmapType>(|s, builder, ctx| {
            match std::str::from_utf8(s)
                .map_err(|e| e.to_string())
                .and_then(|s| {
                    let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
                    let result: Result<Vec<u32>, String> = s
                        .split(',')
                        .map(|v| v.parse::<u32>().map_err(|e| e.to_string()))
                        .collect();
                    result
                }) {
                Ok(v) => {
                    let rb = RoaringBitmap::from_iter(v.iter());
                    builder.push(BitmapWrapper { bitmap: rb })
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                    builder.push(BitmapWrapper::default());
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BitmapType, StringType, _, _>(
        "bitmap_to_string",
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<BitmapType, StringType>(|b, builder, _ctx| {
            let raw = b.bitmap.clone().into_iter().collect::<Vec<_>>();
            let s = join(raw.iter(), ",");
            builder.put_str(&s);
            builder.commit_row();
        }),
    );
}
