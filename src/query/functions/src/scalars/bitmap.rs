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

use std::ops::BitAnd;
use std::ops::BitOr;
use std::ops::BitXor;
use std::ops::Sub;

use common_expression::types::bitmap::BitmapType;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::ArrayType;
use common_expression::types::BooleanType;
use common_expression::types::StringType;
use common_expression::types::UInt64Type;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::vectorize_with_builder_3_arg;
use common_expression::EvalContext;
use common_expression::FunctionDomain;
use common_expression::FunctionRegistry;
use croaring::treemap::NativeSerializer;
use croaring::Treemap;
use itertools::join;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, BitmapType, _, _>(
        "to_bitmap",
        |_, _| FunctionDomain::MayThrow,
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
                    let rb = Treemap::from_iter(v);
                    builder.put(&rb.serialize().unwrap());
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
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, BitmapType>(|arg, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            let mut rb = Treemap::create();
            rb.add(arg);

            builder.put(&rb.serialize().unwrap());
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<ArrayType<UInt64Type>, BitmapType, _, _>(
        "build_bitmap",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<ArrayType<UInt64Type>, BitmapType>(|arg, builder, _ctx| {
            let mut rb = Treemap::create();
            for a in arg.iter() {
                rb.add(*a);
            }

            builder.put(&rb.serialize().unwrap());
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BitmapType, UInt64Type, _, _>(
        "bitmap_count",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<BitmapType, UInt64Type>(|arg, builder, ctx| {
            match Treemap::deserialize(arg) {
                Ok(rb) => {
                    builder.push(rb.cardinality());
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
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BitmapType, StringType>(|b, builder, ctx| {
            match Treemap::deserialize(b) {
                Ok(rb) => {
                    let raw = rb.to_vec();
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

    registry.register_passthrough_nullable_2_arg::<BitmapType, UInt64Type, BooleanType, _, _>(
        "bitmap_contains",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, UInt64Type, BooleanType>(
            |b, item, builder, ctx| match Treemap::deserialize(b) {
                Ok(rb) => {
                    builder.push(rb.contains(item));
                }
                Err(e) => {
                    builder.push(false);
                    ctx.set_error(builder.len(), e.to_string());
                }
            },
        ),
    );

    registry.register_passthrough_nullable_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType, _, _>(
        "bitmap_subset_limit",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType>(
            |b, range_start, limit, builder, ctx| match Treemap::deserialize(b) {
                Ok(rb) => {
                    let collection = rb.iter().filter(|x| x >= &range_start).take(limit as usize);
                    let subset_bitmap = Treemap::from_iter(collection);
                    builder.put(&subset_bitmap.serialize().unwrap());
                    builder.commit_row();
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            },
        ),
    );

    registry.register_passthrough_nullable_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType, _, _>(
        "bitmap_subset_in_range",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType>(
            |b, start, end, builder, ctx| match Treemap::deserialize(b) {
                Ok(rb) => {
                    let collection = rb.iter().filter(|x| x >= &start && x < &end);
                    let subset_bitmap = Treemap::from_iter(collection);
                    builder.put(&subset_bitmap.serialize().unwrap());
                    builder.commit_row();
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            },
        ),
    );

    registry.register_passthrough_nullable_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType, _, _>(
        "sub_bitmap",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType>(
            |b, offset, length, builder, ctx| {
                match Treemap::deserialize(b) {
                    Ok(rb) => {
                        let subset_start = offset;
                        let subset_length = length;
                        if subset_start >= b.len() as u64 {
                            let rb = Treemap::create();
                            builder.put(&rb.serialize().unwrap());
                            builder.commit_row();
                        } else {
                            let adjusted_length = (subset_start + subset_length).min(b.len() as u64) - subset_start;
                            let subset_bitmap = &rb.to_vec()[subset_start as usize..(subset_start + adjusted_length) as usize];
                            let rb = Treemap::from_iter(subset_bitmap.to_vec());
                            builder.put(&rb.serialize().unwrap());
                            builder.commit_row();
                        }
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BooleanType, _, _>(
        "bitmap_has_all",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BooleanType>(
            |b, items, builder, ctx| {
                let rb = match Treemap::deserialize(b) {
                    Ok(rb) => rb,
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                        return;
                    }
                };
                let rb2 = match Treemap::deserialize(items) {
                    Ok(rb) => rb,
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                        return;
                    }
                };
                builder.push(rb2.is_subset(&rb));
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BooleanType, _, _>(
        "bitmap_has_any",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BooleanType>(
            |b, items, builder, ctx| {
                let rb = match Treemap::deserialize(b) {
                    Ok(rb) => rb,
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                        return;
                    }
                };
                let rb2 = match Treemap::deserialize(items) {
                    Ok(rb) => rb,
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                        return;
                    }
                };
                builder.push(rb.bitand(rb2).cardinality() != 0);
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<BitmapType, UInt64Type, _, _>(
        "bitmap_max",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BitmapType, UInt64Type>(|b, builder, ctx| {
            let val = match Treemap::deserialize(b) {
                Ok(rb) => match rb.maximum() {
                    Some(val) => val,
                    None => {
                        ctx.set_error(builder.len(), "The bitmap is empty");
                        0
                    }
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    0
                }
            };
            builder.push(val);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BitmapType, UInt64Type, _, _>(
        "bitmap_min",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BitmapType, UInt64Type>(|b, builder, ctx| {
            let val = match Treemap::deserialize(b) {
                Ok(rb) => match rb.minimum() {
                    Some(val) => val,
                    None => {
                        ctx.set_error(builder.len(), "The bitmap is empty");
                        0
                    }
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    0
                }
            };
            builder.push(val);
        }),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BitmapType, _, _>(
        "bitmap_or",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BitmapType>(
            |arg1, arg2, builder, ctx| bitmap_logic_operate(arg1, arg2, builder, ctx, LogicOp::Or),
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BitmapType, _, _>(
        "bitmap_and",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BitmapType>(
            |arg1, arg2, builder, ctx| bitmap_logic_operate(arg1, arg2, builder, ctx, LogicOp::And),
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BitmapType, _, _>(
        "bitmap_xor",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BitmapType>(
            |arg1, arg2, builder, ctx| bitmap_logic_operate(arg1, arg2, builder, ctx, LogicOp::Xor),
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BitmapType, _, _>(
        "bitmap_not",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BitmapType>(
            |arg1, arg2, builder, ctx| bitmap_logic_operate(arg1, arg2, builder, ctx, LogicOp::Not),
        ),
    );

    registry.register_aliases("bitmap_not", &["bitmap_and_not"]);
}

enum LogicOp {
    Or,
    And,
    Xor,
    Not,
}

/// perform a logical operation on two input bitmap, and write result bitmap to builder
fn bitmap_logic_operate(
    arg1: &[u8],
    arg2: &[u8],
    builder: &mut StringColumnBuilder,
    ctx: &mut EvalContext,
    op: LogicOp,
) {
    let Some(rb1) = Treemap::deserialize(arg1).map_err(|e| {
        ctx.set_error(builder.len(), e.to_string());
        builder.commit_row();
    }).ok() else {
        return;
    };

    let Some(rb2) = Treemap::deserialize(arg2).map_err(|e| {
        ctx.set_error(builder.len(), e.to_string());
        builder.commit_row();
    }).ok() else {
        return;
    };

    let rb = match op {
        LogicOp::Or => rb1.bitor(rb2),
        LogicOp::And => rb1.bitand(rb2),
        LogicOp::Xor => rb1.bitxor(rb2),
        LogicOp::Not => rb1.sub(rb2),
    };

    builder.put(&rb.serialize().unwrap());
    builder.commit_row();
}
