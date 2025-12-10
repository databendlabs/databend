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

use databend_common_expression::types::bitmap::BitmapColumnBuilder;
use databend_common_expression::types::bitmap::BitmapType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ALL_SIGNED_INTEGER_TYPES;
use databend_common_expression::types::ALL_UNSIGNED_INTEGER_TYPES;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::with_signed_integer_mapped_type;
use databend_common_expression::with_unsigned_integer_mapped_type;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_io::parse_bitmap;
use databend_common_io::HybridBitmap;
use itertools::join;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, BitmapType, _, _>(
        "to_bitmap",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, BitmapType>(|s, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push_default();
                    return;
                }
            }
            match parse_bitmap(s.as_bytes()) {
                Ok(rb) => builder.push(&rb),
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.push_default();
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, BitmapType, _, _>(
        "to_bitmap",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, BitmapType>(|arg, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push_default();
                    return;
                }
            }
            let mut rb = HybridBitmap::new();
            rb.insert(arg);

            builder.push(&rb);
        }),
    );

    for num_type in ALL_UNSIGNED_INTEGER_TYPES {
        with_unsigned_integer_mapped_type!(|NUM_TYPE| match num_type {
            NumberDataType::NUM_TYPE => {
                registry.register_passthrough_nullable_1_arg::<ArrayType<NullableType<NumberType<NUM_TYPE>>>, BitmapType, _, _>(
                    "build_bitmap",
                    |_, _| FunctionDomain::Full,
                    vectorize_with_builder_1_arg::<ArrayType<NullableType<NumberType<NUM_TYPE>>>, BitmapType>(|arg, builder, ctx| {
                        if let Some(validity) = &ctx.validity {
                            if !validity.get_bit(builder.len()) {
                                builder.push_default();
                                return;
                            }
                        }
                        let mut rb = HybridBitmap::new();
                        for a in arg.iter() {
                            if let Some(a) = a {
                                rb.insert(a.try_into().unwrap());
                            }
                        }

                        builder.push(&rb);
                    }),
                );
            }
            _ => unreachable!(),
        })
    }

    for num_type in ALL_SIGNED_INTEGER_TYPES {
        with_signed_integer_mapped_type!(|NUM_TYPE| match num_type {
            NumberDataType::NUM_TYPE => {
                registry.register_passthrough_nullable_1_arg::<ArrayType<NullableType<NumberType<NUM_TYPE>>>, BitmapType, _, _>(
                    "build_bitmap",
                    |_, _| FunctionDomain::MayThrow,
                    vectorize_with_builder_1_arg::<ArrayType<NullableType<NumberType<NUM_TYPE>>>, BitmapType>(|arg, builder, ctx| {
                        if let Some(validity) = &ctx.validity {
                            if !validity.get_bit(builder.len()) {
                                builder.push_default();
                                return;
                            }
                        }
                        let mut rb = HybridBitmap::new();
                        for a in arg.iter() {
                            if let Some(a) = a {
                                if a >= 0 {
                                    rb.insert(a.try_into().unwrap());
                                } else {
                                    ctx.set_error(builder.len(), "build_bitmap just support positive integer");
                                }
                            }
                        }

                        builder.push(&rb);
                    }),
                );
            }
            _ => unreachable!(),
        })
    }

    registry.register_passthrough_nullable_1_arg::<BitmapType, UInt64Type, _, _>(
        "bitmap_count",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<BitmapType, UInt64Type>(|arg, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(0_u64);
                    return;
                }
            }
            builder.push(arg.len());
        }),
    );

    registry.register_aliases("bitmap_count", &["bitmap_cardinality"]);

    registry.register_passthrough_nullable_1_arg::<BitmapType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BitmapType, StringType>(|b, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            let raw = b.iter().collect::<Vec<_>>();
            let s = join(raw.iter(), ",");
            builder.put_and_commit(s);
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BitmapType, ArrayType<NumberType<u64>>, _, _>(
        "bitmap_to_array",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BitmapType, ArrayType<NumberType<u64>>>(
            |b, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                let ids = b.iter().collect::<Vec<_>>();
                builder.push(ids.into());
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, UInt64Type, BooleanType, _, _>(
        "bitmap_contains",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, UInt64Type, BooleanType>(
            |b, item, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push(false);
                        return;
                    }
                }
                builder.push(b.contains(item));
            },
        ),
    );

    registry.register_passthrough_nullable_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType, _, _>(
        "bitmap_subset_limit",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType>(
            |b, range_start, limit, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_default();
                        return;
                    }
                }
                let collection = b.iter().filter(|x| x >= &range_start).take(limit as usize);
                let subset_bitmap = HybridBitmap::from_iter(collection);
                builder.push(&subset_bitmap);
            }
        ),
    );

    registry.register_passthrough_nullable_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType, _, _>(
        "bitmap_subset_in_range",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType>(
            |b, start, end, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_default();
                        return;
                    }
                }
                let collection = b.iter().filter(|x| x >= &start && x < &end);
                let subset_bitmap = HybridBitmap::from_iter(collection);
                builder.push(&subset_bitmap);
            }
        ),
    );

    registry.register_passthrough_nullable_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType, _, _>(
        "sub_bitmap",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType>(
            |b, offset, length, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_default();
                        return;
                    }
                }
                let values: Vec<_> = b.iter().collect();
                if offset >= values.len() as u64 {
                    builder.push(&HybridBitmap::new());
                    return;
                }
                let end = ((offset + length) as usize).min(values.len());
                let subset = HybridBitmap::from_iter(values[offset as usize..end].iter().copied());
                builder.push(&subset);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BooleanType, _, _>(
        "bitmap_has_all",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BooleanType>(
            |b, items, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push(false);
                        return;
                    }
                }
                builder.push(b.is_superset(items));
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BooleanType, _, _>(
        "bitmap_has_any",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BooleanType>(
            |b, items, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push(false);
                        return;
                    }
                }
                builder.push(b.intersection_len(items) != 0);
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<BitmapType, UInt64Type, _, _>(
        "bitmap_max",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BitmapType, UInt64Type>(|b, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(0);
                    return;
                }
            }
            let val = match b.max() {
                Some(val) => val,
                None => {
                    ctx.set_error(builder.len(), "The bitmap is empty");
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
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(0);
                    return;
                }
            }
            let val = match b.min() {
                Some(val) => val,
                None => {
                    ctx.set_error(builder.len(), "The bitmap is empty");
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
    arg1: &HybridBitmap,
    arg2: &HybridBitmap,
    builder: &mut BitmapColumnBuilder,
    ctx: &mut EvalContext,
    op: LogicOp,
) {
    if let Some(validity) = &ctx.validity {
        if !validity.get_bit(builder.len()) {
            builder.push_default();
            return;
        }
    }

    let rb = match op {
        LogicOp::Or => arg1.clone().bitor(arg2.clone()),
        LogicOp::And => arg1.clone().bitand(arg2.clone()),
        LogicOp::Xor => arg1.clone().bitxor(arg2.clone()),
        LogicOp::Not => arg1.clone().sub(arg2.clone()),
    };

    builder.push(&rb);
}
