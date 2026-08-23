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

use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::types::ALL_SIGNED_INTEGER_TYPES;
use databend_common_expression::types::ALL_UNSIGNED_INTEGER_TYPES;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::bitmap::BitmapType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::with_signed_integer_mapped_type;
use databend_common_expression::with_unsigned_integer_mapped_type;
use databend_common_io::HybridBitmap;
use databend_common_io::bitmap::bitmap_contains;
use databend_common_io::bitmap::bitmap_has_all;
use databend_common_io::bitmap::bitmap_has_any;
use databend_common_io::bitmap::bitmap_len;
use databend_common_io::bitmap::bitmap_max;
use databend_common_io::bitmap::bitmap_min;
use databend_common_io::deserialize_bitmap;
use databend_common_io::parse_bitmap;
use itertools::join;

pub fn register(registry: &mut FunctionRegistry) {
    registry
        .scalar_builder("to_bitmap")
        .function()
        .typed_1_arg::<StringType, BitmapType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(vectorize_with_builder_1_arg::<StringType, BitmapType>(
            |s, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.commit_row();
                    return;
                }
                match parse_bitmap(s.as_bytes()) {
                    Ok(rb) => {
                        rb.serialize_into(&mut builder.data).unwrap();
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                builder.commit_row();
            },
        ))
        .register();

    registry
        .scalar_builder("to_bitmap")
        .function()
        .typed_1_arg::<UInt64Type, BitmapType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt64Type, BitmapType>(
            |arg, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.commit_row();
                    return;
                }
                let mut rb = HybridBitmap::new();
                rb.insert(arg);

                rb.serialize_into(&mut builder.data).unwrap();
                builder.commit_row();
            },
        ))
        .register();

    for num_type in ALL_UNSIGNED_INTEGER_TYPES {
        with_unsigned_integer_mapped_type!(|NUM_TYPE| match num_type {
            NumberDataType::NUM_TYPE => {
                registry
                    .scalar_builder("build_bitmap")
                    .function()
                    .typed_1_arg::<ArrayType<NullableType<NumberType<NUM_TYPE>>>, BitmapType>()
                    .passthrough_nullable()
                    .calc_domain(|_, _| FunctionDomain::Full)
                    .vectorized(vectorize_with_builder_1_arg::<
                        ArrayType<NullableType<NumberType<NUM_TYPE>>>,
                        BitmapType,
                    >(|arg, builder, ctx| {
                        if let Some(validity) = &ctx.validity {
                            if !validity.get_bit(builder.len()) {
                                builder.commit_row();
                                return;
                            }
                        }
                        let mut rb = HybridBitmap::new();
                        for a in arg.iter() {
                            if let Some(a) = a {
                                rb.insert(a.try_into().unwrap());
                            }
                        }

                        rb.serialize_into(&mut builder.data).unwrap();
                        builder.commit_row();
                    }))
                    .register();
            }
            _ => unreachable!(),
        })
    }

    for num_type in ALL_SIGNED_INTEGER_TYPES {
        with_signed_integer_mapped_type!(|NUM_TYPE| match num_type {
            NumberDataType::NUM_TYPE => {
                registry
                    .scalar_builder("build_bitmap")
                    .function()
                    .typed_1_arg::<ArrayType<NullableType<NumberType<NUM_TYPE>>>, BitmapType>()
                    .passthrough_nullable()
                    .calc_domain(|_, _| FunctionDomain::MayThrow)
                    .vectorized(vectorize_with_builder_1_arg::<
                        ArrayType<NullableType<NumberType<NUM_TYPE>>>,
                        BitmapType,
                    >(|arg, builder, ctx| {
                        if let Some(validity) = &ctx.validity {
                            if !validity.get_bit(builder.len()) {
                                builder.commit_row();
                                return;
                            }
                        }
                        let mut rb = HybridBitmap::new();
                        for a in arg.iter() {
                            if let Some(a) = a {
                                if a >= 0 {
                                    rb.insert(a.try_into().unwrap());
                                } else {
                                    ctx.set_error(
                                        builder.len(),
                                        "build_bitmap just support positive integer",
                                    );
                                }
                            }
                        }

                        rb.serialize_into(&mut builder.data).unwrap();
                        builder.commit_row();
                    }))
                    .register();
            }
            _ => unreachable!(),
        })
    }

    registry
        .scalar_builder("bitmap_count")
        .function()
        .typed_1_arg::<BitmapType, UInt64Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<BitmapType, UInt64Type>(
            |arg, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.push(0_u64);
                    return;
                }

                match bitmap_len(arg) {
                    Ok(n) => {
                        builder.push(n);
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(0_u64);
                    }
                }
            },
        ))
        .register();

    registry.register_aliases("bitmap_count", &["bitmap_cardinality"]);

    registry
        .scalar_builder("to_string")
        .function()
        .typed_1_arg::<BitmapType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(vectorize_with_builder_1_arg::<BitmapType, StringType>(
            |b, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.commit_row();
                    return;
                }
                match deserialize_bitmap(b) {
                    Ok(rb) => {
                        let raw = rb.into_iter().collect::<Vec<_>>();
                        let s = join(raw.iter(), ",");
                        builder.put_and_commit(s);
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.commit_row();
                    }
                }
            },
        ))
        .register();

    registry
        .scalar_builder("bitmap_to_array")
        .function()
        .typed_1_arg::<BitmapType, ArrayType<NumberType<u64>>>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(vectorize_with_builder_1_arg::<
            BitmapType,
            ArrayType<NumberType<u64>>,
        >(|b, builder, ctx| {
            if let Some(validity) = &ctx.validity
                && !validity.get_bit(builder.len())
            {
                builder.commit_row();
                return;
            }
            match deserialize_bitmap(b) {
                Ok(rb) => {
                    let ids = rb.into_iter().collect::<Vec<_>>();
                    builder.push(ids.into());
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.commit_row();
                }
            }
        }))
        .register();

    registry.register_passthrough_nullable_2_arg::<BitmapType, UInt64Type, BooleanType, _, _>(
        "bitmap_contains",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, UInt64Type, BooleanType>(
            |b, item, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.push(false);
                    return;
                }
                match bitmap_contains(b, item) {
                    Ok(found) => {
                        builder.push(found);
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType, _, _>(
        "bitmap_subset_limit",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType>(
            |b, range_start, limit, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                match deserialize_bitmap(b) {
                    Ok(rb) => {
                        let collection = rb.iter().filter(|x| x >= &range_start).take(limit as usize);
                        let subset_bitmap = HybridBitmap::from_iter(collection);
                        subset_bitmap.serialize_into(&mut builder.data).unwrap();
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                builder.commit_row();
            }
        ),
    );

    registry.register_passthrough_nullable_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType, _, _>(
        "bitmap_subset_in_range",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType>(
            |b, start, end, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                match deserialize_bitmap(b) {
                    Ok(rb) => {
                        let collection = rb.iter().filter(|x| x >= &start && x < &end);
                        let subset_bitmap = HybridBitmap::from_iter(collection);
                        subset_bitmap.serialize_into(&mut builder.data).unwrap();
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                builder.commit_row();
            }
        ),
    );

    registry.register_passthrough_nullable_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType, _, _>(
        "sub_bitmap",
        |_, _, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<BitmapType, UInt64Type, UInt64Type, BitmapType>(
            |b, offset, length, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                match deserialize_bitmap(b) {
                    Ok(rb) => {
                        let subset_start = offset;
                        let subset_length = length;
                        let bitmap_len = rb.len();
                        if subset_start >= bitmap_len {
                            let rb = HybridBitmap::new();
                            rb.serialize_into(&mut builder.data).unwrap();
                        } else {
                            let subset_end = subset_start.saturating_add(subset_length).min(bitmap_len);
                            let subset_bitmap = (0_u64..)
                                .zip(rb)
                                .skip_while(|(index, _)| *index < subset_start)
                                .take_while(|(index, _)| *index < subset_end)
                                .map(|(_, value)| value);
                            let rb = HybridBitmap::from_iter(subset_bitmap);
                            rb.serialize_into(&mut builder.data).unwrap();
                        }
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BooleanType, _, _>(
        "bitmap_has_all",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BooleanType>(
            |b, items, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.push(false);
                    return;
                }
                match bitmap_has_all(b, items) {
                    Ok(has_all) => {
                        builder.push(has_all);
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BooleanType, _, _>(
        "bitmap_has_any",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BooleanType>(
            |b, items, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.push(false);
                    return;
                }
                match bitmap_has_any(b, items) {
                    Ok(has_any) => {
                        builder.push(has_any);
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                    }
                }
            },
        ),
    );

    registry
        .scalar_builder("bitmap_max")
        .function()
        .typed_1_arg::<BitmapType, UInt64Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(vectorize_with_builder_1_arg::<BitmapType, UInt64Type>(
            |b, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.push(0);
                    return;
                }
                let val = match bitmap_max(b) {
                    Ok(Some(val)) => val,
                    Ok(None) => {
                        ctx.set_error(builder.len(), "The bitmap is empty");
                        0
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        0
                    }
                };
                builder.push(val);
            },
        ))
        .register();

    registry
        .scalar_builder("bitmap_min")
        .function()
        .typed_1_arg::<BitmapType, UInt64Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(vectorize_with_builder_1_arg::<BitmapType, UInt64Type>(
            |b, builder, ctx| {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(builder.len())
                {
                    builder.push(0);
                    return;
                }
                let val = match bitmap_min(b) {
                    Ok(Some(val)) => val,
                    Ok(None) => {
                        ctx.set_error(builder.len(), "The bitmap is empty");
                        0
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        0
                    }
                };
                builder.push(val);
            },
        ))
        .register();

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
    builder: &mut BinaryColumnBuilder,
    ctx: &mut EvalContext,
    op: LogicOp,
) {
    if let Some(validity) = &ctx.validity
        && !validity.get_bit(builder.len())
    {
        builder.commit_row();
        return;
    }
    let Some(rb1) = deserialize_bitmap(arg1)
        .map_err(|e| {
            ctx.set_error(builder.len(), e.to_string());
            builder.commit_row();
        })
        .ok()
    else {
        return;
    };

    let Some(rb2) = deserialize_bitmap(arg2)
        .map_err(|e| {
            ctx.set_error(builder.len(), e.to_string());
            builder.commit_row();
        })
        .ok()
    else {
        return;
    };

    let rb = match op {
        LogicOp::Or => rb1.bitor(rb2),
        LogicOp::And => rb1.bitand(rb2),
        LogicOp::Xor => rb1.bitxor(rb2),
        LogicOp::Not => rb1.sub(rb2),
    };

    rb.serialize_into(&mut builder.data).unwrap();
    builder.commit_row();
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::ops::RangeInclusive;

    use databend_common_expression::EvalContext;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::types::binary::BinaryColumnBuilder;
    use databend_common_io::HybridBitmap;

    use super::LogicOp;
    use super::bitmap_logic_operate;

    const BITMAP_SCALAR_OPS: [&str; 4] = ["bitmap_and", "bitmap_or", "bitmap_xor", "bitmap_not"];

    fn serialized_bitmap(values: &[u64]) -> Vec<u8> {
        let mut bitmap = HybridBitmap::new();
        for value in values {
            bitmap.insert(*value);
        }
        let mut data = Vec::new();
        bitmap.serialize_into(&mut data).unwrap();
        data
    }

    fn values_in_prefix(prefix: u64, range: RangeInclusive<u64>) -> Vec<u64> {
        range.map(|value| (prefix << 32) + value).collect()
    }

    struct BitmapOperand {
        name: &'static str,
        values: Vec<u64>,
    }

    fn bitmap_operands() -> Vec<BitmapOperand> {
        vec![
            BitmapOperand {
                name: "empty",
                values: vec![],
            },
            BitmapOperand {
                name: "small_32",
                values: values_in_prefix(0, 0..=31),
            },
            BitmapOperand {
                name: "multi_small_overlap",
                values: vec![31, (1u64 << 32) + 1],
            },
            BitmapOperand {
                name: "small_32_disjoint",
                values: values_in_prefix(0, 100..=131),
            },
            BitmapOperand {
                name: "large_33",
                values: values_in_prefix(0, 0..=32),
            },
            BitmapOperand {
                name: "large_array_4096",
                values: values_in_prefix(0, 0..=4095),
            },
            BitmapOperand {
                name: "large_bitmap_4097",
                values: values_in_prefix(0, 0..=4096),
            },
            BitmapOperand {
                name: "multi_large_mixed",
                values: values_in_prefix(0, 0..=4096)
                    .into_iter()
                    .chain(values_in_prefix(1, 0..=31))
                    .collect(),
            },
            BitmapOperand {
                name: "multi_large_mixed_disjoint",
                values: values_in_prefix(2, 0..=4096)
                    .into_iter()
                    .chain(values_in_prefix(3, 0..=31))
                    .collect(),
            },
        ]
    }

    fn eval_scalar_bitmap(name: &str, lhs: &[u8], rhs: &[u8]) -> Result<Vec<u64>, String> {
        let function_context = FunctionContext::default();
        let mut context = EvalContext {
            generics: &[],
            num_rows: 1,
            func_ctx: &function_context,
            validity: None,
            errors: None,
            suppress_error: false,
            strict_eval: false,
        };
        let mut builder = BinaryColumnBuilder::with_capacity(1, 0);
        let op = match name {
            "bitmap_and" => LogicOp::And,
            "bitmap_or" => LogicOp::Or,
            "bitmap_xor" => LogicOp::Xor,
            "bitmap_not" => LogicOp::Not,
            _ => unreachable!(),
        };
        bitmap_logic_operate(lhs, rhs, &mut builder, &mut context, op);
        if let Some((_, error)) = context.errors {
            return Err(error);
        }
        let bitmap = builder.build_scalar();
        Ok(databend_common_io::deserialize_bitmap(&bitmap)
            .unwrap()
            .into_iter()
            .collect())
    }

    #[test]
    fn test_fast_treemap_scalar_op_equals_set_op() {
        let operands = bitmap_operands();
        let pairs = operands
            .iter()
            .flat_map(|lhs| operands.iter().map(move |rhs| (lhs, rhs)))
            .collect::<Vec<_>>();
        for (lhs, rhs) in pairs {
            for name in BITMAP_SCALAR_OPS {
                let lhs_bitmap = serialized_bitmap(&lhs.values);
                let rhs_bitmap = serialized_bitmap(&rhs.values);
                let values =
                    eval_scalar_bitmap(name, &lhs_bitmap, &rhs_bitmap).unwrap_or_else(|error| {
                        panic!("{name} failed for case {}_{}: {error}", lhs.name, rhs.name)
                    });
                let lhs_set = lhs.values.iter().copied().collect::<BTreeSet<_>>();
                let rhs_set = rhs.values.iter().copied().collect::<BTreeSet<_>>();
                let expected: BTreeSet<_> = match name {
                    "bitmap_and" => lhs_set.intersection(&rhs_set).copied().collect(),
                    "bitmap_or" => lhs_set.union(&rhs_set).copied().collect(),
                    "bitmap_xor" => lhs_set.symmetric_difference(&rhs_set).copied().collect(),
                    "bitmap_not" => lhs_set.difference(&rhs_set).copied().collect(),
                    _ => unreachable!(),
                };
                let expected = expected.into_iter().collect::<Vec<_>>();
                assert_eq!(
                    values, expected,
                    "{name} failed for case {}_{}: lhs={:?}, rhs={:?}, expected={:?}, got={:?}",
                    lhs.name, rhs.name, lhs.values, rhs.values, expected, values
                );
            }
        }
    }

    #[test]
    fn test_fast_treemap_scalar_op_malformed_error_badbytes() {
        let valid = serialized_bitmap(
            &values_in_prefix(0, 0..=32)
                .into_iter()
                .chain(values_in_prefix(1, 0..=32))
                .collect::<Vec<_>>(),
        );

        for name in BITMAP_SCALAR_OPS {
            for (lhs, rhs) in [
                (&[0xff][..], valid.as_slice()),
                (valid.as_slice(), &[0xff][..]),
            ] {
                let error = eval_scalar_bitmap(name, lhs, rhs)
                    .expect_err("should fail for malformed bitmap bytes");
                assert!(error.contains("BadBytes"), "{name} raw error: {error}");
            }
        }
    }
}
