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

use std::ops::Add;
use std::ops::Mul;
use std::ops::Sub;

use z3::ast::Ast;
use z3::ast::Bool;
use z3::ast::Datatype;
use z3::ast::Dynamic;
use z3::ast::Int;
use z3::Context;
use z3::DatatypeAccessor;
use z3::DatatypeBuilder;
use z3::DatatypeSort;
use z3::Sort;

/// Get the Z3 sort of Nullable Boolean type.
pub fn nullable_bool_sort(ctx: &Context) -> DatatypeSort {
    DatatypeBuilder::new(ctx, "Nullable(Boolean)")
        .variant("TRUE_BOOL", vec![])
        .variant("FALSE_BOOL", vec![])
        .variant("NULL_BOOL", vec![])
        .finish()
}

/// Construct a TRUE value of Nullable Boolean type.
pub fn true_bool(ctx: &Context) -> Dynamic {
    nullable_bool_sort(ctx).variants[0].constructor.apply(&[])
}

/// Construct a FALSE value of Nullable Boolean type.
pub fn false_bool(ctx: &Context) -> Dynamic {
    nullable_bool_sort(ctx).variants[1].constructor.apply(&[])
}

/// Construct a NULL value of Nullable Boolean type.
pub fn null_bool(ctx: &Context) -> Dynamic {
    nullable_bool_sort(ctx).variants[2].constructor.apply(&[])
}

/// Check if a Nullable Boolean value is true.
pub fn is_true<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>) -> Bool<'ctx> {
    debug_assert!(a.get_sort() == nullable_bool_sort(ctx).sort);
    a._eq(&true_bool(ctx))
}

/// Check if a Nullable Boolean value is false.
pub fn is_false<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>) -> Bool<'ctx> {
    debug_assert!(a.get_sort() == nullable_bool_sort(ctx).sort);
    a._eq(&false_bool(ctx))
}

/// Construct a Nullable Boolean value from a bool constant.
pub fn const_bool(ctx: &Context, a: bool) -> Dynamic<'_> {
    if a { true_bool(ctx) } else { false_bool(ctx) }
}

/// Construct a variable of Nullable Boolean type.
pub fn var_bool<'ctx>(ctx: &'ctx Context, name: &str) -> Dynamic<'ctx> {
    Dynamic::from_ast(&Datatype::new_const(
        ctx,
        name,
        &nullable_bool_sort(ctx).sort,
    ))
}

/// Construct a Nullable Boolean value from a Boolean value.
pub fn from_bool<'ctx>(ctx: &'ctx Context, a: &Bool<'ctx>) -> Dynamic<'ctx> {
    a.ite(&true_bool(ctx), &false_bool(ctx))
}

pub fn nullable_int_sort(ctx: &Context) -> DatatypeSort {
    DatatypeBuilder::new(ctx, "Nullable(Int)")
        .variant("NULL_INT", vec![])
        .variant("JUST_INT", vec![(
            "unwrap-int",
            DatatypeAccessor::Sort(Sort::int(ctx)),
        )])
        .finish()
}

/// Construct a NULL value of Nullable(Int) type.
pub fn null_int(ctx: &Context) -> Dynamic<'_> {
    nullable_int_sort(ctx).variants[0].constructor.apply(&[])
}

/// Construct a const Nullable(Int) from an i64.
pub fn const_int(ctx: &Context, a: i64) -> Dynamic<'_> {
    from_int(ctx, &Int::from_i64(ctx, a))
}

/// Construct a variable of Nullable(Int) type.
pub fn var_int<'ctx>(ctx: &'ctx Context, name: &str) -> Dynamic<'ctx> {
    Dynamic::from_ast(&Datatype::new_const(
        ctx,
        name,
        &nullable_int_sort(ctx).sort,
    ))
}

/// Construct a const value of Int type.
pub fn from_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>) -> Dynamic<'ctx> {
    nullable_int_sort(ctx).variants[1].constructor.apply(&[a])
}

/// Equality check for Nullable(Int) type.
/// The table below shows the result of `eq_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | TRUE |
/// | 1 | 2 | FALSE |
pub fn eq_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_int_sort(ctx).sort);

    // IF(a IS NULL OR b IS NULL, NULL, a = b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)])
        .ite(&null_bool(ctx), &from_bool(ctx, &a._eq(b)))
}

// /// Inequality check for Nullable(Int) type.
// /// The table below shows the result of `ne_int`:
// ///
// /// | a | b | result |
// /// |---|---|--------|
// /// | NULL | NULL | NULL |
// /// | NULL | 1 | NULL |
// /// | 1 | NULL | NULL |
// /// | 1 | 1 | FALSE |
// /// | 1 | 2 | TRUE |
// pub fn ne_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
//     debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);
//     debug_assert!(b.get_sort() == nullable_int_sort(ctx).sort);

//     // IF(a IS NULL OR b IS NULL, NULL, a != b)
//     Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
//         &null_bool(ctx),
//         &a._eq(b).ite(&false_bool(ctx), &true_bool(ctx)),
//     )
// }

/// Greater than check for Nullable(Int) type.
/// The table below shows the result of `gt_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | FALSE |
/// | 1 | 2 | FALSE |
/// | 2 | 1 | TRUE |
pub fn gt_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_int_sort(ctx).sort);

    // IF(a IS NULL OR b IS NULL, NULL, a > b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_bool(ctx),
        &from_bool(ctx, &unwrap_int(ctx, a).gt(&unwrap_int(ctx, b))),
    )
}

/// Less than check for Nullable(Int) type.
/// The table below shows the result of `lt_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | FALSE |
/// | 1 | 2 | TRUE |
/// | 2 | 1 | FALSE |
pub fn lt_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_int_sort(ctx).sort);

    // IF(a IS NULL OR b IS NULL, NULL, a < b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_bool(ctx),
        &from_bool(ctx, &unwrap_int(ctx, a).lt(&unwrap_int(ctx, b))),
    )
}

/// Greater than or equal check for Nullable(Int) type.
/// The table below shows the result of `ge_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | TRUE |
/// | 1 | 2 | FALSE |
/// | 2 | 1 | TRUE |
pub fn ge_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_int_sort(ctx).sort);

    // IF(a IS NULL OR b IS NULL, NULL, a >= b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_bool(ctx),
        &from_bool(ctx, &unwrap_int(ctx, a).ge(&unwrap_int(ctx, b))),
    )
}

/// Less than or equal check for Nullable(Int) type.
/// The table below shows the result of `le_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | TRUE |
/// | 1 | 2 | TRUE |
/// | 2 | 1 | FALSE |
pub fn le_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_int_sort(ctx).sort);

    // IF(a IS NULL OR b IS NULL, NULL, a <= b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_bool(ctx),
        &from_bool(ctx, &unwrap_int(ctx, a).le(&unwrap_int(ctx, b))),
    )
}

/// Plus operation for Nullable(Int) type.
/// The table below shows the result of `plus_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | 2 |
pub fn plus_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_int_sort(ctx).sort);

    // IF(a IS NULL OR b IS NULL, NULL, a + b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_int(ctx),
        &from_int(ctx, &unwrap_int(ctx, a).add(unwrap_int(ctx, b))),
    )
}

/// Minus operation for Nullable(Int) type.
/// The table below shows the result of `minus_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | 0 |
pub fn minus_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_int_sort(ctx).sort);

    // IF(a IS NULL OR b IS NULL, NULL, a - b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_int(ctx),
        &from_int(ctx, &unwrap_int(ctx, a).sub(unwrap_int(ctx, b))),
    )
}

/// Multiply operation for Nullable(Int) type.
/// The table below shows the result of `multiply_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 2 | 2 | 4 |
pub fn multiply_int<'ctx>(
    ctx: &'ctx Context,
    a: &Dynamic<'ctx>,
    b: &Dynamic<'ctx>,
) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_int_sort(ctx).sort);

    // IF(a IS NULL OR b IS NULL, NULL, a * b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_int(ctx),
        &from_int(ctx, &unwrap_int(ctx, a).mul(unwrap_int(ctx, b))),
    )
}

/// Minus operation for Nullable(Int) type.
/// The table below shows the result of `minus_int`:
///
/// | a | result |
/// |---|--------|
/// | NULL | NULL |
/// | 1 | -1 |
pub fn unary_minus_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);

    // IF(a IS NULL, NULL, -a)
    is_null_int(ctx, a).ite(
        &null_int(ctx),
        &from_int(ctx, &unwrap_int(ctx, a).unary_minus()),
    )
}

/// Logical AND operation for Nullable Boolean type.
/// The table below shows the result of `and_bool`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | TRUE | NULL |
/// | NULL | FALSE | FALSE |
/// | TRUE | NULL | NULL |
/// | TRUE | TRUE | TRUE |
/// | TRUE | FALSE | FALSE |
/// | FALSE | NULL | FALSE |
/// | FALSE | TRUE | FALSE |
/// | FALSE | FALSE | FALSE |
pub fn and_bool<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_bool_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_bool_sort(ctx).sort);

    // IF(is_false(a) OR is_false(b), FALSE, IF(a IS NULL OR b IS NULL, NULL, TRUE))
    Bool::or(ctx, &[&is_false(ctx, a), &is_false(ctx, b)]).ite(
        &false_bool(ctx),
        &Bool::or(ctx, &[&is_null_bool(ctx, a), &is_null_bool(ctx, b)])
            .ite(&null_bool(ctx), &true_bool(ctx)),
    )
}

/// Logical OR operation for Nullable Boolean type.
/// The table below shows the result of `or_bool`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | TRUE | TRUE |
/// | NULL | FALSE | NULL |
/// | TRUE | NULL | TRUE |
/// | TRUE | TRUE | TRUE |
/// | TRUE | FALSE | TRUE |
/// | FALSE | NULL | NULL |
/// | FALSE | TRUE | TRUE |
/// | FALSE | FALSE | FALSE |
pub fn or_bool<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_bool_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_bool_sort(ctx).sort);

    // IF(is_true(a) OR is_true(b), TRUE, IF(a IS NULL OR b IS NULL, NULL, FALSE))
    Bool::or(ctx, &[&is_true(ctx, a), &is_true(ctx, b)]).ite(
        &true_bool(ctx),
        &Bool::or(ctx, &[&is_null_bool(ctx, a), &is_null_bool(ctx, b)])
            .ite(&null_bool(ctx), &false_bool(ctx)),
    )
}

/// Equality check for Nullable Boolean type.
/// The table below shows the result of `eq_bool`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | TRUE | NULL |
/// | NULL | FALSE | NULL |
/// | TRUE | NULL | NULL |
/// | TRUE | TRUE | TRUE |
/// | TRUE | FALSE | FALSE |
/// | FALSE | NULL | NULL |
/// | FALSE | TRUE | FALSE |
/// | FALSE | FALSE | TRUE |
pub fn eq_bool<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>, b: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_bool_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_bool_sort(ctx).sort);

    // IF(a IS NULL OR b IS NULL, NULL, a = b)
    Bool::or(ctx, &[&is_null_bool(ctx, a), &is_null_bool(ctx, b)])
        .ite(&null_bool(ctx), &from_bool(ctx, &a._eq(b)))
}

/// Logical NOT operation for Nullable Boolean type.
/// The table below shows the result of `not_bool`:
///
/// | a | result |
/// |---|--------|
/// | NULL | NULL |
/// | TRUE | FALSE |
/// | FALSE | TRUE |
pub fn not_bool<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_bool_sort(ctx).sort);

    // IF(a IS NULL, NULL, NOT a)
    is_null_bool(ctx, a).ite(
        &null_bool(ctx),
        &is_true(ctx, a).ite(&false_bool(ctx), &true_bool(ctx)),
    )
}

pub fn is_null_bool<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>) -> Bool<'ctx> {
    debug_assert!(a.get_sort() == nullable_bool_sort(ctx).sort);

    a._eq(&null_bool(ctx))
}

pub fn is_null_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>) -> Bool<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);

    a._eq(&null_int(ctx))
}

pub fn unwrap_int<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>) -> Int<'ctx> {
    debug_assert!(a.get_sort() == nullable_int_sort(ctx).sort);

    nullable_int_sort(ctx).variants[1].accessors[0]
        .apply(&[a])
        .as_int()
        .unwrap()
}
