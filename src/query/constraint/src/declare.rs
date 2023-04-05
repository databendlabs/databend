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

use std::ops::Add;
use std::ops::Sub;

use z3::ast::Ast;
use z3::ast::Bool;
use z3::ast::Dynamic;
use z3::ast::Int;
use z3::Context;
use z3::DatatypeBuilder;
use z3::DatatypeSort;

/// Get the Z3 sort of Nullable Boolean type.
pub fn nullable_bool_sort(ctx: &Context) -> DatatypeSort {
    DatatypeBuilder::new(ctx, "Nullable(Boolean)")
        .variant("TRUE", vec![])
        .variant("FALSE", vec![])
        .variant("NULL", vec![])
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

/// Check if a Nullable Boolean value is true.
pub fn is_true<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>) -> Bool<'ctx> {
    debug_assert!(a.get_sort() == nullable_bool_sort(ctx).sort);
    a._eq(&true_bool(ctx))
}

/// Construct a NULL value of Nullable Boolean type.
pub fn null_bool(ctx: &Context) -> Dynamic {
    nullable_bool_sort(ctx).variants[2].constructor.apply(&[])
}

/// Construct a NULL value of Nullable Int type.
pub fn null_int(ctx: &Context) -> Int {
    Int::new_const(ctx, "null_int")
}

/// Equality check for Nullable Int type.
/// The table below shows the result of `eq_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | TRUE |
/// | 1 | 2 | FALSE |
pub fn eq_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>, b: &Int<'ctx>) -> Dynamic<'ctx> {
    // IF(a IS NULL OR b IS NULL, NULL, a = b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_bool(ctx),
        &a._eq(b).ite(&true_bool(ctx), &false_bool(ctx)),
    )
}

/// Inequality check for Nullable Int type.
/// The table below shows the result of `ne_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | FALSE |
/// | 1 | 2 | TRUE |
pub fn ne_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>, b: &Int<'ctx>) -> Dynamic<'ctx> {
    // IF(a IS NULL OR b IS NULL, NULL, a != b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_bool(ctx),
        &a._eq(b).ite(&false_bool(ctx), &true_bool(ctx)),
    )
}

/// Greater than check for Nullable Int type.
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
pub fn gt_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>, b: &Int<'ctx>) -> Dynamic<'ctx> {
    // IF(a IS NULL OR b IS NULL, NULL, a > b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_bool(ctx),
        &a.gt(b).ite(&true_bool(ctx), &false_bool(ctx)),
    )
}

/// Less than check for Nullable Int type.
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
pub fn lt_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>, b: &Int<'ctx>) -> Dynamic<'ctx> {
    // IF(a IS NULL OR b IS NULL, NULL, a < b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_bool(ctx),
        &a.lt(b).ite(&true_bool(ctx), &false_bool(ctx)),
    )
}

/// Greater than or equal check for Nullable Int type.
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
pub fn ge_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>, b: &Int<'ctx>) -> Dynamic<'ctx> {
    // IF(a IS NULL OR b IS NULL, NULL, a >= b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_bool(ctx),
        &a.ge(b).ite(&true_bool(ctx), &false_bool(ctx)),
    )
}

/// Less than or equal check for Nullable Int type.
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
pub fn le_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>, b: &Int<'ctx>) -> Dynamic<'ctx> {
    // IF(a IS NULL OR b IS NULL, NULL, a <= b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(
        &null_bool(ctx),
        &a.le(b).ite(&true_bool(ctx), &false_bool(ctx)),
    )
}

/// Plus operation for Nullable Int type.
/// The table below shows the result of `plus_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | 2 |
pub fn plus_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>, b: &Int<'ctx>) -> Int<'ctx> {
    // IF(a IS NULL OR b IS NULL, NULL, a + b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(&null_int(ctx), &a.add(b))
}

/// Minus operation for Nullable Int type.
/// The table below shows the result of `minus_int`:
///
/// | a | b | result |
/// |---|---|--------|
/// | NULL | NULL | NULL |
/// | NULL | 1 | NULL |
/// | 1 | NULL | NULL |
/// | 1 | 1 | 0 |
pub fn minus_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>, b: &Int<'ctx>) -> Int<'ctx> {
    // IF(a IS NULL OR b IS NULL, NULL, a - b)
    Bool::or(ctx, &[&is_null_int(ctx, a), &is_null_int(ctx, b)]).ite(&null_int(ctx), &a.sub(b))
}

/// Logical AND operation for Nullable Boolean type.
/// The table below shows the result of `and_nullable_bool`:
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
pub fn and_nullable_bool<'ctx>(
    ctx: &'ctx Context,
    a: &Dynamic<'ctx>,
    b: &Dynamic<'ctx>,
) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_bool_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_bool_sort(ctx).sort);

    // IF(is_true(a) AND is_true(b),
    //     TRUE,
    //     IF((is_true(a) AND b IS NULL) OR (a IS NULL AND is_true(b)) OR (a IS NULL AND b IS NULL),
    //         NULL,
    //         FALSE
    //     )
    // )
    Bool::and(ctx, &[&is_true(ctx, a), &is_true(ctx, b)]).ite(
        &true_bool(ctx),
        &Bool::or(ctx, &[
            &Bool::and(ctx, &[&is_true(ctx, a), &is_null_bool(ctx, b)]),
            &Bool::and(ctx, &[&is_null_bool(ctx, a), &is_true(ctx, b)]),
            &Bool::and(ctx, &[&is_null_bool(ctx, a), &is_null_bool(ctx, b)]),
        ])
        .ite(&null_bool(ctx), &false_bool(ctx)),
    )
}

/// Logical OR operation for Nullable Boolean type.
/// The table below shows the result of `or_nullable_bool`:
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
pub fn or_nullable_bool<'ctx>(
    ctx: &'ctx Context,
    a: &Dynamic<'ctx>,
    b: &Dynamic<'ctx>,
) -> Dynamic<'ctx> {
    debug_assert!(a.get_sort() == nullable_bool_sort(ctx).sort);
    debug_assert!(b.get_sort() == nullable_bool_sort(ctx).sort);

    // IF(is_true(a) OR is_true(b), TRUE, IF(a IS NULL OR b IS NULL, NULL, FALSE))
    Bool::or(ctx, &[&is_true(ctx, a), &is_true(ctx, b)]).ite(
        &true_bool(ctx),
        &Bool::or(ctx, &[&is_null_bool(ctx, a), &is_null_bool(ctx, b)])
            .ite(&null_bool(ctx), &false_bool(ctx)),
    )
}

/// Logical NOT operation for Nullable Boolean type.
/// The table below shows the result of `not_nullable_bool`:
///
/// | a | result |
/// |---|--------|
/// | NULL | NULL |
/// | TRUE | FALSE |
/// | FALSE | TRUE |
pub fn not_nullable_bool<'ctx>(ctx: &'ctx Context, a: &Dynamic<'ctx>) -> Dynamic<'ctx> {
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

pub fn is_null_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>) -> Bool<'ctx> {
    a._eq(&null_int(ctx))
}

pub fn is_not_null_int<'ctx>(ctx: &'ctx Context, a: &Int<'ctx>) -> Bool<'ctx> {
    a._eq(&null_int(ctx)).not()
}
