// Copyright 2026 Datafuse Labs.
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

use databend_common_base::runtime::catch_unwind;
use databend_common_column::bitmap::Bitmap;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::EvalContext;
use databend_common_expression::FromData;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionSignature;
use databend_common_expression::PassthroughNullable;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarFunction;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::combine_nullable_1_arg;
use databend_common_expression::combine_nullable_2_arg;
use databend_common_expression::converts::meta::LegacyColumn;
use databend_common_expression::converts::meta::LegacyNullableColumn;
use databend_common_expression::domain_evaluator;
use databend_common_expression::error_to_null;
use databend_common_expression::passthrough_nullable_1_arg;
use databend_common_expression::passthrough_nullable_2_arg;
use databend_common_expression::passthrough_nullable_3_arg;
use databend_common_expression::passthrough_nullable_4_arg;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::NullType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::number::Int64Type;

fn context<'a>(
    func_ctx: &'a FunctionContext,
    rows: usize,
    validity: Option<Bitmap>,
) -> EvalContext<'a> {
    EvalContext {
        generics: &[],
        num_rows: rows,
        func_ctx,
        validity,
        errors: None,
        suppress_error: false,
        strict_eval: false,
    }
}

fn check_typed_result(
    ctx: &mut EvalContext,
    expected: &Bitmap,
    eval: impl FnOnce(&mut EvalContext) -> Value<NullableType<Int64Type>>,
) {
    let value = eval(ctx);
    assert_eq!(ctx.validity.as_ref(), Some(expected));
    let actual = match value {
        Value::Scalar(scalar) => {
            // Only a uniform row mask can be represented by a scalar result.
            assert!(expected.null_count() == 0 || expected.true_count() == 0);
            vec![scalar; expected.len()]
        }
        Value::Column(column) => column.iter().collect::<Vec<_>>(),
    };
    assert_eq!(
        actual,
        expected
            .iter()
            .map(|valid| valid.then_some(7))
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_typed_nullable_scalar_results_preserve_row_masks() {
    let func_ctx = FunctionContext::default();
    for mask in [
        vec![],
        vec![true, true],
        vec![false, false],
        vec![true, false],
        vec![false, true],
    ] {
        let validity = Bitmap::from_iter(mask);
        let rows = validity.len();
        let input = Value::Column(NullableColumn::<Int64Type>::new(
            vec![1; rows].into(),
            validity.clone(),
        ));
        for outer in [
            None,
            Some(Bitmap::from_iter((0..rows).map(|row| row % 2 == 0))),
        ] {
            let expected = outer
                .as_ref()
                .map_or_else(|| validity.clone(), |outer| outer & &validity);
            let mut ctx = context(&func_ctx, rows, outer.clone());
            check_typed_result(&mut ctx, &expected, |ctx| {
                passthrough_nullable_1_arg::<Int64Type, Int64Type>(|_, _| Value::Scalar(7))(
                    input.clone(),
                    ctx,
                )
            });
            let mut ctx = context(&func_ctx, rows, outer.clone());
            check_typed_result(&mut ctx, &expected, |ctx| {
                passthrough_nullable_2_arg::<Int64Type, Int64Type, Int64Type>(|_, _, _| {
                    Value::Scalar(7)
                })(Value::Scalar(Some(1)), input.clone(), ctx)
            });
            let mut ctx = context(&func_ctx, rows, outer.clone());
            check_typed_result(&mut ctx, &expected, |ctx| {
                passthrough_nullable_3_arg::<Int64Type, Int64Type, Int64Type, Int64Type>(
                    |_, _, _, _| Value::Scalar(7),
                )(
                    Value::Scalar(Some(1)),
                    input.clone(),
                    Value::Scalar(Some(1)),
                    ctx,
                )
            });
            let mut ctx = context(&func_ctx, rows, outer.clone());
            check_typed_result(&mut ctx, &expected, |ctx| {
                passthrough_nullable_4_arg::<Int64Type, Int64Type, Int64Type, Int64Type, Int64Type>(
                    |_, _, _, _, _| Value::Scalar(7),
                )(
                    Value::Scalar(Some(1)),
                    Value::Scalar(Some(1)),
                    input.clone(),
                    Value::Scalar(Some(1)),
                    ctx,
                )
            });
            let mut ctx = context(&func_ctx, rows, outer.clone());
            check_typed_result(&mut ctx, &expected, |ctx| {
                combine_nullable_1_arg::<Int64Type, Int64Type>(|_, _| Value::Scalar(Some(7)))(
                    input.clone(),
                    ctx,
                )
            });
            let mut ctx = context(&func_ctx, rows, outer);
            check_typed_result(&mut ctx, &expected, |ctx| {
                combine_nullable_2_arg::<Int64Type, Int64Type, Int64Type>(|_, _, _| {
                    Value::Scalar(Some(7))
                })(input.clone(), Value::Scalar(Some(1)), ctx)
            });
        }
    }
}

#[test]
fn test_combine_nullable_intersects_output_validity() {
    let func_ctx = FunctionContext::default();
    let mut ctx = context(&func_ctx, 3, None);
    let input = Value::Column(NullableColumn::<Int64Type>::new(
        vec![1; 3].into(),
        Bitmap::from_iter([true, false, true]),
    ));
    let result = combine_nullable_1_arg::<Int64Type, Int64Type>(|_, _| {
        Value::Column(NullableColumn::new(
            vec![7; 3].into(),
            Bitmap::from_iter([false, true, true]),
        ))
    })(input, &mut ctx);
    let Value::Column(column) = result else {
        panic!("expected column")
    };
    assert_eq!(column.iter().collect::<Vec<_>>(), vec![None, None, Some(7)]);
}

#[test]
fn test_dynamic_nullable_propagates_context_and_scalar_masks() {
    let func_ctx = FunctionContext::default();
    for input_mask in [
        vec![],
        vec![true, true],
        vec![false, false],
        vec![true, false],
        vec![false, true],
    ] {
        let input_mask = Bitmap::from_iter(input_mask);
        let rows = input_mask.len();
        for outer in [
            None,
            Some(Bitmap::from_iter((0..rows).map(|row| row % 2 == 0))),
        ] {
            let expected = outer
                .as_ref()
                .map_or_else(|| input_mask.clone(), |outer| outer & &input_mask);
            let observed = expected.clone();
            let func = PassthroughNullable(move |_: &[Value<AnyType>], ctx: &mut EvalContext| {
                assert_eq!(ctx.validity.as_ref(), Some(&observed));
                Value::Scalar(Scalar::Number(7_i64.into()))
            });
            let input = Value::Column(
                Int64Type::from_data(vec![1; rows]).wrap_nullable(Some(input_mask.clone())),
            );
            let mut ctx = context(&func_ctx, rows, outer);
            let result = func.eval(&[input], &mut ctx);
            let expected = Value::Column(Int64Type::from_opt_data(
                expected
                    .iter()
                    .map(|valid| valid.then_some(7))
                    .collect::<Vec<_>>(),
            ));
            assert_eq!(
                result.into_full_column(&Int64Type::data_type().wrap_nullable(), rows),
                expected.into_full_column(&Int64Type::data_type().wrap_nullable(), rows)
            );
        }
    }
}

#[test]
fn test_dynamic_nullable_all_scalar_inputs() {
    let func_ctx = FunctionContext::default();
    for validity in [
        Bitmap::new(),
        Bitmap::new_constant(true, 2),
        Bitmap::new_constant(false, 2),
        Bitmap::from_iter([false, true]),
    ] {
        let expected_mask = validity.clone();
        let func = PassthroughNullable(move |_: &[Value<AnyType>], ctx: &mut EvalContext| {
            assert_eq!(ctx.validity.as_ref(), Some(&expected_mask));
            Value::Scalar(Scalar::Number(7_i64.into()))
        });
        let rows = validity.len();
        let mut ctx = context(&func_ctx, rows, Some(validity.clone()));
        let result = func.eval(&[Value::Scalar(Scalar::Number(1_i64.into()))], &mut ctx);
        let expected = Int64Type::from_opt_data(
            validity
                .iter()
                .map(|valid| valid.then_some(7))
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            result.into_full_column(&Int64Type::data_type().wrap_nullable(), rows),
            expected
        );
        assert_eq!(
            Value::Scalar(Scalar::Null).wrap_nullable(Some(validity)),
            Value::Scalar(Scalar::Null)
        );
    }
}

#[test]
fn test_dynamic_nullable_suppresses_errors_only_on_inactive_rows() {
    let func_ctx = FunctionContext::default();
    let func = PassthroughNullable(|_: &[Value<AnyType>], ctx: &mut EvalContext| {
        ctx.set_error(1, "inactive row");
        Value::Scalar(Scalar::Number(7_i64.into()))
    });
    let input = Value::Column(
        Int64Type::from_data(vec![1, 1]).wrap_nullable(Some(Bitmap::from_iter([true, false]))),
    );
    let mut ctx = context(&func_ctx, 2, None);
    let _ = func.eval(&[input], &mut ctx);
    assert!(ctx.errors.is_none());

    let func = PassthroughNullable(|_: &[Value<AnyType>], ctx: &mut EvalContext| {
        ctx.set_error(0, "active row");
        Value::Scalar(Scalar::Number(7_i64.into()))
    });
    let input = Value::Column(
        Int64Type::from_data(vec![1, 1]).wrap_nullable(Some(Bitmap::from_iter([true, false]))),
    );
    let mut ctx = context(&func_ctx, 2, None);
    let _ = func.eval(&[input], &mut ctx);
    assert!(ctx.errors.is_some());
}

#[test]
fn test_error_to_null_preserves_scalar_error_mask() {
    let func_ctx = FunctionContext::default();
    for error_row in [0, 1] {
        let mut ctx = context(&func_ctx, 2, None);
        let result = error_to_null::<Int64Type, Int64Type>(|_, ctx| {
            ctx.set_error(error_row, "bad row");
            Value::Scalar(7)
        })(Value::Scalar(1), &mut ctx);
        let Value::Column(column) = result else {
            panic!("expected column")
        };
        assert_eq!(
            column.iter().collect::<Vec<_>>(),
            (0..2)
                .map(|row| (row != error_row).then_some(7))
                .collect::<Vec<_>>()
        );
        assert!(ctx.errors.is_none());
        let result = Value::Scalar(Scalar::Number(7_i64.into()))
            .wrap_nullable(Some(Bitmap::from_iter((0..2).map(|row| row != error_row))));
        let expected = Int64Type::from_opt_data(
            (0..2)
                .map(|row| (row != error_row).then_some(7))
                .collect::<Vec<_>>(),
        );
        assert_eq!(result.into_column().unwrap(), expected);
    }
}

#[test]
fn test_dynamic_try_wrapper_preserves_scalar_error_mask() {
    let func_ctx = FunctionContext::default();
    let ty = Int64Type::data_type();
    let func = Function {
        signature: FunctionSignature {
            name: "test_scalar_try".to_string(),
            args_type: vec![ty.clone()],
            return_type: ty,
        },
        eval: FunctionEval::Scalar {
            calc_domain: domain_evaluator(|_, _| FunctionDomain::MayThrow),
            eval: scalar_evaluator(|_, ctx| {
                ctx.set_error(1, "bad row");
                Value::Scalar(Scalar::Number(7_i64.into()))
            }),
            derive_stat: None,
        },
    }
    .error_to_null();
    let mut ctx = context(&func_ctx, 2, None);
    let (_, _, eval) = func.eval.into_scalar().unwrap();
    let result = eval.eval(&[Value::Scalar(Scalar::Number(1_i64.into()))], &mut ctx);
    assert_eq!(
        result.into_column().unwrap(),
        Int64Type::from_opt_data(vec![Some(7), None])
    );
    assert!(ctx.errors.is_none());
}

#[test]
fn test_nullable_constructors_reject_invalid_columns() {
    let column = Int64Type::from_data(vec![1, 2]);
    assert!(
        catch_unwind(|| NullableColumn::<AnyType>::new(
            column.clone(),
            Bitmap::new_constant(true, 1)
        ))
        .is_err()
    );
    assert!(
        catch_unwind(|| NullableColumn::<Int64Type>::new(
            vec![1, 2].into(),
            Bitmap::new_constant(true, 1)
        ))
        .is_err()
    );
    for column in [column.wrap_nullable(None), Column::Null { len: 2 }] {
        assert!(
            catch_unwind(|| NullableColumn::<AnyType>::new(
                column.clone(),
                Bitmap::new_constant(true, 2)
            ))
            .is_err()
        );
        assert!(
            catch_unwind(|| NullableColumn::<GenericType<0>>::new(
                column.clone(),
                Bitmap::new_constant(true, 2)
            ))
            .is_err()
        );
    }
    assert!(
        catch_unwind(|| NullableColumn::<NullType>::new(2, Bitmap::new_constant(true, 2))).is_err()
    );
    let inner = NullableColumn::<Int64Type>::new(vec![1, 2].into(), Bitmap::new_constant(true, 2));
    assert!(
        catch_unwind(|| NullableColumn::<NullableType<Int64Type>>::new(
            inner,
            Bitmap::new_constant(true, 2)
        ))
        .is_err()
    );
}

#[test]
fn test_legacy_nullable_conversion_merges_layers() {
    let column = Int64Type::from_data(vec![1, 2, 3]);
    let legacy = LegacyColumn::Nullable(Box::new(LegacyNullableColumn {
        column: LegacyColumn::Nullable(Box::new(LegacyNullableColumn {
            column: column.clone().into(),
            validity: Bitmap::from_iter([true, false, true]),
        })),
        validity: Bitmap::from_iter([false, true, true]),
    }));
    assert_eq!(
        Column::from(legacy),
        column.wrap_nullable(Some(Bitmap::from_iter([false, false, true])))
    );
    let legacy = LegacyColumn::Nullable(Box::new(LegacyNullableColumn {
        column: LegacyColumn::Null { len: 2 },
        validity: Bitmap::from_iter([true, false]),
    }));
    assert_eq!(Column::from(legacy), Column::Null { len: 2 });
}

#[test]
fn test_nullable_builders_reject_invalid_logical_types() {
    for inner in [
        DataType::Null,
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
    ] {
        let ty = DataType::Nullable(Box::new(inner));
        assert!(catch_unwind(|| ColumnBuilder::with_capacity(&ty, 2)).is_err());
        assert!(catch_unwind(|| ColumnBuilder::repeat_default(&ty, 2)).is_err());
        assert!(catch_unwind(|| ColumnBuilder::repeat(&ScalarRef::Null, 2, &ty)).is_err());
        let array = Int64Type::from_data(vec![1_i64, 2]).into_arrow_rs();
        assert!(Column::from_arrow_rs(array, &ty).is_err());
    }
}
