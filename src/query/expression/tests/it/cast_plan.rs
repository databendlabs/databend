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

use std::cell::RefCell;
use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::EvalContext;
use databend_common_expression::EvaluateOptions;
use databend_common_expression::Evaluator;
use databend_common_expression::FromData;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionID;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::cast_plan::CastStep;
use databend_common_expression::cast_plan::casts_through;
use databend_common_expression::cast_plan::resolve_cast_plan;
use databend_common_expression::domain_evaluator;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::type_check;
use databend_common_expression::type_check::function_call_to_cast;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;

fn int32() -> DataType {
    DataType::Number(NumberDataType::Int32)
}

fn int64() -> DataType {
    DataType::Number(NumberDataType::Int64)
}

fn int16() -> DataType {
    DataType::Number(NumberDataType::Int16)
}

fn full(registry: &mut FunctionRegistry, src: DataType, dest: DataType) {
    register(registry, src, dest, "to_int64");
}

fn register(registry: &mut FunctionRegistry, src: DataType, dest: DataType, name: &str) {
    registry.register_cast_with(Some(src), dest, name);
}

fn function_id(step: &CastStep) -> FunctionID {
    match step {
        CastStep::Function { call, .. } => call.id.clone(),
        step => panic!("not a function step: {step:?}"),
    }
}

/// A cast runs the overload registered for it, and a nullable source runs the overload that
/// passes the NULLs through.
#[test]
fn test_cast_runs_registered_overloads() {
    let mut registry = FunctionRegistry::empty();
    registry.register_passthrough_nullable_1_arg::<Int32Type, Int64Type, _>(
        "to_int64",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<Int32Type, Int64Type>(|value, _| value as i64),
    );
    full(&mut registry, int32(), int64());

    let plan = resolve_cast_plan(&int32(), &int64(), &registry).unwrap();
    let id = function_id(&plan.step);
    let plan = resolve_cast_plan(
        &int32().wrap_nullable(),
        &int64().wrap_nullable(),
        &registry,
    )
    .unwrap();
    let nullable_id = function_id(&plan.step);
    assert_ne!(id, nullable_id);

    // Both overloads called by name are the cast.
    for (id, src, dest) in [
        (&id, int32(), int64()),
        (
            &nullable_id,
            int32().wrap_nullable(),
            int64().wrap_nullable(),
        ),
    ] {
        let cast = function_call_to_cast(id, &src, &dest, &registry).unwrap();
        assert_eq!(cast, dest);
    }
}

/// Without an overload for nullable values, a nullable source casts its values on their own.
#[test]
fn test_cast_without_nullable_overload() {
    let mut registry = FunctionRegistry::empty();
    registry.register_1_arg_core::<Int32Type, Int64Type, _, _>(
        "to_int64",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<Int32Type, Int64Type>(|value, _| value as i64),
    );
    full(&mut registry, int32(), int64());

    let plan = resolve_cast_plan(
        &int32().wrap_nullable(),
        &int64().wrap_nullable(),
        &registry,
    )
    .unwrap();
    let CastStep::Nullable(inner) = &plan.step else {
        panic!("not a nullable step: {plan}");
    };
    function_id(&inner.step);
}

/// A nullable source runs the nullable function of a cast factory only when that function passes
/// the NULLs through, not when it handles NULL itself.
#[test]
fn test_cast_runs_passthrough_factory() {
    fn to_int64(nullable: bool) -> Function {
        let wrap = |data_type: DataType| {
            if nullable {
                data_type.wrap_nullable()
            } else {
                data_type
            }
        };
        Function {
            signature: FunctionSignature {
                name: "to_int64".to_string(),
                args_type: vec![wrap(int32())],
                return_type: wrap(int64()),
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(|_, _| FunctionDomain::Full),
                eval: scalar_evaluator(|args, _| args[0].clone()),
                derive_stat: None,
            },
        }
    }
    let nullable_plan = |registry: &FunctionRegistry| {
        let src = int32().wrap_nullable();
        resolve_cast_plan(&src, &int64().wrap_nullable(), registry).unwrap()
    };

    let mut registry = FunctionRegistry::empty();
    registry.register_passthrough_nullable_factory("to_int64", |_, args_type: &[DataType]| {
        (args_type[0].remove_nullable() == int32()).then(|| to_int64(false))
    });
    full(&mut registry, int32(), int64());
    let plan = nullable_plan(&registry);
    assert_eq!(function_id(&plan.step).id(), 1);

    let mut registry = FunctionRegistry::empty();
    registry.register_nullable_variants(
        |registry| {
            registry.register_function_factory(
                "to_int64",
                FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
                    (args_type[0] == int32()).then(|| Arc::new(to_int64(false)))
                })),
            );
        },
        |registry| {
            registry.register_function_factory(
                "to_int64",
                FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
                    (args_type[0] == int32().wrap_nullable()).then(|| Arc::new(to_int64(true)))
                })),
            );
        },
    );
    full(&mut registry, int32(), int64());
    let plan = nullable_plan(&registry);
    let CastStep::Nullable(inner) = &plan.step else {
        panic!("not a nullable step: {plan}");
    };
    assert_eq!(function_id(&inner.step).id(), 0);
}

/// A cast is registered only with an overload that takes its source as it is, not one that the
/// source converts to.
#[test]
#[should_panic(expected = "needs one `to_int64` overload")]
fn test_cast_needs_exact_overload() {
    let mut registry = FunctionRegistry::empty();
    registry.register_passthrough_nullable_1_arg::<Int32Type, Int64Type, _>(
        "to_int64",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<Int32Type, Int64Type>(|value, _| value as i64),
    );
    full(
        &mut registry,
        DataType::Number(NumberDataType::Int16),
        int64(),
    );
}

thread_local! {
    /// The rows that the last run of `to_int16` converted.
    static CONVERTED: RefCell<Vec<usize>> = const { RefCell::new(vec![]) };
}

fn is_active(ctx: &EvalContext, row: usize) -> bool {
    ctx.validity
        .as_ref()
        .is_none_or(|validity| validity.get_bit(row))
}

/// Registers `to_int64(Int32)`, which fails on negative values, and `to_int16` from Int32 and
/// Int64, which records the rows it converts. With `chain`, Int32 casts to Int16 through Int64.
fn recording_registry(chain: bool) -> FunctionRegistry {
    let mut registry = FunctionRegistry::empty();
    registry.register_1_arg_core::<Int32Type, Int64Type, _, _>(
        "to_int64",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int32Type, Int64Type>(|value, output, ctx| {
            if value < 0 {
                ctx.set_error(output.len(), "negative");
            }
            output.push(value as i64);
        }),
    );
    registry.register_1_arg_core::<Int64Type, Int16Type, _, _>(
        "to_int16",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<Int64Type, Int16Type>(|value, output, ctx| {
            record(ctx, output.len());
            output.push(value as i16);
        }),
    );
    registry.register_1_arg_core::<Int32Type, Int16Type, _, _>(
        "to_int16",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<Int32Type, Int16Type>(|value, output, ctx| {
            record(ctx, output.len());
            output.push(value as i16);
        }),
    );
    register(&mut registry, int32(), int64(), "to_int64");
    register(&mut registry, int64(), int16(), "to_int16");
    if chain {
        registry.register_cast_chain(int32(), int64(), int16());
    } else {
        register(&mut registry, int32(), int16(), "to_int16");
    }
    registry
}

fn record(ctx: &EvalContext, row: usize) {
    if is_active(ctx, row) {
        CONVERTED.with(|rows| rows.borrow_mut().push(row));
    }
}

/// Runs `expr` on `column` for the rows in `validity`, and returns the result with the rows that
/// `to_int16` converted.
fn run(
    registry: &FunctionRegistry,
    is_try: bool,
    column: Column,
    dest_type: DataType,
    validity: Option<Bitmap>,
) -> (
    databend_common_exception::Result<Value<AnyType>>,
    Vec<usize>,
) {
    let expr = RawExpr::ColumnRef {
        span: None,
        id: 0,
        data_type: column.data_type(),
        display_name: "a".to_string(),
    };
    let block = DataBlock::new_from_columns(vec![column]);
    run_on_block(registry, is_try, expr, &block, dest_type, validity)
}

/// Runs the cast of `expr` on `block` for the rows in `validity`, and returns the result with
/// the rows that `to_int16` converted.
fn run_on_block(
    registry: &FunctionRegistry,
    is_try: bool,
    expr: RawExpr,
    block: &DataBlock,
    dest_type: DataType,
    validity: Option<Bitmap>,
) -> (
    databend_common_exception::Result<Value<AnyType>>,
    Vec<usize>,
) {
    let raw_expr = RawExpr::Cast {
        span: None,
        is_try,
        expr: Box::new(expr),
        dest_type,
    };
    let expr = type_check::check(&raw_expr, registry).unwrap();
    let func_ctx = FunctionContext::default();
    let evaluator = Evaluator::new(block, &func_ctx, registry);
    CONVERTED.with(|rows| rows.borrow_mut().clear());
    let result = evaluator.partial_run(&expr, validity, &mut EvaluateOptions::default(), false);
    (result, CONVERTED.with(|rows| rows.take()))
}

/// TRY_CAST does not convert the rows outside the validity of its caller.
#[test]
fn test_try_cast_skips_inactive_rows() {
    let registry = recording_registry(false);
    let validity = Bitmap::from_iter([true, false, true]);
    let column = Int32Type::from_data(vec![1, 2, 3]);
    let (result, converted) = run(&registry, true, column, int16(), Some(validity));
    result.unwrap();
    assert_eq!(converted, vec![0, 2]);
}

/// A later step of a chain does not convert the rows that failed in an earlier step.
#[test]
fn test_try_cast_chain_skips_failed_rows() {
    let registry = recording_registry(true);
    let column = Int32Type::from_data(vec![1, -1, 2]);
    let (result, converted) = run(&registry, true, column, int16(), None);
    assert_eq!(
        result.unwrap().into_column().unwrap(),
        Int16Type::from_opt_data(vec![Some(1), None, Some(2)])
    );
    assert_eq!(converted, vec![0, 2]);
}

/// A later step of a chain does not convert a scalar that failed in an earlier step, nor a
/// scalar that no active row uses.
#[test]
fn test_cast_chain_skips_unused_scalar() {
    let registry = recording_registry(true);
    let constant = |value: i32| RawExpr::Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::Int32(value)),
        data_type: None,
    };
    let block = DataBlock::new(vec![], 2);

    let (result, converted) = run_on_block(&registry, true, constant(-1), &block, int16(), None);
    assert_eq!(result.unwrap(), Value::Scalar(Scalar::Null));
    assert!(converted.is_empty());

    for is_try in [true, false] {
        let validity = Some(Bitmap::new_constant(false, 2));
        let (result, converted) =
            run_on_block(&registry, is_try, constant(1), &block, int16(), validity);
        result.unwrap();
        assert!(converted.is_empty());
    }

    let (result, converted) = run_on_block(&registry, true, constant(1), &block, int16(), None);
    assert_eq!(
        result.unwrap(),
        Value::Scalar(Scalar::Number(NumberScalar::Int16(1)))
    );
    assert_eq!(converted, vec![0]);
}

/// A NULL cast to a non-nullable type fails, and has no value to convert. `TRY_CAST` keeps the
/// values of a map non-nullable.
#[test]
fn test_try_cast_from_nullable_skips_nulls() {
    let registry = recording_registry(false);
    let entries = Column::Tuple(vec![
        Int32Type::from_data(vec![1, 2, 3]),
        Int32Type::from_opt_data(vec![Some(1), None, Some(2)]),
    ]);
    let column = Column::Map(Box::new(ArrayColumn::new(entries, vec![0, 1, 2, 3].into())));
    let dest_type = DataType::Map(Box::new(DataType::Tuple(vec![int32(), int16()])));
    let (result, converted) = run(&registry, true, column, dest_type, None);
    let column = result.unwrap().into_column().unwrap();
    assert_eq!(
        column.as_nullable().unwrap().validity(),
        &Bitmap::from_iter([true, false, true])
    );
    assert_eq!(converted, vec![0, 2]);
}

/// CAST rejects the NULLs that its function returns for active rows only.
#[test]
fn test_cast_rejects_nulls_of_active_rows() {
    let mut registry = FunctionRegistry::empty();
    registry.register_1_arg_core::<Int32Type, NullableType<Int64Type>, _, _>(
        "to_int64",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<Int32Type, NullableType<Int64Type>>(|value, output, ctx| {
            // 0 converts to NULL, like a JSON null, and an inactive row is left NULL.
            if value == 0 || !is_active(ctx, output.len()) {
                output.push_null();
            } else {
                output.push(value as i64);
            }
        }),
    );
    full(&mut registry, int32(), int64());
    let validity = Bitmap::from_iter([true, false]);

    for values in [vec![1, 2], vec![1, 0]] {
        let column = Int32Type::from_data(values);
        let (result, _) = run(&registry, false, column, int64(), Some(validity.clone()));
        result.unwrap();
    }
    let column = Int32Type::from_data(vec![0, 1]);
    let (result, _) = run(&registry, false, column, int64(), Some(validity));
    assert!(
        result
            .unwrap_err()
            .message()
            .contains("result has null values")
    );
}

/// A cast registered as a chain converts through its intermediate type, and only through it.
#[test]
fn test_casts_through_registered_chain() {
    let registry = recording_registry(true);
    assert!(casts_through(&int32(), &int64(), &int16(), &registry));
    assert!(casts_through(
        &int32().wrap_nullable(),
        &int64().wrap_nullable(),
        &int16().wrap_nullable(),
        &registry
    ));
    assert!(!casts_through(&int32(), &int16(), &int16(), &registry));
    // A cast with a function of its own runs one step.
    assert!(!casts_through(&int32(), &int64(), &int64(), &registry));
}

/// Runs `sql` with the builtin functions over a block of `num_rows` rows with `columns`, for the
/// rows in `validity`.
fn run_builtin(
    sql: &str,
    columns: &[(&str, Column)],
    num_rows: usize,
    validity: Option<Bitmap>,
) -> Value<AnyType> {
    let types = columns
        .iter()
        .map(|(name, column)| (*name, column.data_type()))
        .collect::<Vec<_>>();
    let raw_expr = parse_raw_expr(sql, &types, &BUILTIN_FUNCTIONS);
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
    let entries = columns
        .iter()
        .map(|(_, column)| column.clone().into())
        .collect::<Vec<_>>();
    let block = DataBlock::new(entries, num_rows);
    let func_ctx = FunctionContext::default();
    let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
    evaluator
        .partial_run(&expr, validity, &mut EvaluateOptions::default(), false)
        .unwrap()
}

/// A Variant scalar is converted when any active row uses it, not only the first row.
#[test]
fn test_try_cast_variant_scalar_used_by_later_row() {
    let validity = Bitmap::from_iter([false, true]);
    for (sql, len) in [
        ("TRY_CAST(parse_json('[1,2]') AS ARRAY(INT32))", 2),
        ("TRY_CAST(parse_json('{\"a\":1}') AS MAP(STRING, INT32))", 1),
    ] {
        let value = run_builtin(sql, &[], 2, Some(validity.clone()));
        let scalar = value.into_scalar().unwrap();
        let values = match scalar {
            Scalar::Array(values) | Scalar::Map(values) => values,
            scalar => panic!("{sql}: {scalar}"),
        };
        assert_eq!(values.len(), len, "{sql}");
    }
    let sql = "TRY_CAST('[1,2]' AS VECTOR(2))";
    let expected = run_builtin(sql, &[], 1, None);
    let value = run_builtin(sql, &[], 2, Some(validity));
    assert_eq!(value, expected, "{sql}");
}

/// The only element of the arrays failing is one row, not a scalar shared by every row.
#[test]
fn test_try_cast_single_failed_element() {
    let values = StringType::from_data(vec!["not-an-int"]);
    let column = Column::Array(Box::new(ArrayColumn::new(values, vec![0, 0, 1].into())));
    let value = run_builtin("TRY_CAST(a AS ARRAY(INT32))", &[("a", column)], 2, None);
    let column = value.into_column().unwrap();
    let column = column.as_nullable().unwrap();
    assert_eq!(column.validity(), &Bitmap::from_iter([true, true]));
    let arrays = column.column().as_array().unwrap();
    assert_eq!(arrays.offsets().as_slice(), &[0, 0, 1]);
    let elements = arrays.underlying_column();
    assert_eq!(
        elements.as_nullable().unwrap().validity(),
        &Bitmap::from_iter([false])
    );
}

/// The failed values of a sliced map belong to the rows of the slice.
#[test]
fn test_try_cast_sliced_map_marks_its_own_rows() {
    let entries = Column::Tuple(vec![
        StringType::from_data(vec!["a", "b", "c"]),
        Int32Type::from_opt_data(vec![Some(1), None, Some(2)]),
    ]);
    let column = Column::Map(Box::new(ArrayColumn::new(entries, vec![0, 1, 2, 3].into())));
    let column = column.slice(1..3);
    let value = run_builtin(
        "TRY_CAST(a AS MAP(STRING, INT16))",
        &[("a", column)],
        2,
        None,
    );
    let column = value.into_column().unwrap();
    assert_eq!(
        column.as_nullable().unwrap().validity(),
        &Bitmap::from_iter([false, true])
    );
}
