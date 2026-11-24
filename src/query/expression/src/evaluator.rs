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

use std::collections::HashSet;
use std::ops::Not;

use databend_common_ast::Span;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;
use jsonb::Value as JsonbValue;
use jsonb::jsonpath::JsonPath;
use jsonb::jsonpath::parse_json_path;

use crate::BlockEntry;
use crate::Function;
use crate::FunctionContext;
use crate::FunctionEval;
use crate::FunctionID;
use crate::FunctionRegistry;
use crate::LikePattern;
use crate::RemoteExpr;
use crate::ScalarRef;
use crate::SelectOp;
use crate::block::DataBlock;
use crate::cast_plan::CastCall;
use crate::cast_plan::CastPlan;
use crate::cast_plan::CastStep;
use crate::cast_plan::resolve_cast_plan;
use crate::expr::*;
use crate::expression::Expr;
use crate::function::EvalContext;
use crate::kernels::has_nested_decimal;
use crate::kernels::select_if_then_else;
use crate::type_check::format_function_argument_mismatch_hint;
use crate::types::BinaryColumn;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DecimalColumn;
use crate::types::DecimalDataType;
use crate::types::DictColumn;
use crate::types::EncodedColumn;
use crate::types::F32;
use crate::types::NullableType;
use crate::types::ReturnType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::types::VectorScalar;
use crate::types::any::AnyType;
use crate::types::array::ArrayColumn;
use crate::types::nullable::NullableColumn;
use crate::utils::filter_helper::FilterHelpers;
use crate::utils::json_path_transform::PathStep;
use crate::utils::json_path_transform::replace_at_locations;
use crate::utils::json_path_transform::select_locations;
use crate::values::Column;
use crate::values::ColumnBuilder;
use crate::values::Scalar;
use crate::values::Value;
use crate::visitor::ValueVisitor;

pub struct EvaluateOptions<'a> {
    pub selection: Option<&'a [u32]>,
    pub suppress_error: bool,
    pub errors: Option<(MutableBitmap, String)>,
    pub strict_eval: bool,
}

impl Default for EvaluateOptions<'_> {
    fn default() -> Self {
        Self {
            selection: None,
            suppress_error: false,
            errors: None,
            strict_eval: true,
        }
    }
}

impl<'a> EvaluateOptions<'a> {
    pub fn new_for_select(selection: Option<&'a [u32]>) -> EvaluateOptions<'a> {
        Self {
            selection,
            suppress_error: false,
            errors: None,
            strict_eval: false,
        }
    }

    pub fn with_suppress_error(&mut self, suppress_error: bool) -> Self {
        Self {
            suppress_error,
            errors: None,
            selection: self.selection,
            strict_eval: self.strict_eval,
        }
    }
}

/// Merge a source error set into a target error set, keeping the union of
/// error rows and the first error message. Error sets travel upward through
/// nested calls while suppression is active, so both sibling arguments and
/// parent calls must accumulate instead of overwrite.
///
/// A length-1 source bitmap comes from a scalar subexpression (evaluated in
/// a one-row block); a scalar
/// error applies to every row, so it is broadcast to `num_rows` instead of
/// marking only row 0.
/// Rows that failed in either bitmap; an unset bit is a failed row.
fn and_failures(left: Option<Bitmap>, right: Option<Bitmap>) -> Option<Bitmap> {
    match (left, right) {
        (Some(left), Some(right)) => Some(&left & &right),
        (left, right) => left.or(right),
    }
}

/// `validity` without the rows that `failed` unsets. A failed scalar fails every row.
fn exclude_failures(validity: Option<Bitmap>, failed: Option<&Bitmap>) -> Option<Bitmap> {
    let Some(failed) = failed else {
        return validity;
    };
    match validity {
        Some(validity) if failed.len() == 1 && validity.len() != 1 => {
            if failed.get_bit(0) {
                Some(validity)
            } else {
                Some(Bitmap::new_constant(false, validity.len()))
            }
        }
        Some(validity) => Some(&validity & failed),
        None => Some(failed.clone()),
    }
}

/// A scalar function call whose arguments are evaluated.
struct EvaluatedCall<'a> {
    span: Span,
    id: &'a FunctionID,
    function: &'a Function,
    generics: &'a [DataType],
    args: Vec<Value<AnyType>>,
    /// The rows that the call runs for, of which the rows in `validity` are active.
    num_rows: usize,
    validity: Option<Bitmap>,
    /// Whether the call runs once per entry of a dictionary argument.
    dictionary_eval: bool,
}

/// The validity of the single row of `value` that a scalar became, when `is_scalar`: every active
/// row of `validity` uses the scalar, so it is active when any of them is.
fn scalar_row_validity(validity: Option<Bitmap>, is_scalar: bool) -> Option<Bitmap> {
    match validity {
        Some(validity) if is_scalar => Some(Bitmap::new_constant(validity.true_count() > 0, 1)),
        validity => validity,
    }
}

/// The rows, given by `offsets`, that have a failed nested value.
fn row_failures(offsets: &[u64], failed: Option<Bitmap>) -> Option<Bitmap> {
    let failed = failed?;
    let rows = offsets
        .windows(2)
        .map(|row| (row[0]..row[1]).all(|idx| failed.get_bit(idx as usize)))
        .collect::<Bitmap>();
    (rows.null_count() > 0).then_some(rows)
}

fn merge_eval_errors(
    target: &mut Option<(MutableBitmap, String)>,
    source: Option<(MutableBitmap, String)>,
    num_rows: usize,
) {
    let Some((src_valids, src_msg)) = source else {
        return;
    };
    if src_valids.len() == 1 && num_rows > 1 {
        if !src_valids.get(0) {
            *target = Some((Bitmap::new_constant(false, num_rows).make_mut(), src_msg));
        }
        return;
    }
    match target {
        Some((valids, _)) => {
            for (row, valid) in src_valids.iter().enumerate() {
                if !valid && row < valids.len() {
                    valids.set(row, false);
                }
            }
        }
        None => *target = Some((src_valids, src_msg)),
    }
}

/// Expand the error set of an all-scalar evaluation over the rows the
/// evaluation ran for: the rows enabled by `validity`, or every row when
/// there is no partial selection.
///
/// A scalar evaluation records its error at row 0, which is
/// indistinguishable from a row-0-only column error; expanding here, where
/// the scalar-ness of the evaluation is still known, lets `merge_eval_errors`
/// and `render_error` treat every incoming bitmap as row-aligned. When no
/// row is enabled the error is dropped: the expression ran for nobody.
fn expand_scalar_error(
    errors: &mut Option<(MutableBitmap, String)>,
    validity: Option<&Bitmap>,
    num_rows: usize,
) {
    let Some((valids, msg)) = errors.take() else {
        return;
    };
    let has_enabled_rows = match validity {
        Some(validity) => validity.null_count() < validity.len(),
        None => true,
    };
    if !has_enabled_rows {
        return;
    }
    if valids.null_count() == 0 {
        // No error recorded; keep the (empty) error set.
        *errors = Some((valids, msg));
        return;
    }
    let expanded = match validity {
        Some(validity) => validity.not().make_mut(),
        None => Bitmap::new_constant(false, num_rows).make_mut(),
    };
    *errors = Some((expanded, msg));
}

pub struct Evaluator<'a> {
    data_block: &'a DataBlock,
    func_ctx: &'a FunctionContext,
    fn_registry: &'a FunctionRegistry,
}

impl<'a> Evaluator<'a> {
    pub fn new(
        data_block: &'a DataBlock,
        func_ctx: &'a FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> Self {
        Evaluator {
            data_block,
            func_ctx,
            fn_registry,
        }
    }

    pub fn data_block(&self) -> &DataBlock {
        self.data_block
    }

    pub fn func_ctx(&self) -> &FunctionContext {
        self.func_ctx
    }

    /// Records that a non-deterministic function computed a value valid only for this query.
    pub(crate) fn record_eval(&self, function: &Function) {
        if self
            .fn_registry
            .properties
            .get(&function.signature.name)
            .is_some_and(|property| property.non_deterministic)
        {
            self.func_ctx.query_specific_values.record();
        }
    }

    #[cfg(debug_assertions)]
    fn check_expr(&self, expr: &Expr) {
        let column_refs = expr.column_refs();
        for (index, data_type) in column_refs.iter() {
            let column = self.data_block.get_by_offset(*index);
            if data_type.matches_physical_type(&column.data_type()) {
                continue;
            }
            if (column.data_type() == DataType::Null && data_type.is_nullable())
                || (column.data_type().is_nullable() && data_type == &DataType::Null)
            {
                continue;
            }
            assert_eq!(
                &column.data_type(),
                data_type,
                "column data type mismatch at index: {index}, expr: {}",
                expr.fmt_with_options(true)
            );
        }
    }

    pub fn run(&self, expr: &Expr) -> Result<Value<AnyType>> {
        self.partial_run(expr, None, &mut EvaluateOptions::default(), false)
            .map_err(|err| Self::map_err(err, expr))
    }

    /// Adds the expression to an error of evaluating it, like [`Self::run`] does.
    pub fn map_err(err: ErrorCode, expr: &Expr) -> ErrorCode {
        let expr_str = format!("`{}`", expr.sql_display());
        if err.message().contains(expr_str.as_str()) {
            err
        } else {
            ErrorCode::BadArguments(format!("{}, during run expr: {expr_str}", err.message()))
                .set_span(err.span())
        }
    }

    /// Run an expression partially, only the rows that are valid in the validity bitmap
    /// will be evaluated, the rest will be default values and should not throw any error.
    ///
    /// `keep_encoded`: the result may be an encoded column (see
    /// [`crate::types::EncodedColumn`]), otherwise it is decoded. Only set it where the result
    /// is stored (e.g. into a block) or passed to a function that accepts encoded arguments.
    #[recursive::recursive]
    pub fn partial_run(
        &self,
        expr: &Expr,
        validity: Option<Bitmap>,
        options: &mut EvaluateOptions,
        keep_encoded: bool,
    ) -> Result<Value<AnyType>> {
        debug_assert!(
            validity.is_none() || validity.as_ref().unwrap().len() == self.data_block.num_rows()
        );

        #[cfg(debug_assertions)]
        self.check_expr(expr);

        let result = match expr {
            Expr::Constant(Constant { scalar, .. }) => Value::Scalar(scalar.clone()),
            Expr::ColumnRef(ColumnRef { id, .. }) => self.data_block.get_by_offset(*id).value(),
            Expr::Cast(Cast {
                span,
                is_try,
                expr: inner,
                dest_type,
            }) => {
                let src_type = inner.data_type();
                // A cast that only changes the nullability keeps the values, and their encoding.
                let keep_encoded =
                    keep_encoded && src_type.remove_nullable() == dest_type.remove_nullable();
                let value = self.partial_run(inner, validity.clone(), options, keep_encoded)?;
                self.run_cast(
                    *span,
                    *is_try,
                    src_type,
                    dest_type,
                    value,
                    validity,
                    &|| expr.sql_display(),
                    options,
                )?
            }
            Expr::FunctionCall(FunctionCall {
                function,
                args,
                generics,
                ..
            }) if function.signature.name == "if" => {
                self.eval_if(args, generics, validity, options)?
            }

            Expr::FunctionCall(FunctionCall { function, args, .. })
                if function.signature.name == "and_filters" =>
            {
                self.eval_and_filters(args, validity, options)?
            }

            Expr::FunctionCall(FunctionCall { function, args, .. })
                if function.signature.name == "or_filters" =>
            {
                self.eval_or_filters(args, validity, options)?
            }

            Expr::FunctionCall(call) => {
                self.eval_common_call(call, validity, &|| expr.sql_display(), options)?
            }
            Expr::LambdaFunctionCall(LambdaFunctionCall {
                name,
                args,
                lambda_expr,
                return_type,
                ..
            }) => {
                let data_types = args.iter().map(|arg| arg.data_type().clone()).collect();
                let args = args
                    .iter()
                    .map(|expr| self.partial_run(expr, validity.clone(), options, false))
                    .collect::<Result<Vec<_>>>()?;
                assert!(
                    args.iter()
                        .filter_map(|val| match val {
                            Value::Column(col) => Some(col.len()),
                            Value::Scalar(_) => None,
                        })
                        .all_equal()
                );

                self.run_lambda(name, args, data_types, lambda_expr, return_type, validity)?
            }
        };

        match &result {
            Value::Scalar(result) => {
                assert!(
                    result.as_ref().is_value_of_type(expr.data_type()),
                    "{} is not of type {}",
                    result,
                    expr.data_type()
                )
            }
            Value::Column(col) => assert!(
                expr.data_type().matches_physical_type(&col.data_type()),
                "column result type mismatch: physical {}, logical {}",
                col.data_type(),
                expr.data_type()
            ),
        }

        if !expr.is_column_ref() && !expr.is_constant() && options.strict_eval {
            let mut check = CheckStrictValue;
            assert!(
                check.visit_value(result.clone()).is_ok(),
                "strict check fail on expr: {expr}",
            )
        }

        Ok(if keep_encoded {
            result
        } else {
            result.decode_owned()
        })
    }

    /// `<column> <op> <constant>` over an encoded column, or `<column> <op> <column>` over two
    /// frame of reference encoded columns, evaluated without decoding (on the deltas / once per
    /// dictionary value). `None` if the arguments do not have this form.
    pub fn try_compare_encoded(&self, op: &SelectOp, args: &[Expr]) -> Option<Bitmap> {
        let encoded = |id: usize| match self.data_block.get_by_offset(id) {
            BlockEntry::Column(Column::Encoded(encoded)) => Some(encoded),
            _ => None,
        };
        let (id, scalar, op) = match args {
            [
                Expr::ColumnRef(ColumnRef { id, .. }),
                Expr::Constant(Constant { scalar, .. }),
            ] => (*id, scalar, op.clone()),
            [
                Expr::Constant(Constant { scalar, .. }),
                Expr::ColumnRef(ColumnRef { id, .. }),
            ] => (*id, scalar, op.reverse()),
            // `<column> <op> <column>`, both frame of reference encoded.
            [
                Expr::ColumnRef(ColumnRef { id: lhs, .. }),
                Expr::ColumnRef(ColumnRef { id: rhs, .. }),
            ] => {
                let rhs = encoded(*rhs)?.as_for()?;
                return encoded(*lhs)?.as_for()?.compare_for(op, rhs);
            }
            _ => return None,
        };
        encoded(id)?.compare_scalar(&op, scalar)
    }

    /// Evaluates a filter's LIKE over a dictionary encoded string column without decoding.
    /// Nullable columns apply their validity after LIKE / NOT LIKE; NULL selects as false.
    /// With `rows`, the result is only correct at `rows`, see [`DictColumn::like`].
    pub fn try_like_encoded(
        &self,
        expr: &Expr,
        pattern: &LikePattern,
        not: bool,
        rows: Option<&[u32]>,
    ) -> Option<Bitmap> {
        let Expr::ColumnRef(ColumnRef { id, .. }) = expr else {
            return None;
        };
        let BlockEntry::Column(column) = self.data_block.get_by_offset(*id) else {
            return None;
        };
        let (column, validity) = match column {
            Column::Nullable(column) => (column.column(), Some(column.validity())),
            column => (column, None),
        };
        let Column::Encoded(encoded) = column else {
            return None;
        };
        let bitmap = encoded.as_dict()?.like(pattern, not, rows)?;
        Some(match validity {
            Some(validity) => &bitmap & validity,
            None => bitmap,
        })
    }

    fn take_dictionary_arg(args: &mut [Value<AnyType>]) -> Option<(usize, DictColumn)> {
        let (index, arg) = args
            .iter()
            .enumerate()
            .filter(|(_, arg)| matches!(arg, Value::Column(_)))
            .exactly_one()
            .ok()?;
        let Value::Column(Column::Encoded(encoded)) = arg else {
            return None;
        };
        let dict = encoded.as_dict()?;
        if dict.is_empty() || dict.values.len() >= dict.len() {
            return None;
        }
        let original = std::mem::replace(&mut args[index], Value::Scalar(Scalar::Null));
        let Value::Column(Column::Encoded(encoded)) = original else {
            args[index] = original;
            return None;
        };
        let EncodedColumn::Dict(dict) = *encoded else {
            args[index] = Value::Column(Column::Encoded(encoded));
            return None;
        };
        Some((index, dict))
    }

    fn eval_common_call(
        &self,
        call: &FunctionCall,
        validity: Option<Bitmap>,
        expr_display: &impl Fn() -> String,
        options: &mut EvaluateOptions,
    ) -> Result<Value<AnyType>> {
        let FunctionCall {
            span,
            id,
            function,
            generics,
            args,
            ..
        } = call;
        let child_suppress_error = function.signature.name == "is_not_error";
        // Suppression is sticky downward: once a boundary function opts in
        // (is_not_error), nested evaluations stay suppressed so row errors
        // bubble up to the boundary as data instead of raising midway.
        let mut child_option =
            options.with_suppress_error(options.suppress_error || child_suppress_error);
        if child_option.strict_eval && call.generics.is_empty() {
            child_option.strict_eval = false;
        }

        if args.len() == 2
            && let Some(op) = SelectOp::try_from_func_name(&function.signature.name)
            && let Some(bitmap) = self.try_compare_encoded(&op, args)
        {
            return Ok(Value::Column(Column::Boolean(bitmap)));
        }

        let (accepts_encoded_args, dictionary_eval) =
            self.encoded_args_policy(function, validity.as_ref(), options);
        let mut args_value = Vec::with_capacity(args.len());
        for expr in args {
            match self.partial_run(
                expr,
                validity.clone(),
                &mut child_option,
                accepts_encoded_args || dictionary_eval,
            ) {
                Ok(value) => args_value.push(value),
                Err(err) => {
                    return Err(self.attach_function_argument_mismatch_hint(
                        err,
                        &function.signature.name,
                        id.params(),
                        args,
                    ));
                }
            }
        }
        let errors = if child_suppress_error {
            child_option.errors.take()
        } else {
            None
        };
        // Non-boundary calls are transparent to suppression: errors from
        // nested evaluations keep bubbling up so a boundary function
        // (is_not_error) observes errors from its whole subtree, not only
        // from its direct children.
        let nested_errors = if child_suppress_error {
            None
        } else {
            child_option.errors.take()
        };
        self.call_evaluated(
            EvaluatedCall {
                span: *span,
                id,
                function,
                generics,
                args: args_value,
                num_rows: self.data_block.num_rows(),
                validity,
                dictionary_eval,
            },
            errors,
            nested_errors,
            expr_display,
            options,
        )
    }

    /// Whether a call of `function` for the rows in `validity` takes encoded arguments, and
    /// whether it runs once per entry of a dictionary argument.
    fn encoded_args_policy(
        &self,
        function: &Function,
        validity: Option<&Bitmap>,
        options: &EvaluateOptions,
    ) -> (bool, bool) {
        let accepts_encoded_args = function
            .eval
            .as_scalar()
            .is_some_and(|(_, _, eval)| eval.accepts_encoded_args());
        let dictionary_eval = !accepts_encoded_args
            && validity.is_none()
            && options.selection.is_none()
            && !options.suppress_error
            && !function.signature.return_type.is_nullable_or_null()
            && function
                .signature
                .args_type
                .iter()
                .all(|ty| !ty.is_nullable_or_null())
            && self
                .fn_registry
                .properties
                .get(&function.signature.name)
                .is_some_and(|property| property.dictionary_eval && !property.non_deterministic);
        (accepts_encoded_args, dictionary_eval)
    }

    /// Calls a scalar function on its evaluated arguments. `errors` are the errors that the
    /// call starts with, and `nested_errors` those of evaluating the arguments, which bubble up
    /// through the call.
    fn call_evaluated(
        &self,
        call: EvaluatedCall,
        errors: Option<(MutableBitmap, String)>,
        nested_errors: Option<(MutableBitmap, String)>,
        expr_display: &impl Fn() -> String,
        options: &mut EvaluateOptions,
    ) -> Result<Value<AnyType>> {
        let EvaluatedCall {
            span,
            id,
            function,
            generics,
            mut args,
            num_rows,
            validity,
            dictionary_eval,
        } = call;
        let accepts_encoded_args = function
            .eval
            .as_scalar()
            .is_some_and(|(_, _, eval)| eval.accepts_encoded_args());
        if !accepts_encoded_args && !dictionary_eval {
            for arg in &mut args {
                if arg.is_encoded() {
                    *arg = std::mem::replace(arg, Value::Scalar(Scalar::Null)).decode_owned();
                }
            }
        }

        assert!(
            args.iter()
                .filter_map(|val| match val {
                    Value::Column(col) => Some(col.len()),
                    Value::Scalar(_) => None,
                })
                .all_equal()
        );

        // A call whose arguments are all scalars is evaluated once, and an
        // error it records applies to every row the call runs for. Strip the
        // validity mask during eval so the error is always recorded — a
        // scalar reports at row 0, which the validity gate in `set_error`
        // would otherwise drop whenever row 0 is not enabled — then expand
        // the recorded error over the enabled rows.
        let all_scalar = args.iter().all(|arg| matches!(arg, Value::Scalar(_)));
        let dictionary = dictionary_eval
            .then(|| {
                let Some((index, DictColumn { keys, values })) =
                    Self::take_dictionary_arg(&mut args)
                else {
                    for arg in &mut args {
                        if arg.is_encoded() {
                            *arg =
                                std::mem::replace(arg, Value::Scalar(Scalar::Null)).decode_owned();
                        }
                    }
                    return None;
                };
                args[index] = Value::Column(values);
                Some((index, keys))
            })
            .flatten();
        let mut ctx = EvalContext {
            generics,
            num_rows: dictionary
                .as_ref()
                .map_or(num_rows, |(index, _)| args[*index].len()),
            validity: if all_scalar { None } else { validity.clone() },
            errors,
            func_ctx: self.func_ctx,
            suppress_error: options.suppress_error,
            strict_eval: options.strict_eval,
        };

        let (_, _, eval) = function.eval.as_scalar().unwrap();
        self.record_eval(function);
        let mut result = eval.eval(&args, &mut ctx);
        if let Some((_, keys)) = dictionary {
            if let Some((valids, message)) = ctx.errors.take()
                && keys.iter().any(|key| !valids.get(key as usize))
            {
                return Err(ErrorCode::BadArguments(message).set_span(span));
            }
            ctx.num_rows = num_rows;
            if let Value::Column(values) = result {
                let dict = DictColumn::new(keys, values);
                // Consumers read dictionary columns through encoded views, which only some
                // types support (e.g. not the Boolean result of LIKE): expand the others.
                result = Value::Column(if DictColumn::supports(&dict.values.data_type()) {
                    Column::Encoded(Box::new(EncodedColumn::Dict(dict)))
                } else {
                    dict.decode()
                });
            }
        }

        if all_scalar {
            expand_scalar_error(&mut ctx.errors, validity.as_ref(), ctx.num_rows);
        }

        merge_eval_errors(&mut ctx.errors, nested_errors, ctx.num_rows);

        if options.suppress_error {
            // inject errors into options, parent will handle it
            merge_eval_errors(&mut options.errors, ctx.errors.take(), ctx.num_rows);
        } else {
            EvalContext::render_error(
                span,
                &ctx.errors,
                id.params(),
                args.as_slice(),
                &function.signature.name,
                &expr_display(),
                options.selection,
            )?;
        }
        Ok(result)
    }

    fn attach_function_argument_mismatch_hint(
        &self,
        err: ErrorCode,
        name: &str,
        params: &[Scalar],
        args: &[Expr],
    ) -> ErrorCode {
        let mut has_top_level_cast = false;
        let hint_args = args
            .iter()
            .map(|arg| match arg {
                Expr::Cast(Cast { expr, .. }) => {
                    has_top_level_cast = true;
                    expr.as_ref().clone()
                }
                _ => arg.clone(),
            })
            .collect::<Vec<_>>();
        if !has_top_level_cast {
            return err;
        }

        let Some(hint) =
            format_function_argument_mismatch_hint(name, params, &hint_args, self.fn_registry)
        else {
            return err;
        };

        err.add_message_back(format!("\n\nhint: {hint}"))
    }

    /// Runs `CAST` or, with `is_try`, `TRY_CAST` of `value` from `src_type` to `dest_type`.
    #[allow(clippy::too_many_arguments)]
    pub fn run_cast(
        &self,
        span: Span,
        is_try: bool,
        src_type: &DataType,
        dest_type: &DataType,
        value: Value<AnyType>,
        validity: Option<Bitmap>,
        expr_display: &impl Fn() -> String,
        options: &mut EvaluateOptions,
    ) -> Result<Value<AnyType>> {
        let plan = self.cast_plan(span, src_type, dest_type)?;
        let (value, failed) =
            self.run_cast_plan(span, &plan, value, validity, is_try, expr_display, options)?;
        // A failed row of `TRY_CAST` is NULL by now, as its destination is nullable.
        debug_assert!(failed.is_none());
        Ok(value)
    }

    fn cast_plan(&self, span: Span, src_type: &DataType, dest_type: &DataType) -> Result<CastPlan> {
        resolve_cast_plan(src_type, dest_type, self.fn_registry).ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "unable to cast type `{src_type}` to type `{dest_type}`"
            ))
            .set_span(span)
        })
    }

    /// Runs a cast plan. Rows outside `validity` are not converted.
    ///
    /// A row that fails is an error for `CAST`. For `TRY_CAST` (`is_try`), it is unset in the
    /// returned bitmap, and becomes NULL at the nearest step with a nullable destination.
    #[allow(clippy::too_many_arguments)]
    fn run_cast_plan(
        &self,
        span: Span,
        plan: &CastPlan,
        value: Value<AnyType>,
        validity: Option<Bitmap>,
        is_try: bool,
        expr_display: &impl Fn() -> String,
        options: &mut EvaluateOptions,
    ) -> Result<(Value<AnyType>, Option<Bitmap>)> {
        let dest_type = &plan.dest_type;
        macro_rules! run {
            ($plan:expr, $value:expr, $validity:expr) => {
                self.run_cast_plan(
                    span,
                    $plan,
                    $value,
                    $validity,
                    is_try,
                    expr_display,
                    options,
                )
            };
        }

        let (value, failed) = match &plan.step {
            CastStep::Identity => (value, None),
            CastStep::Null if is_try && !dest_type.is_nullable_or_null() => {
                let len = match &value {
                    Value::Scalar(_) => 1,
                    Value::Column(column) => column.len(),
                };
                let value = Self::cast_null(
                    span,
                    dest_type,
                    value,
                    Some(Bitmap::new_constant(false, len)),
                )?;
                (value, Some(Bitmap::new_constant(false, len)))
            }
            CastStep::Null => (Self::cast_null(span, dest_type, value, validity)?, None),
            CastStep::Function { call, .. } => self.run_cast_function(
                span,
                plan,
                call,
                plan.rejects_null(),
                value,
                validity,
                is_try,
                expr_display,
                options,
            )?,
            CastStep::Chain(steps) => {
                let mut value = value;
                let mut validity = validity;
                let mut failed = None;
                for step in steps {
                    let (new_value, step_failed) = run!(step, value, validity.clone())?;
                    value = new_value;
                    // A row that failed has no value for the next steps to convert.
                    validity = exclude_failures(validity, step_failed.as_ref());
                    failed = and_failures(failed, step_failed);
                }
                (value, failed)
            }
            CastStep::Nullable(inner) => match value {
                Value::Scalar(Scalar::Null) => (Value::Scalar(Scalar::Null), None),
                Value::Scalar(_) => run!(inner, value, validity)?,
                Value::Column(Column::Nullable(col)) => {
                    let (column, column_validity) = col.destructure();
                    let validity = validity
                        .map(|validity| &validity & &column_validity)
                        .unwrap_or(column_validity);
                    let (column, failed) =
                        run!(inner, Value::Column(column), Some(validity.clone()))?;
                    let column = column.into_column().unwrap();
                    (Value::Column(column.wrap_nullable(Some(validity))), failed)
                }
                other => unreachable!("source: {}", other),
            },
            CastStep::FromNullable(inner) => match value {
                Value::Scalar(Scalar::Null) => {
                    let has_valid = validity
                        .map(|validity| validity.null_count() < validity.len())
                        .unwrap_or(true);
                    if has_valid && !is_try {
                        return Err(ErrorCode::BadArguments(format!(
                            "unable to cast type `NULL` to type `{dest_type}`"
                        ))
                        .set_span(span));
                    }
                    let failed = (has_valid && is_try).then(|| Bitmap::new_constant(false, 1));
                    (Value::Scalar(Scalar::default_value(dest_type)), failed)
                }
                Value::Scalar(_) => run!(inner, value, validity)?,
                Value::Column(Column::Nullable(col)) => {
                    let (column, column_validity) = col.destructure();
                    let has_valid_nulls = validity
                        .as_ref()
                        .map(|validity| {
                            (validity & &column_validity).null_count() > validity.null_count()
                        })
                        .unwrap_or_else(|| column_validity.null_count() > 0);
                    if has_valid_nulls && !is_try {
                        return Err(ErrorCode::BadArguments(format!(
                            "unable to cast type `NULL` to type `{dest_type}`"
                        ))
                        .set_span(span));
                    }
                    // A NULL has no value to convert.
                    let validity = Some(match validity {
                        Some(validity) => &validity & &column_validity,
                        None => column_validity.clone(),
                    });
                    let (column, failed) = run!(inner, Value::Column(column), validity)?;
                    let failed = if has_valid_nulls {
                        and_failures(failed, Some(column_validity))
                    } else {
                        failed
                    };
                    (column, failed)
                }
                other => unreachable!("source: {}", other),
            },
            CastStep::ToNullable(inner) => {
                let (value, failed) = run!(inner, value, validity)?;
                let value = match value {
                    Value::Scalar(scalar) => Value::Scalar(scalar),
                    Value::Column(column) => Value::Column(column.wrap_nullable(None)),
                };
                (value, failed)
            }
            CastStep::Empty => {
                let inner_type = match dest_type {
                    DataType::Array(inner) | DataType::Map(inner) => inner,
                    _ => unreachable!("destination: {}", dest_type),
                };
                let value = match value {
                    Value::Scalar(_) => {
                        let column = ColumnBuilder::with_capacity(inner_type, 0).build();
                        match dest_type {
                            DataType::Array(_) => Value::Scalar(Scalar::Array(column)),
                            _ => Value::Scalar(Scalar::Map(column)),
                        }
                    }
                    Value::Column(column) => Value::Column(
                        ColumnBuilder::repeat_default(dest_type, column.len()).build(),
                    ),
                };
                (value, None)
            }
            CastStep::Elements(inner) => {
                let is_map = matches!(dest_type, DataType::Map(_));
                match value {
                    Value::Scalar(Scalar::Array(values) | Scalar::Map(values)) => {
                        let len = values.len();
                        let validity = validity.map(|validity| {
                            Bitmap::new_constant(validity.null_count() != validity.len(), len)
                        });
                        let (values, failed) = run!(inner, Value::Column(values), validity)?;
                        let values = values.into_column().unwrap();
                        let value = if is_map {
                            Scalar::Map(values)
                        } else {
                            Scalar::Array(values)
                        };
                        (Value::Scalar(value), row_failures(&[0, len as u64], failed))
                    }
                    Value::Column(Column::Array(col) | Column::Map(col)) => {
                        let validity = validity.map(|validity| {
                            let offsets = col.offsets();
                            let len = offsets[offsets.len() - 1] - offsets[0];
                            let mut inner_validity = MutableBitmap::with_capacity(len as usize);
                            for (index, row) in offsets.windows(2).enumerate() {
                                inner_validity.extend_constant(
                                    (row[1] - row[0]) as usize,
                                    validity.get_bit(index),
                                );
                            }
                            inner_validity.into()
                        });
                        let (values, failed) =
                            run!(inner, Value::Column(col.underlying_column()), validity)?;
                        // The underlying values start at 0, unlike the offsets of a slice.
                        let offsets = col.underlying_offsets();
                        let failed = row_failures(&offsets, failed);
                        let col =
                            Box::new(ArrayColumn::new(values.into_column().unwrap(), offsets));
                        let col = if is_map {
                            Column::Map(col)
                        } else {
                            Column::Array(col)
                        };
                        (Value::Column(col), failed)
                    }
                    other => unreachable!("source: {}", other),
                }
            }
            CastStep::Tuple(fields_plan) => match value {
                Value::Scalar(Scalar::Tuple(fields)) => {
                    let mut new_fields = Vec::with_capacity(fields.len());
                    let mut failed = None;
                    for (field, field_plan) in fields.into_iter().zip(fields_plan) {
                        let (field, field_failed) =
                            run!(field_plan, Value::Scalar(field), validity.clone())?;
                        new_fields.push(field.into_scalar().unwrap());
                        failed = and_failures(failed, field_failed);
                    }
                    (Value::Scalar(Scalar::Tuple(new_fields)), failed)
                }
                Value::Column(Column::Tuple(fields)) => {
                    let mut new_fields = Vec::with_capacity(fields.len());
                    let mut failed = None;
                    for (field, field_plan) in fields.into_iter().zip(fields_plan) {
                        let (field, field_failed) =
                            run!(field_plan, Value::Column(field), validity.clone())?;
                        new_fields.push(field.into_column().unwrap());
                        failed = and_failures(failed, field_failed);
                    }
                    (Value::Column(Column::Tuple(new_fields)), failed)
                }
                other => unreachable!("source: {}", other),
            },
            CastStep::VariantToArray(inner) => {
                let (col, is_scalar) = Self::variant_column(value);
                let validity = scalar_row_validity(validity, is_scalar);
                let (elements, offsets, invalid_rows) =
                    Self::variant_array_elements(&col, validity.as_ref(), is_try)?;
                let (values, failed) = run!(inner, Value::Column(elements), None)?;
                let failed = and_failures(invalid_rows, row_failures(&offsets, failed));
                let col = Column::Array(Box::new(ArrayColumn::new(
                    values.into_column().unwrap(),
                    offsets.into(),
                )));
                (Self::nested_value(col, is_scalar), failed)
            }
            CastStep::VariantToMap(inner) => {
                let (col, is_scalar) = Self::variant_column(value);
                let validity = scalar_row_validity(validity, is_scalar);
                let (keys, values, offsets, invalid_rows) =
                    Self::variant_object_entries(&col, validity.as_ref(), is_try)?;
                let (values, failed) = run!(inner, Value::Column(values), None)?;
                let failed = and_failures(invalid_rows, row_failures(&offsets, failed));
                let col = Column::Map(Box::new(ArrayColumn::new(
                    Column::Tuple(vec![keys, values.into_column().unwrap()]),
                    offsets.into(),
                )));
                (Self::nested_value(col, is_scalar), failed)
            }
            CastStep::ToVector => Self::cast_to_vector(span, dest_type, value, validity, is_try)?,
        };

        // A failed row becomes NULL at the nearest nullable destination.
        Ok(match failed {
            Some(failed) if dest_type.is_nullable_or_null() => {
                (value.wrap_nullable(Some(failed)), None)
            }
            failed => (value, failed),
        })
    }

    /// Runs the cast function of a plan step.
    #[allow(clippy::too_many_arguments)]
    fn run_cast_function(
        &self,
        span: Span,
        plan: &CastPlan,
        call: &CastCall,
        reject_null: bool,
        value: Value<AnyType>,
        validity: Option<Bitmap>,
        is_try: bool,
        expr_display: &impl Fn() -> String,
        options: &mut EvaluateOptions,
    ) -> Result<(Value<AnyType>, Option<Bitmap>)> {
        // A scalar that no active row uses, e.g. one that failed in an earlier step of a chain,
        // is not converted.
        if matches!(value, Value::Scalar(_))
            && validity.as_ref().is_some_and(|v| v.true_count() == 0)
        {
            return Ok((Value::Scalar(Scalar::default_value(&plan.dest_type)), None));
        }
        let src_type = &plan.src_type;
        let active = validity.clone();
        // `TRY_CAST` collects the failed rows instead of reporting them. The errors of a column
        // are its own rows, e.g. the elements of nested values. Those of a scalar belong to every
        // row that uses it; `TRY_CAST` records them for the single row of the scalar.
        let mut try_options = EvaluateOptions {
            suppress_error: true,
            ..EvaluateOptions::default()
        };
        let (validity, num_rows, step_options) = match &value {
            Value::Scalar(_) if is_try => (None, 1, &mut try_options),
            Value::Scalar(_) => {
                let num_rows = validity
                    .as_ref()
                    .map_or(self.data_block.num_rows(), Bitmap::len);
                (validity, num_rows, options)
            }
            Value::Column(col) if is_try => (validity, col.len(), &mut try_options),
            Value::Column(col) => (validity, col.len(), options),
        };
        // A cast function is not evaluated per dictionary entry: none has the property, and
        // `partial_run` decodes the input of a cast that converts its values. The errors of the
        // function belong to the cast.
        let value = self.call_evaluated(
            EvaluatedCall {
                span,
                id: &call.id,
                function: &call.function,
                generics: &call.generics,
                args: vec![value],
                num_rows,
                validity,
                dictionary_eval: false,
            },
            None,
            None,
            expr_display,
            step_options,
        )?;
        let mut failed = try_options.errors.map(|(failed, _)| failed.into());

        // Only a NULL in an active row is rejected: an inactive row is not converted, and the
        // function may leave a NULL there.
        if reject_null {
            let (value, nulls) = match value {
                Value::Scalar(Scalar::Null) => {
                    let has_active = active
                        .as_ref()
                        .is_none_or(|active| active.null_count() < active.len());
                    (
                        Value::Scalar(Scalar::default_value(&plan.dest_type)),
                        has_active.then(|| Bitmap::new_constant(false, 1)),
                    )
                }
                Value::Column(Column::Nullable(col)) => {
                    let (column, validity) = col.destructure();
                    let validity = match &active {
                        Some(active) => &validity | &!active,
                        None => validity,
                    };
                    (
                        Value::Column(column),
                        (validity.null_count() > 0).then_some(validity),
                    )
                }
                value => (value, None),
            };
            if let Some(nulls) = nulls {
                if !is_try {
                    return Err(ErrorCode::BadArguments(format!(
                        "unable to cast type `{src_type}` to type `{}`, result has null values",
                        plan.dest_type
                    ))
                    .set_span(span));
                }
                failed = and_failures(failed, Some(nulls));
            }
            return Ok((value, failed));
        }
        Ok((value, failed))
    }

    /// Runs `call`, the function of a cast step, on the single value `value` as `CAST` does, or
    /// `None` when it fails.
    pub(crate) fn run_cast_call(&self, call: &CastCall, value: Scalar) -> Option<Scalar> {
        let value = self
            .call_evaluated(
                EvaluatedCall {
                    span: None,
                    id: &call.id,
                    function: &call.function,
                    generics: &call.generics,
                    args: vec![Value::Scalar(value)],
                    num_rows: 1,
                    validity: None,
                    dictionary_eval: false,
                },
                None,
                None,
                &String::new,
                &mut EvaluateOptions::default(),
            )
            .ok()?;
        value.into_scalar().ok()
    }

    /// Converts each String, Variant or numeric Array value to a vector.
    fn cast_to_vector(
        span: Span,
        dest_type: &DataType,
        value: Value<AnyType>,
        validity: Option<Bitmap>,
        is_try: bool,
    ) -> Result<(Value<AnyType>, Option<Bitmap>)> {
        let DataType::Vector(vector_type) = dest_type else {
            unreachable!("destination: {}", dest_type);
        };
        let dimension = vector_type.dimension() as usize;
        let convert = |value: ScalarRef| match value {
            ScalarRef::String(val) => Self::cast_string_to_vector_scalar(span, val, dimension),
            ScalarRef::Variant(val) => Self::cast_variant_to_vector_scalar(span, val, dimension),
            ScalarRef::Array(col) => Self::cast_array_to_vector_scalar(span, col, dimension),
            other => unreachable!("source: {}", other),
        };

        match value {
            Value::Scalar(scalar) => {
                // A scalar is converted when any active row uses it.
                if validity.as_ref().is_some_and(|v| v.true_count() == 0) {
                    return Ok((Value::Scalar(Scalar::default_value(dest_type)), None));
                }
                match convert(scalar.as_ref()) {
                    Ok(vector) => Ok((Value::Scalar(Scalar::Vector(vector)), None)),
                    Err(_) if is_try => Ok((
                        Value::Scalar(Scalar::default_value(dest_type)),
                        Some(Bitmap::new_constant(false, 1)),
                    )),
                    Err(err) => Err(err),
                }
            }
            Value::Column(col) => {
                let mut builder = ColumnBuilder::with_capacity(dest_type, col.len());
                let mut failed = MutableBitmap::from_len_set(col.len());
                for (idx, val) in col.iter().enumerate() {
                    if validity.as_ref().map(|v| !v.get_bit(idx)).unwrap_or(false) {
                        builder.push_default();
                        continue;
                    }
                    match convert(val) {
                        Ok(vector) => builder.push(ScalarRef::Vector(vector.as_ref())),
                        Err(_) if is_try => {
                            builder.push_default();
                            failed.set(idx, false);
                        }
                        Err(err) => return Err(err),
                    }
                }
                let failed: Bitmap = failed.into();
                let failed = (failed.null_count() > 0).then_some(failed);
                Ok((Value::Column(builder.build()), failed))
            }
        }
    }

    /// `col` as a scalar when the cast input was a scalar.
    fn nested_value(col: Column, is_scalar: bool) -> Value<AnyType> {
        if is_scalar {
            Value::Scalar(col.index(0).unwrap().to_owned())
        } else {
            Value::Column(col)
        }
    }

    /// The value of a cast from `NULL`. Rows outside `validity` are not converted.
    fn cast_null(
        span: Span,
        dest_type: &DataType,
        value: Value<AnyType>,
        validity: Option<Bitmap>,
    ) -> Result<Value<AnyType>> {
        let len = match &value {
            Value::Scalar(_) => None,
            Value::Column(column) => Some(column.len()),
        };
        if !dest_type.is_nullable_or_null() {
            let has_valid = match &validity {
                Some(validity) => validity.null_count() < validity.len(),
                None => len != Some(0),
            };
            if has_valid {
                return Err(ErrorCode::BadArguments(format!(
                    "unable to cast type `NULL` to type `{dest_type}`"
                ))
                .set_span(span));
            }
        }
        Ok(match len {
            None => Value::Scalar(Scalar::default_value(dest_type)),
            Some(len) => Value::Column(ColumnBuilder::repeat_default(dest_type, len).build()),
        })
    }

    /// `value` as a Variant column, and whether it was a scalar.
    fn variant_column(value: Value<AnyType>) -> (BinaryColumn, bool) {
        match value {
            Value::Scalar(scalar) => {
                let column = ColumnBuilder::repeat(&scalar.as_ref(), 1, &DataType::Variant).build();
                (column.into_variant().unwrap(), true)
            }
            Value::Column(column) => (column.into_variant().unwrap(), false),
        }
    }

    /// The elements of each JSON array in `col`, with the offsets of each row. A value that is
    /// not an array has no elements, and neither has a row outside `validity`. A value that is
    /// not valid JSON is an error, or, with `invalid_as_null`, a row without elements that is
    /// unset in the returned bitmap, which is `None` when every value is valid.
    fn variant_array_elements(
        col: &BinaryColumn,
        validity: Option<&Bitmap>,
        invalid_as_null: bool,
    ) -> Result<(Column, Vec<u64>, Option<Bitmap>)> {
        let mut offsets = Vec::with_capacity(col.len() + 1);
        offsets.push(0);
        let mut builder = VariantType::create_builder(col.len(), &[]);
        let mut invalid_rows: Option<MutableBitmap> = None;
        for (idx, x) in col.iter().enumerate() {
            if validity.is_none_or(|validity| validity.get_bit(idx)) {
                match jsonb::from_slice(x) {
                    Ok(value) => {
                        for v in value.as_array().into_iter().flatten() {
                            v.write_to_vec(&mut builder.data);
                            builder.commit_row();
                        }
                    }
                    Err(_) if invalid_as_null => invalid_rows
                        .get_or_insert_with(|| MutableBitmap::from_len_set(col.len()))
                        .set(idx, false),
                    Err(e) => {
                        return Err(ErrorCode::BadArguments(format!(
                            "Expect to be valid json, got err: {e:?}"
                        )));
                    }
                }
            }
            offsets.push(builder.len() as u64);
        }
        Ok((
            Column::Variant(builder.build()),
            offsets,
            invalid_rows.map(Into::into),
        ))
    }

    /// The keys and values of each JSON object in `col`, with the offsets of each row, following
    /// the rules of [`Self::variant_array_elements`].
    fn variant_object_entries(
        col: &BinaryColumn,
        validity: Option<&Bitmap>,
        invalid_as_null: bool,
    ) -> Result<(Column, Column, Vec<u64>, Option<Bitmap>)> {
        let mut offsets = Vec::with_capacity(col.len() + 1);
        offsets.push(0);
        let mut key_builder = StringType::create_builder(col.len(), &[]);
        let mut value_builder = VariantType::create_builder(col.len(), &[]);
        let mut invalid_rows: Option<MutableBitmap> = None;
        for (idx, x) in col.iter().enumerate() {
            if validity.is_none_or(|validity| validity.get_bit(idx)) {
                match jsonb::from_slice(x) {
                    Ok(value) => {
                        for (k, v) in value.as_object().into_iter().flatten() {
                            key_builder.put_and_commit(k.as_str());
                            v.write_to_vec(&mut value_builder.data);
                            value_builder.commit_row();
                        }
                    }
                    Err(_) if invalid_as_null => invalid_rows
                        .get_or_insert_with(|| MutableBitmap::from_len_set(col.len()))
                        .set(idx, false),
                    Err(e) => {
                        return Err(ErrorCode::BadArguments(format!(
                            "Expect to be valid json, got err: {e:?}"
                        )));
                    }
                }
            }
            offsets.push(key_builder.len() as u64);
        }
        Ok((
            Column::String(key_builder.build()),
            Column::Variant(value_builder.build()),
            offsets,
            invalid_rows.map(Into::into),
        ))
    }

    fn cast_array_to_vector_scalar(
        span: Span,
        col: Column,
        dimension: usize,
    ) -> Result<VectorScalar> {
        if col.len() != dimension {
            return Err(ErrorCode::BadArguments(format!(
                "Array value cast to vector expected {} dimensions, not {}",
                dimension,
                col.len()
            ))
            .set_span(span));
        }

        let col = match col {
            Column::Nullable(inner) => {
                if inner.validity().null_count() > 0 {
                    return Err(ErrorCode::BadArguments(
                        "Array value cast to a vector has invalid value".to_string(),
                    )
                    .set_span(span));
                }
                inner.destructure().0
            }
            other => other,
        };

        let mut vals = Vec::with_capacity(dimension);
        match col {
            Column::Number(num_col) => {
                for i in 0..dimension {
                    let num = unsafe { num_col.index_unchecked(i) };
                    vals.push(num.to_f32());
                }
            }
            Column::Decimal(dec_col) => {
                for i in 0..dimension {
                    let dec = unsafe { dec_col.index_unchecked(i) };
                    vals.push(F32::from(dec.to_float32()));
                }
            }
            _ => {
                return Err(ErrorCode::BadArguments(
                    "Array value cast to a vector has invalid value".to_string(),
                )
                .set_span(span));
            }
        }

        Ok(VectorScalar::Float32(vals))
    }

    fn cast_string_to_vector_scalar(
        span: Span,
        val: &str,
        dimension: usize,
    ) -> Result<VectorScalar> {
        let input = val.trim();
        let Some(input) = input.strip_prefix('[') else {
            return Err(
                ErrorCode::BadArguments("Vector contents must start with `[`".to_string())
                    .set_span(span),
            );
        };
        let Some(input) = input.strip_suffix(']') else {
            return Err(
                ErrorCode::BadArguments("Vector contents must end with `]`".to_string())
                    .set_span(span),
            );
        };

        let mut vals = Vec::with_capacity(dimension);
        for item in input.split(',') {
            let Ok(num) = item.trim().parse::<f32>() else {
                return Err(ErrorCode::BadArguments(
                    "String value cast to a vector has invalid value".to_string(),
                )
                .set_span(span));
            };
            vals.push(F32::from(num));
        }

        if vals.len() != dimension {
            return Err(ErrorCode::BadArguments(format!(
                "String value cast to vector expected {} dimensions, not {}",
                dimension,
                vals.len()
            ))
            .set_span(span));
        }
        Ok(VectorScalar::Float32(vals))
    }

    fn cast_variant_to_vector_scalar(
        span: Span,
        val: &[u8],
        dimension: usize,
    ) -> Result<VectorScalar> {
        let vals = match jsonb::from_slice(val) {
            Ok(JsonbValue::Array(vals)) => vals,
            _ => {
                return Err(ErrorCode::BadArguments(
                    "Variant value cast to vector only support Array".to_string(),
                )
                .set_span(span));
            }
        };
        if vals.len() != dimension {
            return Err(ErrorCode::BadArguments(format!(
                "Variant value cast to vector expected {} dimensions, not {}",
                dimension,
                vals.len()
            ))
            .set_span(span));
        }
        let mut vector = Vec::with_capacity(dimension);
        for val in vals {
            let num = match val {
                JsonbValue::Number(num) => num.as_f64(),
                _ => {
                    return Err(ErrorCode::BadArguments(
                        "Variant value cast to a vector has invalid value".to_string(),
                    )
                    .set_span(span));
                }
            };
            vector.push(F32::from(num as f32));
        }

        Ok(VectorScalar::Float32(vector))
    }

    // `if` is a special builtin function that could partially evaluate its arguments
    // depending on the truthiness of the condition. `if` should register it's signature
    // as other functions do in `FunctionRegistry`, but it's does not necessarily implement
    // the eval function because it will be evaluated here.
    pub fn eval_if(
        &self,
        args: &[Expr],
        generics: &[DataType],
        validity: Option<Bitmap>,
        options: &mut EvaluateOptions,
    ) -> Result<Value<AnyType>> {
        if args.len() < 3 || args.len() % 2 == 0 {
            unreachable!()
        }

        let num_rows = self.data_block.num_rows();
        let len = self
            .data_block
            .columns()
            .iter()
            .find_map(|col| col.as_column().map(Column::len));

        // Evaluate the conditions in order and each result branch only for the
        // enabled rows that reach it. A branch that no enabled row reaches is not
        // evaluated (`None`): with an all-unset validity it would run for nobody,
        // so it could neither produce a selected value nor report an error.
        let mut validity = validity.unwrap_or_else(|| Bitmap::new_constant(true, num_rows));
        let mut conds = Vec::new();
        let mut flags = Vec::new();
        let mut results = Vec::new();
        for cond_idx in (0..args.len() - 1).step_by(2) {
            if validity.true_count() == 0 {
                break;
            }
            let cond = self.partial_run(&args[cond_idx], Some(validity.clone()), options, false)?;
            match cond.try_downcast::<NullableType<BooleanType>>().unwrap() {
                Value::Scalar(None | Some(false)) => {
                    results.push(None);
                    flags.push(Bitmap::new_constant(false, len.unwrap_or(1)));
                }
                Value::Scalar(Some(true)) => {
                    results.push(Some(self.partial_run(
                        &args[cond_idx + 1],
                        Some(validity.clone()),
                        options,
                        false,
                    )?));
                    validity = Bitmap::new_constant(false, num_rows);
                    flags.push(Bitmap::new_constant(true, len.unwrap_or(1)));
                    break;
                }
                Value::Column(cond) => {
                    let flag = cond.column() & cond.validity();
                    let branch_validity = (&validity) & (&flag);
                    if branch_validity.true_count() == 0 {
                        results.push(None);
                    } else {
                        results.push(Some(self.partial_run(
                            &args[cond_idx + 1],
                            Some(branch_validity),
                            options,
                            false,
                        )?));
                    }
                    validity = (&validity) & (&flag.not());
                    flags.push(flag);
                }
            };
            conds.push(cond);
        }
        let else_result = if validity.true_count() == 0 {
            None
        } else {
            Some(self.partial_run(&args[args.len() - 1], Some(validity), options, false)?)
        };

        let (first_reached, single_branch) = {
            let mut reached = results.iter().chain([&else_result]).flatten();
            (reached.next().cloned(), reached.next().is_none())
        };
        let Some(first_reached) = first_reached else {
            // No row is enabled, so every row of the result may hold any value.
            let default = ColumnBuilder::repeat_default(&generics[0], len.unwrap_or(1));
            return Ok(match len {
                Some(_) => Value::Column(default.build()),
                None => Value::Scalar(default.build_scalar()),
            });
        };

        // When every enabled row reaches the same branch, that branch is the
        // result: the rows that are not enabled may hold any value. A decimal
        // carried with a non-strict width, also inside a nested type, still goes
        // through the selection, which converts it to the width of its precision.
        if let Some(len) = len
            && single_branch
            && !has_nested_decimal(&generics[0])
            && DecimalDataType::from_value(&first_reached).is_none_or(|(ty, _)| ty.is_strict())
        {
            return Ok(Value::Column(
                first_reached.convert_to_full_column(&generics[0], len),
            ));
        }

        // The selection reads a branch that no enabled row reaches only on rows
        // that are not enabled, so any value of the result type can stand in for
        // it. A reached branch is used because `Scalar::default_value` does not
        // support every type.
        let results = results
            .into_iter()
            .map(|result| result.unwrap_or_else(|| first_reached.clone()))
            .collect::<Vec<_>>();
        let else_result = else_result.unwrap_or(first_reached);

        // Assert that all the arguments have the same length.
        assert!(
            conds
                .iter()
                .chain(results.iter())
                .chain([&else_result])
                .filter_map(|val| match val {
                    Value::Column(col) => Some(col.len()),
                    Value::Scalar(_) => None,
                })
                .all_equal()
        );

        if len.is_some()
            && let ([flag], [then_result]) = (flags.as_slice(), results.as_slice())
        {
            return Ok(Value::Column(select_if_then_else(
                flag,
                then_result,
                &else_result,
                &generics[0],
            )));
        }

        // Pick the results from the result branches depending on the condition.
        let mut output_builder = ColumnBuilder::with_capacity(&generics[0], len.unwrap_or(1));
        for row_idx in 0..(len.unwrap_or(1)) {
            unsafe {
                let result = flags
                    .iter()
                    .position(|flag| flag.get_bit(row_idx))
                    .map(|idx| results[idx].index_unchecked(row_idx))
                    .unwrap_or(else_result.index_unchecked(row_idx));
                output_builder.push(result);
            }
        }
        match len {
            Some(_) => Ok(Value::Column(output_builder.build())),
            None => Ok(Value::Scalar(output_builder.build_scalar())),
        }
    }

    // `and_filters` is a special builtin function similar to `if` that conditionally evaluate its arguments.
    fn eval_and_filters(
        &self,
        args: &[Expr],
        validity: Option<Bitmap>,
        options: &mut EvaluateOptions,
    ) -> Result<Value<AnyType>> {
        self.eval_filters(args, validity, options, false)
    }

    // `or_filters` is the or version of `and_filters`
    fn eval_or_filters(
        &self,
        args: &[Expr],
        validity: Option<Bitmap>,
        options: &mut EvaluateOptions,
    ) -> Result<Value<AnyType>> {
        self.eval_filters(args, validity, options, true)
    }

    fn eval_filters(
        &self,
        args: &[Expr],
        mut validity: Option<Bitmap>,
        options: &mut EvaluateOptions,
        is_or: bool,
    ) -> Result<Value<AnyType>> {
        assert!(args.len() >= 2);

        let base_validity = validity.clone();
        let mut result = None;
        for arg in args {
            match FilterHelpers::decode_predicate(self.partial_run(
                arg,
                validity.clone(),
                options,
                false,
            )?) {
                Value::Scalar(false) if is_or => {
                    continue;
                }
                Value::Scalar(true) if !is_or => {
                    continue;
                }
                Value::Scalar(_) if is_or => {
                    return Ok(FilterHelpers::finish_predicate(
                        base_validity.as_ref(),
                        true,
                    ));
                }
                Value::Scalar(_) => {
                    return Ok(Value::Scalar(Scalar::Boolean(false)));
                }
                Value::Column(flag) if is_or => {
                    result = Some(match result.take() {
                        Some(v) => &v | &flag,
                        None => flag.clone(),
                    });
                    validity = Some(match validity.take() {
                        Some(v) => &v & &flag.not(),
                        None => flag.not(),
                    });
                    if FilterHelpers::is_all_unset_bitmap(validity.as_ref().unwrap()) {
                        return Ok(FilterHelpers::finish_predicate(
                            base_validity.as_ref(),
                            true,
                        ));
                    }
                }
                Value::Column(flag) => {
                    validity = Some(match validity.take() {
                        Some(v) => &v & &flag,
                        None => flag,
                    });
                    if FilterHelpers::is_all_unset_bitmap(validity.as_ref().unwrap()) {
                        return Ok(Value::Scalar(Scalar::Boolean(false)));
                    }
                }
            }
        }

        match (is_or, result, validity) {
            (true, Some(bitmap), _) | (false, _, Some(bitmap)) => {
                Ok(Value::Column(Column::Boolean(bitmap)))
            }
            (true, None, _) => Ok(Value::Scalar(Scalar::Boolean(false))),
            (false, _, None) => Ok(Value::Scalar(Scalar::Boolean(true))),
        }
    }

    /// Evaluate a set-returning-function. Return multiple sets of results
    /// for each input row, along with the number of rows in each set.
    pub fn run_srf(
        &self,
        call: &FunctionCall,
        max_nums_per_row: &mut [usize],
    ) -> Result<Vec<(Value<AnyType>, usize)>> {
        let FunctionCall {
            span,
            id,
            function,
            args,
            return_type,
            generics,
            ..
        } = call;

        let FunctionEval::SRF { eval } = &function.eval else {
            unreachable!(
                "expr is not a set returning function: {}",
                function.signature.name
            );
        };

        assert!(return_type.as_tuple().is_some());
        let args = args
            .iter()
            .map(|expr| self.run(expr))
            .collect::<Result<Vec<_>>>()?;
        let mut ctx = EvalContext {
            generics,
            num_rows: self.data_block.num_rows(),
            validity: None,
            errors: None,
            func_ctx: self.func_ctx,
            suppress_error: false,
            strict_eval: true,
        };
        let result = (eval)(&args, &mut ctx, max_nums_per_row);
        if !ctx.suppress_error {
            EvalContext::render_error(
                *span,
                &ctx.errors,
                id.params(),
                &args,
                &function.signature.name,
                &Expr::from(call.clone()).sql_display(),
                None,
            )?;
        }
        assert_eq!(result.len(), self.data_block.num_rows());
        Ok(result)
    }

    fn run_array_reduce(
        &self,
        col_entries: Vec<BlockEntry>,
        column: &Column,
        expr: &Expr,
    ) -> Result<Scalar> {
        let col_type = column.data_type();
        if col_type.is_null() || column.len() < 1 {
            return Ok(Scalar::Null);
        }
        let mut arg0 = unsafe { column.index_unchecked(0).to_owned() };
        let mut eval_options = EvaluateOptions::default();
        for i in 1..column.len() {
            let arg1 = unsafe { column.index_unchecked(i).to_owned() };
            let mut entries = col_entries.clone();
            entries.extend_from_slice(&[
                BlockEntry::new_const_column(col_type.clone(), arg0.clone(), 1),
                BlockEntry::new_const_column(col_type.clone(), arg1, 1),
            ]);
            let block = DataBlock::new(entries, 1);
            let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
            let result = evaluator.run(expr)?;
            arg0 = self
                .run_cast(
                    None,
                    false,
                    expr.data_type(),
                    &col_type,
                    result,
                    None,
                    &|| expr.sql_display(),
                    &mut eval_options,
                )?
                .into_scalar()
                .unwrap();
        }
        Ok(arg0)
    }

    fn eval_lambda_block(&self, block: &DataBlock, expr: &Expr) -> Result<Column> {
        let evaluator = Evaluator::new(block, self.func_ctx, self.fn_registry);
        let result = evaluator.run(expr)?;
        Ok(result.convert_to_full_column(expr.data_type(), block.num_rows()))
    }

    fn build_filtered_offsets(offsets: &[u64], bitmap: &Bitmap) -> Vec<u64> {
        let mut new_offset = 0;
        let mut filtered_offsets = Vec::with_capacity(offsets.len());
        filtered_offsets.push(0);
        for offset in offsets.windows(2) {
            let start = offset[0] as usize;
            let len = (offset[1] - offset[0]) as usize;
            new_offset += (len - bitmap.null_count_range(start, len)) as u64;
            filtered_offsets.push(new_offset);
        }
        filtered_offsets
    }

    fn ensure_unique_map_keys(keys: &Column, offsets: Option<&[u64]>) -> Result<()> {
        let mut key_set = HashSet::new();

        if let Some(offsets) = offsets {
            for offset in offsets.windows(2) {
                key_set.clear();
                for index in offset[0] as usize..offset[1] as usize {
                    let key = unsafe { keys.index_unchecked(index) };
                    if !key_set.insert(key) {
                        return Err(ErrorCode::SemanticError(
                            "map keys have to be unique".to_string(),
                        ));
                    }
                }
            }
            return Ok(());
        }

        for index in 0..keys.len() {
            let key = unsafe { keys.index_unchecked(index) };
            if !key_set.insert(key) {
                return Err(ErrorCode::SemanticError(
                    "map keys have to be unique".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn wrap_lambda_column(column: Column, validity: Option<Bitmap>) -> Value<AnyType> {
        match validity {
            Some(validity) => Value::Column(column.wrap_nullable(Some(validity))),
            None => Value::Column(column),
        }
    }

    pub fn run_lambda(
        &self,
        func_name: &str,
        args: Vec<Value<AnyType>>,
        data_types: Vec<DataType>,
        lambda_expr: &RemoteExpr,
        return_type: &DataType,
        validity: Option<Bitmap>,
    ) -> Result<Value<AnyType>> {
        let expr = lambda_expr.as_expr(self.fn_registry);

        if func_name == "json_path_transform" {
            return self.run_json_path_transform(args, data_types, &expr, return_type, validity);
        }

        // array_reduce differs
        if func_name == "array_reduce" {
            let len = args.iter().find_map(|arg| match arg {
                Value::Column(col) => Some(col.len()),
                _ => None,
            });

            let lambda_idx = args.len() - 1;
            let mut builder = ColumnBuilder::with_capacity(return_type, len.unwrap_or(1));
            for idx in 0..(len.unwrap_or(1)) {
                let mut entries = Vec::with_capacity(args.len() - 1);
                for i in 0..lambda_idx {
                    let scalar = unsafe { args[i].index_unchecked(idx) };
                    let entry =
                        BlockEntry::new_const_column(data_types[i].clone(), scalar.to_owned(), 1);
                    entries.push(entry);
                }
                let scalar = unsafe { args[lambda_idx].index_unchecked(idx) };
                match scalar {
                    ScalarRef::Array(col) => {
                        let result = self.run_array_reduce(entries, &col, &expr)?;
                        builder.push(result.as_ref());
                    }
                    ScalarRef::Null => {
                        builder.push_default();
                    }
                    _ => unreachable!(),
                }
            }
            let res = match len {
                Some(_) => Value::Column(builder.build()),
                None => Value::Scalar(builder.build_scalar()),
            };
            return Ok(res);
        }

        // If there is only one column, we can extract the inner column and execute on all rows at once
        if args.len() == 1 && matches!(args[0], Value::Column(_)) {
            let keep_outer_validity = data_types[0].is_nullable_or_null();
            let (inner_col, offsets, validity) = match &args[0] {
                Value::Column(Column::Array(deref!(array_col))) => (
                    array_col.underlying_column(),
                    array_col.underlying_offsets(),
                    None,
                ),
                Value::Column(Column::Map(deref!(map_col))) => (
                    map_col.underlying_column(),
                    map_col.underlying_offsets(),
                    None,
                ),
                Value::Column(Column::Nullable(nullable_col)) => match nullable_col.column() {
                    Column::Array(deref!(array_col)) => (
                        array_col.underlying_column(),
                        array_col.underlying_offsets(),
                        keep_outer_validity.then(|| nullable_col.validity().clone()),
                    ),
                    Column::Map(deref!(map_col)) => (
                        map_col.underlying_column(),
                        map_col.underlying_offsets(),
                        keep_outer_validity.then(|| nullable_col.validity().clone()),
                    ),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            };

            if matches!(
                func_name,
                "map_filter" | "map_transform_keys" | "map_transform_values"
            ) {
                let (key_col, value_col) = match inner_col.clone() {
                    Column::Tuple(t) => (t[0].clone(), t[1].clone()),
                    _ => unreachable!(),
                };
                let block = DataBlock::new(
                    vec![key_col.clone().into(), value_col.clone().into()],
                    inner_col.len(),
                );
                let result_col = self.eval_lambda_block(&block, &expr)?;

                let map_col = match func_name {
                    "map_filter" => {
                        let result_col = result_col.remove_nullable();
                        let bitmap = result_col.as_boolean().unwrap();
                        let (filtered_key_col, filtered_value_col) =
                            (key_col.filter(bitmap), value_col.filter(bitmap));
                        let inner_column = Column::Tuple(vec![
                            filtered_key_col.clone(),
                            filtered_value_col.clone(),
                        ]);
                        let offsets =
                            Self::build_filtered_offsets(offsets.as_slice(), bitmap).into();
                        Column::Map(Box::new(ArrayColumn::new(inner_column, offsets)))
                    }
                    "map_transform_keys" => {
                        Self::ensure_unique_map_keys(&result_col, Some(offsets.as_slice()))?;
                        let inner_column = Column::Tuple(vec![result_col, value_col]);
                        Column::Map(Box::new(ArrayColumn::new(inner_column, offsets)))
                    }
                    "map_transform_values" => {
                        let inner_column = Column::Tuple(vec![key_col, result_col]);
                        Column::Map(Box::new(ArrayColumn::new(inner_column, offsets)))
                    }
                    _ => unreachable!(),
                };
                return Ok(Self::wrap_lambda_column(map_col, validity));
            } else {
                let entry = inner_col.clone().into();
                let block = DataBlock::new(vec![entry], inner_col.len());
                let result_col = self.eval_lambda_block(&block, &expr)?;

                let array_col = if func_name == "array_filter" {
                    let result_col = result_col.remove_nullable();
                    let bitmap = result_col.as_boolean().unwrap();
                    let filtered_inner_col = inner_col.filter(bitmap);

                    Column::Array(Box::new(ArrayColumn::new(
                        filtered_inner_col,
                        Self::build_filtered_offsets(offsets.as_slice(), bitmap).into(),
                    )))
                } else {
                    Column::Array(Box::new(ArrayColumn::new(result_col, offsets)))
                };
                return Ok(Self::wrap_lambda_column(array_col, validity));
            }
        }

        let len = args.iter().find_map(|arg| match arg {
            Value::Column(col) => Some(col.len()),
            _ => None,
        });
        let lambda_idx = args.len() - 1;
        let mut builder = ColumnBuilder::with_capacity(return_type, len.unwrap_or(1));
        for idx in 0..(len.unwrap_or(1)) {
            let scalars = (0..lambda_idx)
                .map(|i| {
                    let scalar = unsafe { args[i].index_unchecked(idx) };

                    (scalar.to_owned(), data_types[i].clone())
                })
                .collect::<Vec<_>>();
            let scalar = unsafe { args[lambda_idx].index_unchecked(idx) };
            match scalar {
                ScalarRef::Array(col) => {
                    // add lambda array scalar value as a column
                    let col_len = col.len();
                    let entries = scalars
                        .into_iter()
                        .map(|(scalar, data_type)| {
                            BlockEntry::new_const_column(data_type, scalar, col_len)
                        })
                        .chain(Some(col.into()))
                        .collect();

                    let block = DataBlock::new(entries, col_len);
                    let result_col = self.eval_lambda_block(&block, &expr)?;

                    let val = if func_name == "array_filter" {
                        let result_col = result_col.remove_nullable();
                        let bitmap = result_col.as_boolean().unwrap();

                        let src_entry = block.get_by_offset(lambda_idx);
                        let src_col = src_entry.as_column().unwrap();
                        let filtered_col = src_col.filter(bitmap);
                        Scalar::Array(filtered_col)
                    } else {
                        Scalar::Array(result_col)
                    };
                    builder.push(val.as_ref());
                }
                ScalarRef::Map(col) => {
                    let col_len = col.len();
                    let (key_col, value_col) = match col {
                        Column::Tuple(t) => (t[0].clone(), t[1].clone()),
                        _ => unreachable!(),
                    };

                    let entries = scalars
                        .into_iter()
                        .map(|(scalar, data_type)| {
                            BlockEntry::new_const_column(data_type, scalar, col_len)
                        })
                        .chain([key_col.clone().into(), value_col.clone().into()])
                        .collect();

                    let block = DataBlock::new(entries, col_len);
                    let result_col = self.eval_lambda_block(&block, &expr)?;
                    let val = match func_name {
                        "map_filter" => {
                            let result_col = result_col.remove_nullable();
                            let bitmap = result_col.as_boolean().unwrap();

                            let (key_entry, value_entry) = (
                                block.get_by_offset(lambda_idx),
                                block.get_by_offset(lambda_idx + 1),
                            );
                            let (key_col, value_col) = (
                                key_entry.as_column().unwrap(),
                                value_entry.as_column().unwrap(),
                            );
                            let (filtered_key_col, filtered_value_col) =
                                (key_col.filter(bitmap), value_col.filter(bitmap));
                            Scalar::Map(Column::Tuple(vec![
                                filtered_key_col.clone(),
                                filtered_value_col.clone(),
                            ]))
                        }
                        "map_transform_keys" => {
                            Self::ensure_unique_map_keys(&result_col, None)?;
                            Scalar::Map(Column::Tuple(vec![result_col, value_col]))
                        }
                        "map_transform_values" => {
                            Scalar::Map(Column::Tuple(vec![key_col, result_col]))
                        }
                        _ => unreachable!(),
                    };
                    builder.push(val.as_ref());
                }
                ScalarRef::Null => {
                    builder.push_default();
                }
                _ => unreachable!(),
            }
        }
        let res = match len {
            Some(_) => Value::Column(builder.build()),
            None => Value::Scalar(builder.build_scalar()),
        };
        Ok(res)
    }

    /// Evaluates `json_path_transform(<json>, <path>, <lambda>)`.
    ///
    /// Arguments are laid out as `[json, path, captures...]` and the lambda
    /// body was cast to `Nullable(Variant)` during type checking. Rows
    /// outside `validity` (e.g. masked out of an IF/CASE branch) are passed
    /// through unchanged: their paths are never parsed or selected and their
    /// matches never reach the lambda, so they cannot fail the query.
    fn run_json_path_transform(
        &self,
        args: Vec<Value<AnyType>>,
        data_types: Vec<DataType>,
        expr: &Expr,
        return_type: &DataType,
        validity: Option<Bitmap>,
    ) -> Result<Value<AnyType>> {
        debug_assert!(args.len() >= 2);
        let json_idx = 0;
        let path_idx = 1;

        let len = args.iter().find_map(|arg| match arg {
            Value::Column(col) => Some(col.len()),
            _ => None,
        });
        let num_rows = len.unwrap_or(1);
        debug_assert!(len.is_none() || validity.as_ref().is_none_or(|v| v.len() == num_rows));

        // The branch is fully masked out: the caller discards every row, so
        // pass the json argument through without locating any match.
        if let Some(v) = &validity {
            if v.null_count() == v.len() {
                let json = args.into_iter().next().unwrap();
                return Ok(match json {
                    Value::Column(col) if col.data_type() != *return_type => {
                        debug_assert!(return_type.is_nullable());
                        Value::Column(col.wrap_nullable(Some(Bitmap::new_constant(true, num_rows))))
                    }
                    json => json,
                });
            }
        }

        // `None` means every row is active. A partially masked scalar row is
        // a broadcast picked by at least one row, so it stays active.
        let validity = match len {
            Some(_) => validity.filter(|v| v.null_count() > 0),
            None => None,
        };

        fn parse_path(path_str: &str) -> Result<JsonPath<'_>> {
            parse_json_path(path_str.as_bytes())
                .map_err(|_| ErrorCode::BadArguments(format!("Invalid JSON Path '{path_str}'")))
        }

        // A constant path is parsed lazily on the first row that needs it, so
        // an invalid constant path does not fail queries whose rows never
        // evaluate it, matching the per-row handling of a non-constant path.
        let const_path_str = match &args[path_idx] {
            Value::Scalar(Scalar::String(path_str)) => Some(path_str.as_str()),
            _ => None,
        };
        let mut const_path: Option<JsonPath> = None;

        enum JsonTransformRow {
            Null,
            Unchanged,
            Matched {
                locations: Vec<Vec<PathStep>>,
                count: usize,
            },
        }

        // Phase 1: locate matched values in every row.
        let mut rows = Vec::with_capacity(num_rows);
        let mut matched_values = VariantType::create_builder(num_rows, &[]);
        let mut total_matches = 0usize;
        for idx in 0..num_rows {
            let json_scalar = unsafe { args[json_idx].index_unchecked(idx) };
            let path_scalar = unsafe { args[path_idx].index_unchecked(idx) };
            let (data, path_str) = match (json_scalar, path_scalar) {
                (ScalarRef::Null, _) | (_, ScalarRef::Null) => {
                    rows.push(JsonTransformRow::Null);
                    continue;
                }
                (ScalarRef::Variant(data), ScalarRef::String(path_str)) => (data, path_str),
                _ => {
                    return Err(ErrorCode::Internal(
                        "json_path_transform: unexpected argument types",
                    ));
                }
            };
            if let Some(validity) = &validity {
                if !unsafe { validity.get_bit_unchecked(idx) } {
                    rows.push(JsonTransformRow::Unchanged);
                    continue;
                }
            }
            if let Some(path_str) = const_path_str
                && const_path.is_none()
            {
                const_path = Some(parse_path(path_str)?);
            }
            let matches = match &const_path {
                Some(json_path) => select_locations(data, json_path)?,
                None => select_locations(data, &parse_path(path_str)?)?,
            };
            if matches.is_empty() {
                rows.push(JsonTransformRow::Unchanged);
            } else {
                let count = matches.len();
                let mut locations = Vec::with_capacity(count);
                for m in matches {
                    locations.push(m.location);
                    matched_values.put_slice(&m.value);
                    matched_values.commit_row();
                }
                total_matches += count;
                rows.push(JsonTransformRow::Matched { locations, count });
            }
        }

        // Phase 2: evaluate the lambda once over all matched values.
        let result_col = if total_matches > 0 {
            let mut entries = Vec::with_capacity(args.len() - 1);
            for i in 2..args.len() {
                let entry = match &args[i] {
                    Value::Scalar(scalar) => BlockEntry::new_const_column(
                        data_types[i].clone(),
                        scalar.clone(),
                        total_matches,
                    ),
                    Value::Column(_) => {
                        // Repeat the captured value of each row once per match.
                        let mut builder =
                            ColumnBuilder::with_capacity(&data_types[i], total_matches);
                        for (row, transform_row) in rows.iter().enumerate() {
                            if let JsonTransformRow::Matched { count, .. } = transform_row {
                                let scalar = unsafe { args[i].index_unchecked(row) };
                                for _ in 0..*count {
                                    builder.push(scalar.clone());
                                }
                            }
                        }
                        builder.build().into()
                    }
                };
                entries.push(entry);
            }
            let elem_col = Column::Variant(matched_values.build()).wrap_nullable(None);
            entries.push(elem_col.into());
            let block = DataBlock::new(entries, total_matches);
            Some(self.eval_lambda_block(&block, expr)?)
        } else {
            None
        };

        // Phase 3: rebuild each row with its matched values replaced.
        let mut builder = ColumnBuilder::with_capacity(return_type, num_rows);
        let mut cursor = 0usize;
        for (row, transform_row) in rows.iter().enumerate() {
            match transform_row {
                JsonTransformRow::Null => {
                    debug_assert!(return_type.is_nullable());
                    builder.push_default();
                }
                JsonTransformRow::Unchanged => {
                    let json_scalar = unsafe { args[json_idx].index_unchecked(row) };
                    builder.push(json_scalar);
                }
                JsonTransformRow::Matched { locations, count } => {
                    let result_col = result_col.as_ref().unwrap();
                    let mut replacements: Vec<Option<&[u8]>> = Vec::with_capacity(*count);
                    for i in cursor..cursor + count {
                        match unsafe { result_col.index_unchecked(i) } {
                            ScalarRef::Null => replacements.push(None),
                            ScalarRef::Variant(data) => replacements.push(Some(data)),
                            _ => {
                                return Err(ErrorCode::Internal(
                                    "json_path_transform: lambda result must be variant",
                                ));
                            }
                        }
                    }
                    cursor += count;
                    let json_scalar = unsafe { args[json_idx].index_unchecked(row) };
                    let ScalarRef::Variant(data) = json_scalar else {
                        unreachable!()
                    };
                    let new_data = replace_at_locations(data, locations, &replacements)?;
                    builder.push(ScalarRef::Variant(&new_data));
                }
            }
        }
        match len {
            Some(_) => Ok(Value::Column(builder.build())),
            None => Ok(Value::Scalar(builder.build_scalar())),
        }
    }

    pub fn get_children(
        &self,
        args: &[Expr],
        options: &mut EvaluateOptions,
    ) -> Result<Vec<(Value<AnyType>, DataType)>> {
        let children = args
            .iter()
            .map(|expr| self.get_select_child(expr, options))
            .collect::<Result<Vec<_>>>()?;
        assert!(
            children
                .iter()
                .filter_map(|val| match &val.0 {
                    Value::Column(col) => Some(col.len()),
                    Value::Scalar(_) => None,
                })
                .all_equal()
        );
        Ok(children)
    }

    pub fn remove_generics_data_type(
        &self,
        generics: &[DataType],
        data_type: &DataType,
    ) -> DataType {
        match data_type {
            DataType::Generic(index) => generics[*index].clone(),
            DataType::Nullable(deref!(DataType::Generic(index))) => {
                DataType::Nullable(Box::new(generics[*index].clone()))
            }
            _ => data_type.clone(),
        }
    }

    #[recursive::recursive]
    pub fn get_select_child(
        &self,
        expr: &Expr,
        options: &mut EvaluateOptions,
    ) -> Result<(Value<AnyType>, DataType)> {
        #[cfg(debug_assertions)]
        self.check_expr(expr);

        let result = match expr {
            Expr::Constant(Constant { scalar, .. }) => Ok((
                Value::Scalar(scalar.clone()),
                scalar.as_ref().infer_data_type(),
            )),
            Expr::ColumnRef(ColumnRef { id, .. }) => {
                let entry = self.data_block.get_by_offset(*id);
                Ok((entry.decoded_value(), entry.data_type()))
            }
            Expr::Cast(Cast {
                span,
                is_try,
                expr,
                dest_type,
            }) => {
                let value = self.get_select_child(expr, options)?.0;
                let src_type = expr.data_type();
                let value = self.run_cast(
                    *span,
                    *is_try,
                    src_type,
                    dest_type,
                    value,
                    None,
                    &|| expr.sql_display(),
                    options,
                )?;
                Ok((value, dest_type.clone()))
            }
            Expr::FunctionCall(FunctionCall {
                function,
                args,
                generics,
                ..
            }) if function.signature.name == "if" => {
                let return_type =
                    self.remove_generics_data_type(generics, &function.signature.return_type);
                Ok((self.eval_if(args, generics, None, options)?, return_type))
            }

            Expr::FunctionCall(FunctionCall {
                function,
                args,
                generics,
                ..
            }) if function.signature.name == "and_filters" => {
                let return_type =
                    self.remove_generics_data_type(generics, &function.signature.return_type);
                Ok((self.eval_and_filters(args, None, options)?, return_type))
            }

            Expr::FunctionCall(FunctionCall {
                function,
                args,
                generics,
                ..
            }) if function.signature.name == "or_filters" => {
                let return_type =
                    self.remove_generics_data_type(generics, &function.signature.return_type);
                Ok((self.eval_or_filters(args, None, options)?, return_type))
            }

            Expr::FunctionCall(FunctionCall {
                span,
                id,
                function,
                args,
                generics,
                ..
            }) => {
                let child_suppress_error = function.signature.name == "is_not_error";
                // Suppression is sticky downward: once a boundary function
                // opts in (is_not_error), nested evaluations stay suppressed
                // so row errors bubble up to the boundary as data.
                let mut child_option =
                    options.with_suppress_error(options.suppress_error || child_suppress_error);
                let args = args
                    .iter()
                    .map(|expr| self.get_select_child(expr, &mut child_option))
                    .collect::<Result<Vec<_>>>()?;
                assert!(
                    args.iter()
                        .filter_map(|val| match &val.0 {
                            Value::Column(col) => Some(col.len()),
                            Value::Scalar(_) => None,
                        })
                        .all_equal()
                );

                let args = args.into_iter().map(|(val, _)| val).collect::<Vec<_>>();

                let errors = if !child_suppress_error {
                    None
                } else {
                    child_option.errors.take()
                };
                let mut ctx = EvalContext {
                    generics,
                    num_rows: self.data_block.num_rows(),
                    validity: None,
                    errors,
                    func_ctx: self.func_ctx,
                    suppress_error: options.suppress_error,
                    strict_eval: options.strict_eval,
                };
                let (_, _, eval) = function.eval.as_scalar().unwrap();
                self.record_eval(function);
                let result = eval.eval(&args, &mut ctx);

                // Same all-scalar rule as `eval_common_call`: an error from a
                // scalar evaluation applies to every row. This path carries no
                // branch validity mask, so expand over the whole block.
                if args.iter().all(|arg| matches!(arg, Value::Scalar(_))) {
                    expand_scalar_error(&mut ctx.errors, None, ctx.num_rows);
                }

                // Non-boundary calls are transparent to suppression: errors
                // from nested evaluations keep bubbling up so a boundary
                // function (is_not_error) observes errors from its whole
                // subtree, not only from its direct children.
                if !child_suppress_error {
                    merge_eval_errors(&mut ctx.errors, child_option.errors.take(), ctx.num_rows);
                }

                // inject errors into options, parent will handle it
                if options.suppress_error {
                    merge_eval_errors(&mut options.errors, ctx.errors.take(), ctx.num_rows);
                } else {
                    EvalContext::render_error(
                        *span,
                        &ctx.errors,
                        id.params(),
                        &args,
                        &function.signature.name,
                        &expr.sql_display(),
                        options.selection,
                    )?
                }

                let return_type =
                    self.remove_generics_data_type(generics, &function.signature.return_type);
                Ok((result, return_type))
            }
            Expr::LambdaFunctionCall(LambdaFunctionCall {
                name,
                args,
                lambda_expr,
                return_type,
                ..
            }) => {
                let data_types = args.iter().map(|arg| arg.data_type().clone()).collect();
                let args = args
                    .iter()
                    .map(|expr| {
                        self.partial_run(expr, None, &mut EvaluateOptions::default(), false)
                    })
                    .collect::<Result<Vec<_>>>()?;
                assert!(
                    args.iter()
                        .filter_map(|val| match val {
                            Value::Column(col) => Some(col.len()),
                            Value::Scalar(_) => None,
                        })
                        .all_equal()
                );

                Ok((
                    self.run_lambda(name, args, data_types, lambda_expr, return_type, None)?,
                    return_type.clone(),
                ))
            }
        };

        result
    }
}

struct CheckStrictValue;

impl ValueVisitor for CheckStrictValue {
    type Error = ();

    fn visit_scalar(&mut self, scalar: Scalar) -> std::result::Result<(), ()> {
        match scalar {
            Scalar::Decimal(scalar) => {
                let value = Value::Scalar(Scalar::Decimal(scalar));
                if Self::is_strict_decimal(&value) {
                    Ok(())
                } else {
                    Err(())
                }
            }
            Scalar::Array(column) => self.visit_column(column),
            Scalar::Map(column) => self.visit_column(column),
            _ => Ok(()),
        }
    }

    fn visit_any_decimal(&mut self, column: DecimalColumn) -> std::result::Result<(), ()> {
        let value = Value::Column(Column::Decimal(column));
        if Self::is_strict_decimal(&value) {
            Ok(())
        } else {
            Err(())
        }
    }

    fn visit_nullable(
        &mut self,
        column: Box<NullableColumn<AnyType>>,
    ) -> std::result::Result<(), ()> {
        self.visit_column(column.destructure().0)
    }

    fn visit_typed_column<T: ValueType>(
        &mut self,
        _: T::Column,
        _: &DataType,
    ) -> std::result::Result<(), ()> {
        Ok(())
    }
}

impl CheckStrictValue {
    fn is_strict_decimal(value: &Value<AnyType>) -> bool {
        DecimalDataType::from_value(value).unwrap().0.is_strict()
    }
}
