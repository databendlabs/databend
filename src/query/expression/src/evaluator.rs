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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::ops::Not;

use databend_common_ast::Span;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;
use jsonb::RawJsonb;
use jsonb::Value as JsonbValue;

use crate::BlockEntry;
use crate::FunctionContext;
use crate::FunctionEval;
use crate::FunctionRegistry;
use crate::RemoteExpr;
use crate::ScalarRef;
use crate::block::DataBlock;
use crate::expr::*;
use crate::expression::Expr;
use crate::function::EvalContext;
use crate::type_check::check_function;
use crate::type_check::format_function_argument_mismatch_hint;
use crate::type_check::get_simple_cast_function;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DecimalColumn;
use crate::types::DecimalDataType;
use crate::types::F32;
use crate::types::NullableType;
use crate::types::NumberScalar;
use crate::types::ReturnType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::types::VectorDataType;
use crate::types::VectorScalar;
use crate::types::any::AnyType;
use crate::types::array::ArrayColumn;
use crate::types::boolean;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumnBuilder;
use crate::utils::filter_helper::FilterHelpers;
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

    #[cfg(debug_assertions)]
    fn check_expr(&self, expr: &Expr) {
        let column_refs = expr.column_refs();
        for (index, data_type) in column_refs.iter() {
            let column = self.data_block.get_by_offset(*index);
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
        self.partial_run(expr, None, &mut EvaluateOptions::default())
            .map_err(|err| Self::map_err(err, expr))
    }

    fn map_err(err: ErrorCode, expr: &Expr) -> ErrorCode {
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
    pub fn partial_run(
        &self,
        expr: &Expr,
        validity: Option<Bitmap>,
        options: &mut EvaluateOptions,
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
                let value = self.partial_run(inner, validity.clone(), options)?;
                let src_type = inner.data_type();
                if *is_try {
                    self.run_try_cast(*span, src_type, dest_type, value, &|| expr.sql_display())?
                } else {
                    self.run_cast(
                        *span,
                        src_type,
                        dest_type,
                        value,
                        validity,
                        &|| expr.sql_display(),
                        options,
                    )?
                }
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
                    .map(|expr| self.partial_run(expr, validity.clone(), options))
                    .collect::<Result<Vec<_>>>()?;
                assert!(
                    args.iter()
                        .filter_map(|val| match val {
                            Value::Column(col) => Some(col.len()),
                            Value::Scalar(_) => None,
                        })
                        .all_equal()
                );

                self.run_lambda(name, args, data_types, lambda_expr, return_type)?
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
            Value::Column(col) => assert_eq!(&col.data_type(), expr.data_type()),
        }

        if !expr.is_column_ref() && !expr.is_constant() && options.strict_eval {
            let mut check = CheckStrictValue;
            assert!(
                check.visit_value(result.clone()).is_ok(),
                "strict check fail on expr: {expr}",
            )
        }

        Ok(result)
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
        let mut child_option = options.with_suppress_error(child_suppress_error);
        if child_option.strict_eval && call.generics.is_empty() {
            child_option.strict_eval = false;
        }

        let mut args_value = Vec::with_capacity(args.len());
        for expr in args {
            match self.partial_run(expr, validity.clone(), &mut child_option) {
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
        let args = args_value;

        assert!(
            args.iter()
                .filter_map(|val| match val {
                    Value::Column(col) => Some(col.len()),
                    Value::Scalar(_) => None,
                })
                .all_equal()
        );

        let errors = if child_suppress_error {
            child_option.errors.take()
        } else {
            None
        };
        let mut ctx = EvalContext {
            generics,
            num_rows: self.data_block.num_rows(),
            validity,
            errors,
            func_ctx: self.func_ctx,
            suppress_error: options.suppress_error,
            strict_eval: options.strict_eval,
        };

        let (_, _, eval) = function.eval.as_scalar().unwrap();
        let result = eval.eval(&args, &mut ctx);

        if options.suppress_error {
            // inject errors into options, parent will handle it
            options.errors = ctx.errors.take();
        } else {
            EvalContext::render_error(
                *span,
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

    pub fn run_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        value: Value<AnyType>,
        validity: Option<Bitmap>,
        expr_display: &impl Fn() -> String,
        options: &mut EvaluateOptions,
    ) -> Result<Value<AnyType>> {
        if src_type == dest_type {
            return Ok(value);
        }

        if let Some(cast_fn) = get_simple_cast_function(false, src_type, dest_type) {
            if let Some(new_value) = self.run_simple_cast(
                span,
                src_type,
                dest_type,
                value.clone(),
                &cast_fn,
                validity.clone(),
                expr_display,
                options,
            )? {
                return Ok(new_value);
            }
        }

        let result = match (src_type, dest_type) {
            (DataType::Null, DataType::Nullable(_)) => match value {
                Value::Scalar(Scalar::Null) => Ok(Value::Scalar(Scalar::Null)),
                Value::Column(Column::Null { len }) => {
                    let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                    for _ in 0..len {
                        builder.push_default();
                    }
                    Ok(Value::Column(builder.build()))
                }
                _ => unreachable!(),
            },
            (
                DataType::Nullable(box DataType::Variant) | DataType::Variant,
                DataType::Nullable(box DataType::Boolean)
                | DataType::Nullable(box DataType::Number(_))
                | DataType::Nullable(box DataType::Decimal(_))
                | DataType::Nullable(box DataType::String)
                | DataType::Nullable(box DataType::Binary)
                | DataType::Nullable(box DataType::Date)
                | DataType::Nullable(box DataType::Timestamp)
                | DataType::Nullable(box DataType::TimestampTz)
                | DataType::Nullable(box DataType::Interval),
            ) => {
                // allow cast variant to nullable types.
                let inner_dest_ty = dest_type.remove_nullable();
                let cast_fn = if inner_dest_ty.is_decimal() {
                    "to_decimal".to_owned()
                } else {
                    format!("to_{}", inner_dest_ty.to_string().to_lowercase())
                };
                if let Some(new_value) = self.run_simple_cast(
                    span,
                    src_type,
                    dest_type,
                    value.clone(),
                    &cast_fn,
                    validity.clone(),
                    expr_display,
                    options,
                )? {
                    Ok(new_value)
                } else {
                    Err(ErrorCode::BadArguments(format!(
                        "unable to cast type `{src_type}` to type `{dest_type}`"
                    ))
                    .set_span(span))
                }
            }
            (
                DataType::Nullable(box DataType::Variant) | DataType::Variant,
                DataType::Boolean
                | DataType::Number(_)
                | DataType::Decimal(_)
                | DataType::String
                | DataType::Binary
                | DataType::Date
                | DataType::Timestamp
                | DataType::TimestampTz
                | DataType::Interval,
            ) => {
                // allow cast variant to not null types.
                let cast_fn = if dest_type.is_decimal() {
                    "to_decimal".to_owned()
                } else {
                    format!("to_{}", dest_type.to_string().to_lowercase())
                };
                if let Some(new_value) = self.run_simple_cast(
                    span,
                    src_type,
                    &dest_type.wrap_nullable(),
                    value.clone(),
                    &cast_fn,
                    validity.clone(),
                    expr_display,
                    options,
                )? {
                    let (new_value, has_null) = new_value.remove_nullable();
                    if has_null {
                        return Err(ErrorCode::BadArguments(format!(
                            "unable to cast type `{src_type}` to type `{dest_type}`, result has null values"
                        ))
                        .set_span(span));
                    }
                    Ok(new_value)
                } else {
                    Err(ErrorCode::BadArguments(format!(
                        "unable to cast type `{src_type}` to type `{dest_type}`"
                    ))
                    .set_span(span))
                }
            }
            (DataType::Nullable(inner_src_ty), DataType::Nullable(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::Null) => Ok(Value::Scalar(Scalar::Null)),
                Value::Scalar(_) => self.run_cast(
                    span,
                    inner_src_ty,
                    inner_dest_ty,
                    value,
                    validity,
                    expr_display,
                    options,
                ),
                Value::Column(Column::Nullable(col)) => {
                    let validity = validity
                        .map(|validity| (&validity) & (&col.validity))
                        .unwrap_or_else(|| col.validity.clone());
                    let column = self
                        .run_cast(
                            span,
                            inner_src_ty,
                            inner_dest_ty,
                            Value::Column(col.column),
                            Some(validity.clone()),
                            expr_display,
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(NullableColumn::new_column(column, validity)))
                }
                other => unreachable!("source: {}", other),
            },
            (DataType::Nullable(inner_src_ty), _) => match value {
                Value::Scalar(Scalar::Null) => {
                    let has_valid = validity
                        .map(|validity| validity.null_count() < validity.len())
                        .unwrap_or(true);
                    if has_valid {
                        Err(ErrorCode::BadArguments(format!(
                            "unable to cast type `NULL` to type `{dest_type}`"
                        ))
                        .set_span(span))
                    } else {
                        Ok(Value::Scalar(Scalar::default_value(dest_type)))
                    }
                }
                Value::Scalar(_) => self.run_cast(
                    span,
                    inner_src_ty,
                    dest_type,
                    value,
                    validity,
                    expr_display,
                    options,
                ),
                Value::Column(Column::Nullable(col)) => {
                    let has_valid_nulls = validity
                        .as_ref()
                        .map(|validity| {
                            (validity & (&col.validity)).null_count() > validity.null_count()
                        })
                        .unwrap_or_else(|| col.validity.null_count() > 0);
                    if has_valid_nulls {
                        return Err(ErrorCode::Internal(format!(
                            "unable to cast `NULL` to type `{dest_type}`"
                        ))
                        .set_span(span));
                    }
                    let column = self
                        .run_cast(
                            span,
                            inner_src_ty,
                            dest_type,
                            Value::Column(col.column),
                            validity,
                            expr_display,
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(column))
                }
                other => unreachable!("source: {}", other),
            },
            (_, DataType::Nullable(inner_dest_ty)) => match value {
                Value::Scalar(scalar) => self.run_cast(
                    span,
                    src_type,
                    inner_dest_ty,
                    Value::Scalar(scalar),
                    validity,
                    expr_display,
                    options,
                ),
                Value::Column(col) => {
                    let column = self
                        .run_cast(
                            span,
                            src_type,
                            inner_dest_ty,
                            Value::Column(col),
                            validity,
                            expr_display,
                            options,
                        )?
                        .into_column()
                        .unwrap();

                    let validity = Bitmap::new_constant(true, column.len());
                    Ok(Value::Column(NullableColumn::new_column(column, validity)))
                }
            },

            (DataType::EmptyArray, DataType::Array(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::EmptyArray) => {
                    let new_column = ColumnBuilder::with_capacity(inner_dest_ty, 0).build();
                    Ok(Value::Scalar(Scalar::Array(new_column)))
                }
                Value::Column(Column::EmptyArray { len }) => {
                    let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                    for _ in 0..len {
                        builder.push_default();
                    }
                    Ok(Value::Column(builder.build()))
                }
                other => unreachable!("source: {}", other),
            },
            (DataType::Array(inner_src_ty), DataType::Array(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::Array(array)) => {
                    let validity = validity.map(|validity| {
                        Bitmap::new_constant(validity.null_count() != validity.len(), array.len())
                    });
                    let new_array = self
                        .run_cast(
                            span,
                            inner_src_ty,
                            inner_dest_ty,
                            Value::Column(array),
                            validity,
                            expr_display,
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Scalar(Scalar::Array(new_array)))
                }
                Value::Column(Column::Array(col)) => {
                    let validity = validity.map(|validity| {
                        let mut inner_validity = MutableBitmap::with_capacity(col.len());
                        for (index, offsets) in col.offsets().windows(2).enumerate() {
                            inner_validity.extend_constant(
                                (offsets[1] - offsets[0]) as usize,
                                validity.get_bit(index),
                            );
                        }
                        inner_validity.into()
                    });
                    let new_col = self
                        .run_cast(
                            span,
                            inner_src_ty,
                            inner_dest_ty,
                            Value::Column(col.underlying_column()),
                            validity,
                            expr_display,
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(Column::Array(Box::new(ArrayColumn::new(
                        new_col,
                        col.underlying_offsets(),
                    )))))
                }
                other => unreachable!("source: {}", other),
            },
            (DataType::Variant, DataType::Array(inner_dest_ty)) => {
                let empty_vec = vec![];
                let mut temp_array: jsonb::Value;
                match value {
                    Value::Scalar(Scalar::Variant(x)) => {
                        let array = if validity.as_ref().map(|v| v.get_bit(0)).unwrap_or(true) {
                            temp_array = jsonb::from_slice(&x).map_err(|e| {
                                ErrorCode::BadArguments(format!(
                                    "Expect to be valid json, got err: {e:?}"
                                ))
                            })?;
                            temp_array.as_array().unwrap_or(&empty_vec)
                        } else {
                            &empty_vec
                        };
                        let validity = None;
                        let column = Column::Variant(VariantType::create_column_from_variants(
                            array.as_slice(),
                        ));
                        let new_array = self
                            .run_cast(
                                span,
                                &DataType::Variant,
                                inner_dest_ty,
                                Value::Column(column),
                                validity,
                                expr_display,
                                options,
                            )?
                            .into_column()
                            .unwrap();
                        Ok(Value::Scalar(Scalar::Array(new_array)))
                    }
                    Value::Column(Column::Variant(col)) => {
                        let mut offsets = Vec::with_capacity(col.len() + 1);
                        offsets.push(0);
                        let mut builder = VariantType::create_builder(col.len(), &[]);
                        for (idx, x) in col.iter().enumerate() {
                            let array = if validity.as_ref().map(|v| v.get_bit(idx)).unwrap_or(true)
                            {
                                temp_array = jsonb::from_slice(x).map_err(|e| {
                                    ErrorCode::BadArguments(format!(
                                        "Expect to be valid json, got err: {e:?}"
                                    ))
                                })?;
                                temp_array.as_array().unwrap_or(&empty_vec)
                            } else {
                                &empty_vec
                            };
                            for v in array.iter() {
                                v.write_to_vec(&mut builder.data);
                                builder.commit_row();
                            }
                            offsets.push(builder.len() as u64);
                        }
                        let value_col = Column::Variant(builder.build());

                        let new_col = self
                            .run_cast(
                                span,
                                &DataType::Variant,
                                inner_dest_ty,
                                Value::Column(value_col),
                                None,
                                expr_display,
                                options,
                            )?
                            .into_column()
                            .unwrap();

                        Ok(Value::Column(Column::Array(Box::new(ArrayColumn::new(
                            new_col,
                            offsets.into(),
                        )))))
                    }
                    other => unreachable!("source: {}", other),
                }
            }
            (DataType::Variant, DataType::Map(box DataType::Tuple(fields_dest_ty)))
                if fields_dest_ty.len() == 2 && fields_dest_ty[0] == DataType::String =>
            {
                let empty_obj = BTreeMap::new();
                let mut temp_obj: jsonb::Value;
                match value {
                    Value::Scalar(Scalar::Variant(x)) => {
                        let obj = if validity.as_ref().map(|v| v.get_bit(0)).unwrap_or(true) {
                            temp_obj = jsonb::from_slice(&x).map_err(|e| {
                                ErrorCode::BadArguments(format!(
                                    "Expect to be valid json, got err: {e:?}"
                                ))
                            })?;
                            temp_obj.as_object().unwrap_or(&empty_obj)
                        } else {
                            &empty_obj
                        };
                        let validity = None;

                        let mut key_builder = StringColumnBuilder::with_capacity(obj.len());
                        for k in obj.keys() {
                            key_builder.put_and_commit(k.as_str());
                        }
                        let key_column = Column::String(key_builder.build());

                        let values: Vec<_> = obj.values().cloned().collect();
                        let value_column = Column::Variant(
                            VariantType::create_column_from_variants(values.as_slice()),
                        );

                        let new_value_column = self
                            .run_cast(
                                span,
                                &DataType::Variant,
                                &fields_dest_ty[1],
                                Value::Column(value_column),
                                validity,
                                expr_display,
                                options,
                            )?
                            .into_column()
                            .unwrap();
                        Ok(Value::Scalar(Scalar::Map(Column::Tuple(vec![
                            key_column,
                            new_value_column,
                        ]))))
                    }
                    Value::Column(Column::Variant(col)) => {
                        let mut offsets = Vec::with_capacity(col.len() + 1);
                        offsets.push(0);
                        let mut key_builder = StringType::create_builder(col.len(), &[]);
                        let mut value_builder = VariantType::create_builder(col.len(), &[]);

                        for (idx, x) in col.iter().enumerate() {
                            let obj = if validity.as_ref().map(|v| v.get_bit(idx)).unwrap_or(true) {
                                temp_obj = jsonb::from_slice(x).map_err(|e| {
                                    ErrorCode::BadArguments(format!(
                                        "Expect to be valid json, got err: {e:?}"
                                    ))
                                })?;
                                temp_obj.as_object().unwrap_or(&empty_obj)
                            } else {
                                &empty_obj
                            };

                            for (k, v) in obj.iter() {
                                key_builder.put_and_commit(k.as_str());
                                v.write_to_vec(&mut value_builder.data);
                                value_builder.commit_row();
                            }
                            offsets.push(key_builder.len() as u64);
                        }

                        let key_col = Column::String(key_builder.build());
                        let value_col = Column::Variant(value_builder.build());

                        let new_value_col = self
                            .run_cast(
                                span,
                                &DataType::Variant,
                                &fields_dest_ty[1],
                                Value::Column(value_col),
                                None,
                                expr_display,
                                options,
                            )?
                            .into_column()
                            .unwrap();

                        let kv_col = Column::Tuple(vec![key_col, new_value_col]);
                        Ok(Value::Column(Column::Map(Box::new(ArrayColumn::new(
                            kv_col,
                            offsets.into(),
                        )))))
                    }
                    other => unreachable!("source: {}", other),
                }
            }
            (DataType::EmptyMap, DataType::Map(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::EmptyMap) => {
                    let new_column = ColumnBuilder::with_capacity(inner_dest_ty, 0).build();
                    Ok(Value::Scalar(Scalar::Map(new_column)))
                }
                Value::Column(Column::EmptyMap { len }) => {
                    let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                    for _ in 0..len {
                        builder.push_default();
                    }
                    Ok(Value::Column(builder.build()))
                }
                other => unreachable!("source: {}", other),
            },
            (DataType::Map(inner_src_ty), DataType::Map(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::Map(array)) => {
                    let validity = validity.map(|validity| {
                        Bitmap::new_constant(validity.null_count() != validity.len(), array.len())
                    });
                    let new_array = self
                        .run_cast(
                            span,
                            inner_src_ty,
                            inner_dest_ty,
                            Value::Column(array),
                            validity,
                            expr_display,
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Scalar(Scalar::Map(new_array)))
                }
                Value::Column(Column::Map(col)) => {
                    let validity = validity.map(|validity| {
                        let mut inner_validity = MutableBitmap::with_capacity(col.len());
                        for (index, offsets) in col.offsets().windows(2).enumerate() {
                            inner_validity.extend_constant(
                                (offsets[1] - offsets[0]) as usize,
                                validity.get_bit(index),
                            );
                        }
                        inner_validity.into()
                    });
                    let new_col = self
                        .run_cast(
                            span,
                            inner_src_ty,
                            inner_dest_ty,
                            Value::Column(col.underlying_column()),
                            validity,
                            expr_display,
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(Column::Map(Box::new(ArrayColumn::new(
                        new_col,
                        col.underlying_offsets(),
                    )))))
                }
                other => unreachable!("source: {}", other),
            },
            (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty))
                if fields_src_ty.len() == fields_dest_ty.len() =>
            {
                match value {
                    Value::Scalar(Scalar::Tuple(fields)) => {
                        let new_fields = fields
                            .into_iter()
                            .zip(fields_src_ty.iter())
                            .zip(fields_dest_ty.iter())
                            .map(|((field, src_ty), dest_ty)| {
                                self.run_cast(
                                    span,
                                    src_ty,
                                    dest_ty,
                                    Value::Scalar(field),
                                    validity.clone(),
                                    expr_display,
                                    options,
                                )
                                .map(|val| val.into_scalar().unwrap())
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Ok(Value::Scalar(Scalar::Tuple(new_fields)))
                    }
                    Value::Column(Column::Tuple(fields)) => {
                        let new_fields = fields
                            .into_iter()
                            .zip(fields_src_ty.iter())
                            .zip(fields_dest_ty.iter())
                            .map(|((field, src_ty), dest_ty)| {
                                self.run_cast(
                                    span,
                                    src_ty,
                                    dest_ty,
                                    Value::Column(field),
                                    validity.clone(),
                                    expr_display,
                                    options,
                                )
                                .map(|val| val.into_column().unwrap())
                            })
                            .collect::<Result<_>>()?;
                        Ok(Value::Column(Column::Tuple(new_fields)))
                    }
                    other => unreachable!("source: {}", other),
                }
            }
            (DataType::String, DataType::Vector(inner_dest_ty)) => {
                if matches!(inner_dest_ty, VectorDataType::Int8(_)) {
                    return Err(ErrorCode::BadArguments(format!(
                        "unable to cast type `{src_type}` to vector type `{dest_type}`"
                    ))
                    .set_span(span));
                }

                let dimension = inner_dest_ty.dimension() as usize;
                match value {
                    Value::Scalar(Scalar::String(val)) => {
                        if validity.as_ref().map(|v| v.get_bit(0)).unwrap_or(true) {
                            Ok(Value::Scalar(Scalar::Vector(
                                Self::cast_string_to_vector_scalar(span, &val, dimension)?,
                            )))
                        } else {
                            Ok(Value::Scalar(Scalar::default_value(dest_type)))
                        }
                    }
                    Value::Column(Column::String(col)) => {
                        let mut builder = ColumnBuilder::with_capacity(dest_type, col.len());
                        for (idx, val) in col.iter().enumerate() {
                            if validity.as_ref().map(|v| !v.get_bit(idx)).unwrap_or(false) {
                                builder.push_default();
                                continue;
                            }
                            let vector = Self::cast_string_to_vector_scalar(span, val, dimension)?;
                            builder.push(ScalarRef::Vector(vector.as_ref()));
                        }
                        Ok(Value::Column(builder.build()))
                    }
                    other => unreachable!("source: {}", other),
                }
            }
            (DataType::Variant, DataType::Vector(inner_dest_ty)) => {
                if matches!(inner_dest_ty, VectorDataType::Int8(_)) {
                    return Err(ErrorCode::BadArguments(format!(
                        "unable to cast type `{src_type}` to vector type `{dest_type}`"
                    ))
                    .set_span(span));
                }

                let dimension = inner_dest_ty.dimension() as usize;
                match value {
                    Value::Scalar(Scalar::Variant(val)) => {
                        if validity.as_ref().map(|v| v.get_bit(0)).unwrap_or(true) {
                            Ok(Value::Scalar(Scalar::Vector(
                                Self::cast_variant_to_vector_scalar(span, &val, dimension)?,
                            )))
                        } else {
                            Ok(Value::Scalar(Scalar::default_value(dest_type)))
                        }
                    }
                    Value::Column(Column::Variant(col)) => {
                        let mut builder = ColumnBuilder::with_capacity(dest_type, col.len());
                        for (idx, val) in col.iter().enumerate() {
                            if validity.as_ref().map(|v| !v.get_bit(idx)).unwrap_or(false) {
                                builder.push_default();
                                continue;
                            }
                            let vector = Self::cast_variant_to_vector_scalar(span, val, dimension)?;
                            builder.push(ScalarRef::Vector(vector.as_ref()));
                        }
                        Ok(Value::Column(builder.build()))
                    }
                    other => unreachable!("source: {}", other),
                }
            }
            (DataType::Array(inner_src_ty), DataType::Vector(inner_dest_ty)) => {
                if !matches!(
                    inner_src_ty.remove_nullable(),
                    DataType::Number(_) | DataType::Decimal(_)
                ) || matches!(inner_dest_ty, VectorDataType::Int8(_))
                {
                    return Err(ErrorCode::BadArguments(format!(
                        "unable to cast type `{src_type}` to vector type `{dest_type}`"
                    ))
                    .set_span(span));
                }
                let dimension = inner_dest_ty.dimension() as usize;
                match value {
                    Value::Scalar(Scalar::Array(col)) => {
                        if validity.as_ref().map(|v| v.get_bit(0)).unwrap_or(true) {
                            Ok(Value::Scalar(Scalar::Vector(
                                Self::cast_array_to_vector_scalar(span, col, dimension)?,
                            )))
                        } else {
                            Ok(Value::Scalar(Scalar::default_value(dest_type)))
                        }
                    }
                    Value::Column(Column::Array(array_col)) => {
                        let mut builder = ColumnBuilder::with_capacity(dest_type, array_col.len());
                        for (idx, col) in array_col.iter().enumerate() {
                            if validity.as_ref().map(|v| !v.get_bit(idx)).unwrap_or(false) {
                                builder.push_default();
                                continue;
                            }
                            let vector = Self::cast_array_to_vector_scalar(span, col, dimension)?;
                            builder.push(ScalarRef::Vector(vector.as_ref()));
                        }
                        Ok(Value::Column(builder.build()))
                    }
                    other => unreachable!("source: {}", other),
                }
            }
            _ => Err(ErrorCode::BadArguments(format!(
                "unable to cast type `{src_type}` to type `{dest_type}`"
            ))
            .set_span(span)),
        }?;

        if options.strict_eval {
            let mut check = CheckStrictValue;
            assert!(
                check.visit_value(result.clone()).is_ok(),
                "strict check fail on expr: {}, result: {result}",
                expr_display()
            )
        }

        Ok(result)
    }

    pub(crate) fn run_try_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        value: Value<AnyType>,
        expr_display: &impl Fn() -> String,
    ) -> Result<Value<AnyType>> {
        if src_type == dest_type {
            return Ok(value);
        }

        let nullable_dest_type = dest_type.wrap_nullable();
        let inner_dest_type = &**nullable_dest_type.as_nullable().unwrap();

        if let Some(cast_fn) = get_simple_cast_function(true, src_type, inner_dest_type) {
            // `try_to_xxx` functions must not return errors, so we can safely call them without concerning validity.
            if let Ok(Some(new_value)) = self.run_simple_cast(
                span,
                src_type,
                dest_type,
                value.clone(),
                &cast_fn,
                None,
                expr_display,
                &mut EvaluateOptions::default(),
            ) {
                return Ok(new_value);
            }
        }

        match (src_type, inner_dest_type) {
            (DataType::Null, _) => match value {
                Value::Scalar(Scalar::Null) => Ok(Value::Scalar(Scalar::Null)),
                Value::Column(Column::Null { len }) => {
                    let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                    for _ in 0..len {
                        builder.push_default();
                    }
                    Ok(Value::Column(builder.build()))
                }
                other => unreachable!("source: {}", other),
            },
            (DataType::Nullable(inner_src_ty), _) => match value {
                Value::Scalar(Scalar::Null) => Ok(Value::Scalar(Scalar::Null)),
                Value::Scalar(_) => {
                    self.run_try_cast(span, inner_src_ty, inner_dest_type, value, expr_display)
                }
                Value::Column(Column::Nullable(col)) => {
                    let value = Value::Column(col.column);
                    let new_col = *self
                        .run_try_cast(span, inner_src_ty, dest_type, value, expr_display)?
                        .into_column()
                        .unwrap()
                        .into_nullable()
                        .unwrap();
                    let validity = boolean::and(&col.validity, &new_col.validity);
                    Ok(Value::Column(NullableColumn::new_column(
                        new_col.column,
                        validity,
                    )))
                }
                other => unreachable!("source: {}", other),
            },
            (src_ty, inner_dest_ty) if src_ty == inner_dest_ty => match value {
                Value::Scalar(_) => Ok(value),
                Value::Column(column) => {
                    let validity = Bitmap::new_constant(true, column.len());
                    Ok(Value::Column(NullableColumn::new_column(column, validity)))
                }
            },

            (DataType::EmptyArray, DataType::Array(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::EmptyArray) => {
                    let new_column = ColumnBuilder::with_capacity(inner_dest_ty, 0).build();
                    Ok(Value::Scalar(Scalar::Array(new_column)))
                }
                Value::Column(Column::EmptyArray { len }) => {
                    let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                    for _ in 0..len {
                        builder.push_default();
                    }
                    Ok(Value::Column(builder.build()))
                }
                other => unreachable!("source: {}", other),
            },
            (DataType::Array(inner_src_ty), DataType::Array(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::Array(array)) => {
                    let value = Value::Column(array);
                    let new_array = self
                        .run_try_cast(span, inner_src_ty, inner_dest_ty, value, expr_display)?
                        .into_column()
                        .unwrap();
                    Ok(Value::Scalar(Scalar::Array(new_array)))
                }
                Value::Column(Column::Array(col)) => {
                    let value = Value::Column(col.underlying_column());
                    let new_values = self
                        .run_try_cast(span, inner_src_ty, inner_dest_ty, value, expr_display)?
                        .into_column()
                        .unwrap();
                    let new_col = Column::Array(Box::new(ArrayColumn::new(
                        new_values,
                        col.underlying_offsets(),
                    )));
                    let validity = Bitmap::new_constant(true, new_col.len());

                    Ok(Value::Column(NullableColumn::new_column(new_col, validity)))
                }
                _ => unreachable!(),
            },
            (DataType::EmptyMap, DataType::Map(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::EmptyMap) => {
                    let new_column = ColumnBuilder::with_capacity(inner_dest_ty, 0).build();
                    Ok(Value::Scalar(Scalar::Map(new_column)))
                }
                Value::Column(Column::EmptyMap { len }) => {
                    let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                    for _ in 0..len {
                        builder.push_default();
                    }
                    Ok(Value::Column(builder.build()))
                }
                other => unreachable!("source: {}", other),
            },
            (DataType::Map(inner_src_ty), DataType::Map(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::Map(array)) => {
                    let value = Value::Column(array);
                    let new_array = self
                        .run_try_cast(span, inner_src_ty, inner_dest_ty, value, expr_display)?
                        .into_column()
                        .unwrap();
                    Ok(Value::Scalar(Scalar::Map(new_array)))
                }
                Value::Column(Column::Map(col)) => {
                    let value = Value::Column(col.underlying_column());
                    let new_values = self
                        .run_try_cast(span, inner_src_ty, inner_dest_ty, value, expr_display)?
                        .into_column()
                        .unwrap();
                    let new_col = Column::Map(Box::new(ArrayColumn::new(
                        new_values,
                        col.underlying_offsets(),
                    )));
                    let validity = Bitmap::new_constant(true, new_col.len());

                    Ok(Value::Column(NullableColumn::new_column(new_col, validity)))
                }
                _ => unreachable!(),
            },
            (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty))
                if fields_src_ty.len() == fields_dest_ty.len() =>
            {
                match value {
                    Value::Scalar(Scalar::Tuple(fields)) => {
                        let new_fields = fields
                            .into_iter()
                            .zip(fields_src_ty.iter())
                            .zip(fields_dest_ty.iter())
                            .map(|((field, src_ty), dest_ty)| {
                                let value = Value::Scalar(field);
                                Ok(self
                                    .run_try_cast(span, src_ty, dest_ty, value, expr_display)?
                                    .into_scalar()
                                    .unwrap())
                            })
                            .collect::<Result<_>>()?;
                        Ok(Value::Scalar(Scalar::Tuple(new_fields)))
                    }
                    Value::Column(Column::Tuple(fields)) => {
                        let new_fields = fields
                            .into_iter()
                            .zip(fields_src_ty.iter())
                            .zip(fields_dest_ty.iter())
                            .map(|((field, src_ty), dest_ty)| {
                                let value = Value::Column(field);
                                Ok(self
                                    .run_try_cast(span, src_ty, dest_ty, value, expr_display)?
                                    .into_column()
                                    .unwrap())
                            })
                            .collect::<Result<_>>()?;
                        let new_col = Column::Tuple(new_fields);
                        let validity = Bitmap::new_constant(true, new_col.len());
                        Ok(Value::Column(NullableColumn::new_column(new_col, validity)))
                    }
                    other => unreachable!("source: {}", other),
                }
            }
            (DataType::String, DataType::Vector(inner_vector_ty)) => {
                if matches!(inner_vector_ty, VectorDataType::Int8(_)) {
                    return Err(ErrorCode::BadArguments(format!(
                        "unable to cast type `{src_type}` to type `{dest_type}`"
                    ))
                    .set_span(span));
                }

                let dimension = inner_vector_ty.dimension() as usize;
                match value {
                    Value::Scalar(Scalar::String(val)) => Ok(
                        match Self::cast_string_to_vector_scalar(span, &val, dimension) {
                            Ok(vector) => Value::Scalar(Scalar::Vector(vector)),
                            Err(_) => Value::Scalar(Scalar::Null),
                        },
                    ),
                    Value::Column(Column::String(col)) => {
                        let mut builder = ColumnBuilder::with_capacity(dest_type, col.len());
                        for val in col.iter() {
                            match Self::cast_string_to_vector_scalar(span, val, dimension) {
                                Ok(vector) => builder.push(ScalarRef::Vector(vector.as_ref())),
                                Err(_) => builder.push(ScalarRef::Null),
                            }
                        }
                        Ok(Value::Column(builder.build()))
                    }
                    _ => unreachable!(),
                }
            }
            (DataType::Variant, DataType::Vector(inner_vector_ty)) => {
                if matches!(inner_vector_ty, VectorDataType::Int8(_)) {
                    return Err(ErrorCode::BadArguments(format!(
                        "unable to cast type `{src_type}` to type `{dest_type}`"
                    ))
                    .set_span(span));
                }

                let dimension = inner_vector_ty.dimension() as usize;
                match value {
                    Value::Scalar(Scalar::Variant(val)) => Ok(
                        match Self::cast_variant_to_vector_scalar(span, &val, dimension) {
                            Ok(vector) => Value::Scalar(Scalar::Vector(vector)),
                            Err(_) => Value::Scalar(Scalar::Null),
                        },
                    ),
                    Value::Column(Column::Variant(col)) => {
                        let mut builder = ColumnBuilder::with_capacity(dest_type, col.len());
                        for val in col.iter() {
                            match Self::cast_variant_to_vector_scalar(span, val, dimension) {
                                Ok(vector) => builder.push(ScalarRef::Vector(vector.as_ref())),
                                Err(_) => builder.push(ScalarRef::Null),
                            }
                        }
                        Ok(Value::Column(builder.build()))
                    }
                    _ => unreachable!(),
                }
            }
            (DataType::Array(inner_src_ty), DataType::Vector(inner_vector_ty)) => {
                if !matches!(
                    inner_src_ty.remove_nullable(),
                    DataType::Number(_) | DataType::Decimal(_)
                ) || matches!(inner_vector_ty, VectorDataType::Int8(_))
                {
                    return Err(ErrorCode::BadArguments(format!(
                        "unable to cast type `{src_type}` to type `{dest_type}`"
                    ))
                    .set_span(span));
                }

                let dimension = inner_vector_ty.dimension() as usize;
                match value {
                    Value::Scalar(Scalar::Array(col)) => Ok(
                        match Self::cast_array_to_vector_scalar(span, col, dimension) {
                            Ok(vector) => Value::Scalar(Scalar::Vector(vector)),
                            Err(_) => Value::Scalar(Scalar::Null),
                        },
                    ),
                    Value::Column(Column::Array(col)) => {
                        let mut builder = ColumnBuilder::with_capacity(dest_type, col.len());
                        for value in col.iter() {
                            match Self::cast_array_to_vector_scalar(span, value, dimension) {
                                Ok(vector) => builder.push(ScalarRef::Vector(vector.as_ref())),
                                Err(_) => builder.push(ScalarRef::Null),
                            }
                        }
                        Ok(Value::Column(builder.build()))
                    }
                    _ => unreachable!(),
                }
            }

            _ => Err(ErrorCode::BadArguments(format!(
                "unable to cast type `{src_type}` to type `{dest_type}`"
            ))
            .set_span(span)),
        }
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
                if inner.validity.null_count() > 0 {
                    return Err(ErrorCode::BadArguments(
                        "Array value cast to a vector has invalid value".to_string(),
                    )
                    .set_span(span));
                }
                inner.column
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

    fn run_simple_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        value: Value<AnyType>,
        cast_fn: &str,
        validity: Option<Bitmap>,
        expr_display: &impl Fn() -> String,
        options: &mut EvaluateOptions,
    ) -> Result<Option<Value<AnyType>>> {
        if src_type.remove_nullable() == dest_type.remove_nullable() {
            return Ok(None);
        }

        let expr = ColumnRef {
            span,
            id: 0,
            data_type: src_type.clone(),
            display_name: String::new(),
        };

        let params = if let DataType::Decimal(ty) = dest_type.remove_nullable() {
            vec![
                Scalar::Number(NumberScalar::Int64(ty.precision() as _)),
                Scalar::Number(NumberScalar::Int64(ty.scale() as _)),
            ]
        } else {
            vec![]
        };

        let Ok(func_expr) =
            check_function(span, cast_fn, &params, &[expr.into()], self.fn_registry)
        else {
            return Ok(None);
        };

        let call = func_expr.into_function_call().unwrap();
        if call.return_type != *dest_type {
            return Ok(None);
        }

        let num_rows = validity
            .as_ref()
            .map(|validity| validity.len())
            .unwrap_or_else(|| match &value {
                Value::Scalar(_) => 1,
                Value::Column(col) => col.len(),
            });

        let entry = BlockEntry::new(value, || (src_type.clone(), num_rows));
        let block = DataBlock::new(vec![entry], num_rows);
        let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
        let result = evaluator.eval_common_call(&call, validity, expr_display, options)?;
        Ok(Some(result))
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

        // Evaluate the condition first and then partially evaluate the result branches.
        let mut validity = validity.unwrap_or_else(|| Bitmap::new_constant(true, num_rows));
        let mut conds = Vec::new();
        let mut flags = Vec::new();
        let mut results = Vec::new();
        for cond_idx in (0..args.len() - 1).step_by(2) {
            let cond = self.partial_run(&args[cond_idx], Some(validity.clone()), options)?;
            match cond.try_downcast::<NullableType<BooleanType>>().unwrap() {
                Value::Scalar(None | Some(false)) => {
                    results.push(Value::Scalar(Scalar::default_value(&generics[0])));
                    flags.push(Bitmap::new_constant(false, len.unwrap_or(1)));
                }
                Value::Scalar(Some(true)) => {
                    results.push(self.partial_run(
                        &args[cond_idx + 1],
                        Some(validity.clone()),
                        options,
                    )?);
                    validity = Bitmap::new_constant(false, num_rows);
                    flags.push(Bitmap::new_constant(true, len.unwrap_or(1)));
                    break;
                }
                Value::Column(cond) => {
                    let flag = (&cond.column) & (&cond.validity);
                    results.push(self.partial_run(
                        &args[cond_idx + 1],
                        Some((&validity) & (&flag)),
                        options,
                    )?);
                    validity = (&validity) & (&flag.not());
                    flags.push(flag);
                }
            };
            conds.push(cond);
        }
        let else_result = self.partial_run(&args[args.len() - 1], Some(validity), options)?;

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
            Some(validity) => Value::Column(NullableColumn::new_column(column, validity)),
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
    ) -> Result<Value<AnyType>> {
        let expr = lambda_expr.as_expr(self.fn_registry);
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
                Value::Column(Column::Array(box array_col)) => (
                    array_col.underlying_column(),
                    array_col.underlying_offsets(),
                    None,
                ),
                Value::Column(Column::Map(box map_col)) => (
                    map_col.underlying_column(),
                    map_col.underlying_offsets(),
                    None,
                ),
                Value::Column(Column::Nullable(box nullable_col)) => match &nullable_col.column {
                    Column::Array(box array_col) => (
                        array_col.underlying_column(),
                        array_col.underlying_offsets(),
                        keep_outer_validity.then(|| nullable_col.validity.clone()),
                    ),
                    Column::Map(box map_col) => (
                        map_col.underlying_column(),
                        map_col.underlying_offsets(),
                        keep_outer_validity.then(|| nullable_col.validity.clone()),
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
            DataType::Nullable(box DataType::Generic(index)) => {
                DataType::Nullable(Box::new(generics[*index].clone()))
            }
            _ => data_type.clone(),
        }
    }

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
                Ok((entry.value().clone(), entry.data_type()))
            }
            Expr::Cast(Cast {
                span,
                is_try,
                expr,
                dest_type,
            }) => {
                let value = self.get_select_child(expr, options)?.0;
                let src_type = expr.data_type();
                let value = if *is_try {
                    self.run_try_cast(*span, src_type, dest_type, value, &|| expr.sql_display())?
                } else {
                    self.run_cast(
                        *span,
                        src_type,
                        dest_type,
                        value,
                        None,
                        &|| expr.sql_display(),
                        options,
                    )?
                };
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
                let mut child_option = options.with_suppress_error(child_suppress_error);
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
                let result = eval.eval(&args, &mut ctx);

                // inject errors into options, parent will handle it
                if options.suppress_error {
                    options.errors = ctx.errors.take();
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
                    .map(|expr| self.partial_run(expr, None, &mut EvaluateOptions::default()))
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
                    self.run_lambda(name, args, data_types, lambda_expr, return_type)?,
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
        self.visit_column(column.column)
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
