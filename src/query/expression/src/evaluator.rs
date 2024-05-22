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

use std::collections::HashMap;
use std::ops::Not;

use databend_common_arrow::arrow::bitmap;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;
use log::error;

use crate::block::DataBlock;
use crate::expression::Expr;
use crate::function::EvalContext;
use crate::property::Domain;
use crate::type_check::check_function;
use crate::type_check::get_simple_cast_function;
use crate::types::any::AnyType;
use crate::types::array::ArrayColumn;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableDomain;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::NullableType;
use crate::types::NumberScalar;
use crate::values::Column;
use crate::values::ColumnBuilder;
use crate::values::Scalar;
use crate::values::Value;
use crate::BlockEntry;
use crate::ColumnIndex;
use crate::FunctionContext;
use crate::FunctionDomain;
use crate::FunctionEval;
use crate::FunctionRegistry;
use crate::RemoteExpr;

#[derive(Default)]
pub struct EvaluateOptions<'a> {
    pub selection: Option<&'a [u32]>,
    pub suppress_error: bool,
    pub errors: Option<(MutableBitmap, String)>,
}

impl<'a> EvaluateOptions<'a> {
    pub fn new(selection: Option<&'a [u32]>) -> EvaluateOptions<'a> {
        Self {
            suppress_error: false,
            selection,
            errors: None,
        }
    }

    pub fn with_suppress_error(&mut self, suppress_error: bool) -> Self {
        Self {
            suppress_error,
            selection: self.selection,
            errors: None,
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
            if (column.data_type == DataType::Null && data_type.is_nullable())
                || (column.data_type.is_nullable() && data_type == &DataType::Null)
            {
                continue;
            }
            assert_eq!(
                &column.data_type,
                data_type,
                "column data type mismatch at index: {index}, expr: {}",
                expr.sql_display(),
            );
        }
    }

    pub fn run(&self, expr: &Expr) -> Result<Value<AnyType>> {
        self.partial_run(expr, None, &mut EvaluateOptions::default())
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
            Expr::Constant { scalar, .. } => Ok(Value::Scalar(scalar.clone())),
            Expr::ColumnRef { id, .. } => Ok(self.data_block.get_by_offset(*id).value.clone()),
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => {
                let value = self.partial_run(expr, validity.clone(), options)?;
                if *is_try {
                    self.run_try_cast(*span, expr.data_type(), dest_type, value)
                } else {
                    self.run_cast(*span, expr.data_type(), dest_type, value, validity, options)
                }
            }
            Expr::FunctionCall {
                function,
                args,
                generics,
                ..
            } if function.signature.name == "if" => self.eval_if(args, generics, validity, options),

            Expr::FunctionCall { function, args, .. }
                if function.signature.name == "and_filters" =>
            {
                self.eval_and_filters(args, validity, options)
            }

            Expr::FunctionCall {
                span,
                id,
                function,
                args,
                generics,
                ..
            } => {
                let child_suppress_error = function.signature.name == "is_not_error";
                let mut child_option = options.with_suppress_error(child_suppress_error);

                let args = args
                    .iter()
                    .map(|expr| self.partial_run(expr, validity.clone(), &mut child_option))
                    .collect::<Result<Vec<_>>>()?;

                assert!(
                    args.iter()
                        .filter_map(|val| match val {
                            Value::Column(col) => Some(col.len()),
                            Value::Scalar(_) => None,
                        })
                        .all_equal()
                );
                let cols_ref = args.iter().map(Value::as_ref).collect::<Vec<_>>();

                let errors = if !child_suppress_error {
                    None
                } else {
                    child_option.errors.take()
                };
                let mut ctx = EvalContext {
                    generics,
                    num_rows: self.data_block.num_rows(),
                    validity,
                    errors,
                    func_ctx: self.func_ctx,
                    suppress_error: options.suppress_error,
                };

                let (_, eval) = function.eval.as_scalar().unwrap();
                let result = (eval)(cols_ref.as_slice(), &mut ctx);
                ctx.render_error(
                    *span,
                    id.params(),
                    &args,
                    &function.signature.name,
                    options.selection,
                )?;

                // inject errors into options, parent will handle it
                if options.suppress_error {
                    options.errors = ctx.errors.take();
                }

                Ok(result)
            }
            Expr::LambdaFunctionCall {
                name,
                args,
                lambda_expr,
                return_type,
                ..
            } => {
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

                self.run_lambda(name, args, lambda_expr, return_type)
            }
        };

        match &result {
            Ok(Value::Scalar(result)) => {
                assert!(
                    result.as_ref().is_value_of_type(expr.data_type()),
                    "{} is not of type {}",
                    result,
                    expr.data_type()
                )
            }
            Ok(Value::Column(result)) => assert_eq!(&result.data_type(), expr.data_type()),
            Err(_) => {
                #[cfg(debug_assertions)]
                {
                    use std::sync::atomic::AtomicBool;
                    use std::sync::atomic::Ordering;

                    static RECURSING: AtomicBool = AtomicBool::new(false);
                    if RECURSING
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        assert_eq!(
                            ConstantFolder::fold_with_domain(
                                expr,
                                &self.data_block.domains().into_iter().enumerate().collect(),
                                self.func_ctx,
                                self.fn_registry
                            )
                            .1,
                            None,
                            "domain calculation should not return any domain for expressions that are possible to fail with err {}",
                            result.unwrap_err()
                        );
                        RECURSING.store(false, Ordering::SeqCst);
                    }
                }
            }
        }

        result
    }

    pub fn run_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        value: Value<AnyType>,
        validity: Option<Bitmap>,
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
                options,
            )? {
                return Ok(new_value);
            }
        }

        match (src_type, dest_type) {
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
            (DataType::Nullable(inner_src_ty), DataType::Nullable(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::Null) => Ok(Value::Scalar(Scalar::Null)),
                Value::Scalar(_) => {
                    self.run_cast(span, inner_src_ty, inner_dest_ty, value, validity, options)
                }
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
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(Column::Nullable(Box::new(NullableColumn {
                        column,
                        validity,
                    }))))
                }
                other => unreachable!("source: {}", other),
            },
            (DataType::Nullable(inner_src_ty), _) => match value {
                Value::Scalar(Scalar::Null) => {
                    let has_valid = validity
                        .map(|validity| validity.unset_bits() < validity.len())
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
                Value::Scalar(_) => {
                    self.run_cast(span, inner_src_ty, dest_type, value, validity, options)
                }
                Value::Column(Column::Nullable(col)) => {
                    let has_valid_nulls = validity
                        .as_ref()
                        .map(|validity| {
                            (validity & (&col.validity)).unset_bits() > validity.unset_bits()
                        })
                        .unwrap_or_else(|| col.validity.unset_bits() > 0);
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
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(Column::Nullable(Box::new(NullableColumn {
                        validity: Bitmap::new_constant(true, column.len()),
                        column,
                    }))))
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
                        Bitmap::new_constant(validity.unset_bits() != validity.len(), array.len())
                    });
                    let new_array = self
                        .run_cast(
                            span,
                            inner_src_ty,
                            inner_dest_ty,
                            Value::Column(array),
                            validity,
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Scalar(Scalar::Array(new_array)))
                }
                Value::Column(Column::Array(col)) => {
                    let validity = validity.map(|validity| {
                        let mut inner_validity = MutableBitmap::with_capacity(col.len());
                        for (index, offsets) in col.offsets.windows(2).enumerate() {
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
                            Value::Column(col.values),
                            validity,
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(Column::Array(Box::new(ArrayColumn {
                        values: new_col,
                        offsets: col.offsets,
                    }))))
                }
                other => unreachable!("source: {}", other),
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
                    let validity = validity.map(|validity| {
                        Bitmap::new_constant(validity.unset_bits() != validity.len(), array.len())
                    });
                    let new_array = self
                        .run_cast(
                            span,
                            inner_src_ty,
                            inner_dest_ty,
                            Value::Column(array),
                            validity,
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Scalar(Scalar::Map(new_array)))
                }
                Value::Column(Column::Map(col)) => {
                    let validity = validity.map(|validity| {
                        let mut inner_validity = MutableBitmap::with_capacity(col.len());
                        for (index, offsets) in col.offsets.windows(2).enumerate() {
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
                            Value::Column(col.values),
                            validity,
                            options,
                        )?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(Column::Map(Box::new(ArrayColumn {
                        values: new_col,
                        offsets: col.offsets,
                    }))))
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

            _ => Err(ErrorCode::BadArguments(format!(
                "unable to cast type `{src_type}` to type `{dest_type}`"
            ))
            .set_span(span)),
        }
    }

    pub fn run_try_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        value: Value<AnyType>,
    ) -> Result<Value<AnyType>> {
        if src_type == dest_type {
            return Ok(value);
        }

        // The dest_type of `TRY_CAST` must be `Nullable`, which is guaranteed by the type checker.
        let inner_dest_type = &**dest_type.as_nullable().unwrap();

        if let Some(cast_fn) = get_simple_cast_function(true, src_type, inner_dest_type) {
            // `try_to_xxx` functions must not return errors, so we can safely call them without concerning validity.
            if let Ok(Some(new_value)) = self.run_simple_cast(
                span,
                src_type,
                dest_type,
                value.clone(),
                &cast_fn,
                None,
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
                Value::Scalar(_) => self.run_try_cast(span, inner_src_ty, inner_dest_type, value),
                Value::Column(Column::Nullable(col)) => {
                    let new_col = *self
                        .run_try_cast(span, inner_src_ty, dest_type, Value::Column(col.column))?
                        .into_column()
                        .unwrap()
                        .into_nullable()
                        .unwrap();
                    Ok(Value::Column(Column::Nullable(Box::new(NullableColumn {
                        column: new_col.column,
                        validity: bitmap::and(&col.validity, &new_col.validity),
                    }))))
                }
                other => unreachable!("source: {}", other),
            },
            (src_ty, inner_dest_ty) if src_ty == inner_dest_ty => match value {
                Value::Scalar(_) => Ok(value),
                Value::Column(column) => {
                    Ok(Value::Column(Column::Nullable(Box::new(NullableColumn {
                        validity: Bitmap::new_constant(true, column.len()),
                        column,
                    }))))
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
                    let new_array = self
                        .run_try_cast(span, inner_src_ty, inner_dest_ty, Value::Column(array))?
                        .into_column()
                        .unwrap();
                    Ok(Value::Scalar(Scalar::Array(new_array)))
                }
                Value::Column(Column::Array(col)) => {
                    let new_values = self
                        .run_try_cast(span, inner_src_ty, inner_dest_ty, Value::Column(col.values))?
                        .into_column()
                        .unwrap();
                    let new_col = Column::Array(Box::new(ArrayColumn {
                        values: new_values,
                        offsets: col.offsets,
                    }));
                    Ok(Value::Column(Column::Nullable(Box::new(NullableColumn {
                        validity: Bitmap::new_constant(true, new_col.len()),
                        column: new_col,
                    }))))
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
                    let new_array = self
                        .run_try_cast(span, inner_src_ty, inner_dest_ty, Value::Column(array))?
                        .into_column()
                        .unwrap();
                    Ok(Value::Scalar(Scalar::Map(new_array)))
                }
                Value::Column(Column::Map(col)) => {
                    let new_values = self
                        .run_try_cast(span, inner_src_ty, inner_dest_ty, Value::Column(col.values))?
                        .into_column()
                        .unwrap();
                    let new_col = Column::Map(Box::new(ArrayColumn {
                        values: new_values,
                        offsets: col.offsets,
                    }));
                    Ok(Value::Column(Column::Nullable(Box::new(NullableColumn {
                        validity: Bitmap::new_constant(true, new_col.len()),
                        column: new_col,
                    }))))
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
                                Ok(self
                                    .run_try_cast(span, src_ty, dest_ty, Value::Scalar(field))?
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
                                Ok(self
                                    .run_try_cast(span, src_ty, dest_ty, Value::Column(field))?
                                    .into_column()
                                    .unwrap())
                            })
                            .collect::<Result<_>>()?;
                        let new_col = Column::Tuple(new_fields);
                        Ok(Value::Column(Column::Nullable(Box::new(NullableColumn {
                            validity: Bitmap::new_constant(true, new_col.len()),
                            column: new_col,
                        }))))
                    }
                    other => unreachable!("source: {}", other),
                }
            }

            _ => Err(ErrorCode::BadArguments(format!(
                "unable to cast type `{src_type}` to type `{dest_type}`"
            ))
            .set_span(span)),
        }
    }

    fn run_simple_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        value: Value<AnyType>,
        cast_fn: &str,
        validity: Option<Bitmap>,
        options: &mut EvaluateOptions,
    ) -> Result<Option<Value<AnyType>>> {
        let expr = Expr::ColumnRef {
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

        let cast_expr = match check_function(span, cast_fn, &params, &[expr], self.fn_registry) {
            Ok(cast_expr) => cast_expr,
            Err(_) => return Ok(None),
        };

        if cast_expr.data_type() != dest_type {
            return Ok(None);
        }

        let num_rows = validity
            .as_ref()
            .map(|validity| validity.len())
            .unwrap_or_else(|| match &value {
                Value::Scalar(_) => 1,
                Value::Column(col) => col.len(),
            });
        let block = DataBlock::new(vec![BlockEntry::new(src_type.clone(), value)], num_rows);
        let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
        Ok(Some(evaluator.partial_run(&cast_expr, validity, options)?))
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
        if args.len() < 3 && args.len() % 2 == 0 {
            unreachable!()
        }

        let num_rows = self.data_block.num_rows();
        let len = self
            .data_block
            .columns()
            .iter()
            .find_map(|col| match &col.value {
                Value::Column(col) => Some(col.len()),
                _ => None,
            });

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
        mut validity: Option<Bitmap>,
        options: &mut EvaluateOptions,
    ) -> Result<Value<AnyType>> {
        assert!(args.len() >= 2);

        for arg in args {
            let cond = self.partial_run(arg, validity.clone(), options)?;
            match &cond {
                Value::Scalar(Scalar::Null | Scalar::Boolean(false)) => {
                    return Ok(Value::Scalar(Scalar::Boolean(false)));
                }
                Value::Scalar(Scalar::Boolean(true)) => {
                    continue;
                }
                Value::Column(column) => {
                    let flag = match column {
                        Column::Nullable(box nullable_column) => {
                            let boolean_column = nullable_column.column.as_boolean().unwrap();
                            boolean_column & (&nullable_column.validity)
                        }
                        Column::Boolean(boolean_column) => boolean_column.clone(),
                        _ => unreachable!(),
                    };
                    match &validity {
                        Some(v) => {
                            validity = Some(v & (&flag));
                        }
                        None => {
                            validity = Some(flag);
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        match validity {
            Some(bitmap) => Ok(Value::Column(Column::Boolean(bitmap))),
            None => Ok(Value::Scalar(Scalar::Boolean(true))),
        }
    }

    /// Evaluate a set-returning-function. Return multiple sets of results
    /// for each input row, along with the number of rows in each set.
    pub fn run_srf(
        &self,
        expr: &Expr,
        max_nums_per_row: &mut [usize],
    ) -> Result<Vec<(Value<AnyType>, usize)>> {
        if let Expr::FunctionCall {
            span,
            id,
            function,
            args,
            return_type,
            generics,
            ..
        } = expr
        {
            if let FunctionEval::SRF { eval } = &function.eval {
                assert!(return_type.as_tuple().is_some());
                let args = args
                    .iter()
                    .map(|expr| self.run(expr))
                    .collect::<Result<Vec<_>>>()?;
                let cols_ref = args.iter().map(Value::as_ref).collect::<Vec<_>>();
                let mut ctx = EvalContext {
                    generics,
                    num_rows: self.data_block.num_rows(),
                    validity: None,
                    errors: None,
                    func_ctx: self.func_ctx,
                    suppress_error: false,
                };
                let result = (eval)(&cols_ref, &mut ctx, max_nums_per_row);
                ctx.render_error(*span, id.params(), &args, &function.signature.name, None)?;
                assert_eq!(result.len(), self.data_block.num_rows());
                return Ok(result);
            }
        }

        unreachable!("expr is not a set returning function: {expr}")
    }

    fn run_array_reduce(&self, column: &Column, expr: &Expr) -> Result<Scalar> {
        let col_type = column.data_type();
        if col_type.is_null() || column.len() < 1 {
            return Ok(Scalar::Null);
        }
        let mut arg0 = column.index(0).unwrap().to_owned();
        let mut eval_options = EvaluateOptions::default();
        for i in 1..column.len() {
            let arg1 = column.index(i).unwrap().to_owned();
            let entries = {
                vec![
                    BlockEntry::new(col_type.clone(), Value::Scalar(arg0.clone())),
                    BlockEntry::new(col_type.clone(), Value::Scalar(arg1)),
                ]
            };
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
                    &mut eval_options,
                )?
                .as_scalar()
                .unwrap()
                .clone();
        }
        Ok(arg0)
    }

    pub fn run_lambda(
        &self,
        func_name: &str,
        args: Vec<Value<AnyType>>,
        lambda_expr: &RemoteExpr,
        return_type: &DataType,
    ) -> Result<Value<AnyType>> {
        let expr = lambda_expr.as_expr(self.fn_registry);
        // array_reduce differs
        if func_name == "array_reduce" {
            match &args[0] {
                Value::Scalar(s) => match s {
                    Scalar::Array(c) => {
                        let result = self.run_array_reduce(c, &expr)?;
                        return Ok(Value::Scalar(result));
                    }
                    _ => unreachable!(),
                },
                Value::Column(c) => {
                    let mut builder = ColumnBuilder::with_capacity(return_type, c.len());
                    for val in c.iter() {
                        match &val.to_owned() {
                            Scalar::Array(c) => {
                                let result = self.run_array_reduce(c, &expr)?;
                                let item = result.as_ref();
                                builder.push(item);
                            }
                            Scalar::Null => {
                                builder.push_default();
                            }
                            _ => unreachable!(),
                        }
                    }
                    return Ok(Value::Column(builder.build()));
                }
            }
        }
        // TODO: Support multi args
        match &args[0] {
            Value::Scalar(s) => match s {
                Scalar::Array(c) => {
                    let entry = BlockEntry::new(c.data_type(), Value::Column(c.clone()));
                    let block = DataBlock::new(vec![entry], c.len());

                    let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
                    let result = evaluator.run(&expr)?;
                    let result_col = result.convert_to_full_column(expr.data_type(), c.len());

                    let val = if func_name == "array_filter" {
                        let result_col = result_col.remove_nullable();
                        let bitmap = result_col.as_boolean().unwrap();
                        let filtered_inner_col = c.filter(bitmap);
                        Value::Scalar(Scalar::Array(filtered_inner_col))
                    } else {
                        Value::Scalar(Scalar::Array(result_col))
                    };
                    Ok(val)
                }
                _ => unreachable!(),
            },
            Value::Column(c) => {
                let (inner_col, inner_ty, offsets, validity) = match c {
                    Column::Array(box array_col) => (
                        array_col.values.clone(),
                        array_col.values.data_type(),
                        array_col.offsets.clone(),
                        None,
                    ),
                    Column::Nullable(box nullable_col) => match &nullable_col.column {
                        Column::Array(box array_col) => (
                            array_col.values.clone(),
                            array_col.values.data_type(),
                            array_col.offsets.clone(),
                            Some(nullable_col.validity.clone()),
                        ),
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                };
                let entry = BlockEntry::new(inner_ty, Value::Column(inner_col.clone()));
                let block = DataBlock::new(vec![entry], inner_col.len());

                let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
                let result = evaluator.run(&expr)?;
                let result_col = result.convert_to_full_column(expr.data_type(), inner_col.len());

                let array_col = if func_name == "array_filter" {
                    let result_col = result_col.remove_nullable();
                    let bitmap = result_col.as_boolean().unwrap();
                    let filtered_inner_col = inner_col.filter(bitmap);
                    // generate new offsets after filter.
                    let mut new_offset = 0;
                    let mut filtered_offsets = Vec::with_capacity(offsets.len());
                    filtered_offsets.push(0);
                    for offset in offsets.windows(2) {
                        let off = offset[0] as usize;
                        let len = (offset[1] - offset[0]) as usize;
                        let unset_count = bitmap.null_count_range(off, len);
                        new_offset += (len - unset_count) as u64;
                        filtered_offsets.push(new_offset);
                    }

                    Column::Array(Box::new(ArrayColumn {
                        values: filtered_inner_col,
                        offsets: filtered_offsets.into(),
                    }))
                } else {
                    Column::Array(Box::new(ArrayColumn {
                        values: result_col,
                        offsets,
                    }))
                };
                let col = match validity {
                    Some(validity) => Value::Column(Column::Nullable(Box::new(NullableColumn {
                        column: array_col,
                        validity,
                    }))),
                    None => Value::Column(array_col),
                };
                Ok(col)
            }
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
            Expr::Constant { scalar, .. } => Ok((
                Value::Scalar(scalar.clone()),
                scalar.as_ref().infer_data_type(),
            )),
            Expr::ColumnRef { id, .. } => {
                let entry = self.data_block.get_by_offset(*id);
                Ok((entry.value.clone(), entry.data_type.clone()))
            }
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => {
                let value = self.get_select_child(expr, options)?.0;
                if *is_try {
                    Ok((
                        self.run_try_cast(*span, expr.data_type(), dest_type, value)?,
                        dest_type.clone(),
                    ))
                } else {
                    Ok((
                        self.run_cast(*span, expr.data_type(), dest_type, value, None, options)?,
                        dest_type.clone(),
                    ))
                }
            }
            Expr::FunctionCall {
                function,
                args,
                generics,
                ..
            } if function.signature.name == "if" => {
                let return_type =
                    self.remove_generics_data_type(generics, &function.signature.return_type);
                Ok((self.eval_if(args, generics, None, options)?, return_type))
            }

            Expr::FunctionCall {
                function,
                args,
                generics,
                ..
            } if function.signature.name == "and_filters" => {
                let return_type =
                    self.remove_generics_data_type(generics, &function.signature.return_type);
                Ok((self.eval_and_filters(args, None, options)?, return_type))
            }

            Expr::FunctionCall {
                span,
                id,
                function,
                args,
                generics,
                ..
            } => {
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

                let cols_ref = args
                    .iter()
                    .map(|(val, _)| Value::as_ref(val))
                    .collect::<Vec<_>>();

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
                };
                let (_, eval) = function.eval.as_scalar().unwrap();
                let result = (eval)(cols_ref.as_slice(), &mut ctx);
                let args = args.into_iter().map(|(val, _)| val).collect::<Vec<_>>();

                ctx.render_error(
                    *span,
                    id.params(),
                    &args,
                    &function.signature.name,
                    options.selection,
                )?;

                // inject errors into options, parent will handle it
                if options.suppress_error {
                    options.errors = ctx.errors.take();
                }

                let return_type =
                    self.remove_generics_data_type(generics, &function.signature.return_type);
                Ok((result, return_type))
            }
            Expr::LambdaFunctionCall {
                name,
                args,
                lambda_expr,
                return_type,
                ..
            } => {
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
                    self.run_lambda(name, args, lambda_expr, return_type)?,
                    return_type.clone(),
                ))
            }
        };

        #[cfg(debug_assertions)]
        if result.is_err() {
            use std::sync::atomic::AtomicBool;
            use std::sync::atomic::Ordering;

            static RECURSING: AtomicBool = AtomicBool::new(false);
            if RECURSING
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                assert_eq!(
                    ConstantFolder::fold_with_domain(
                        expr,
                        &self.data_block.domains().into_iter().enumerate().collect(),
                        self.func_ctx,
                        self.fn_registry
                    )
                    .1,
                    None,
                    "domain calculation should not return any domain for expressions that are possible to fail with err {}",
                    result.unwrap_err()
                );
                RECURSING.store(false, Ordering::SeqCst);
            }
        }
        result
    }
}

const MAX_FUNCTION_ARGS_TO_FOLD: usize = 4096;

pub struct ConstantFolder<'a, Index: ColumnIndex> {
    input_domains: &'a HashMap<Index, Domain>,
    func_ctx: &'a FunctionContext,
    fn_registry: &'a FunctionRegistry,
}

impl<'a, Index: ColumnIndex> ConstantFolder<'a, Index> {
    /// Fold a single expression, returning the new expression and the domain of the new expression.
    pub fn fold(
        expr: &Expr<Index>,
        func_ctx: &'a FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> (Expr<Index>, Option<Domain>) {
        let input_domains = Self::full_input_domains(expr);

        let folder = ConstantFolder {
            input_domains: &input_domains,
            func_ctx,
            fn_registry,
        };

        folder.fold_to_stable(expr)
    }

    /// Fold a single expression with columns' domain, and then return the new expression and the
    /// domain of the new expression.
    pub fn fold_with_domain(
        expr: &Expr<Index>,
        input_domains: &'a HashMap<Index, Domain>,
        func_ctx: &'a FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> (Expr<Index>, Option<Domain>) {
        let folder = ConstantFolder {
            input_domains,
            func_ctx,
            fn_registry,
        };

        folder.fold_to_stable(expr)
    }

    fn full_input_domains(expr: &Expr<Index>) -> HashMap<Index, Domain> {
        expr.column_refs()
            .into_iter()
            .map(|(id, ty)| {
                let domain = Domain::full(&ty);
                (id, domain)
            })
            .collect()
    }

    /// Running `fold_once()` for only one time may not reach the simplest form of expression,
    /// therefore we need to call it repeatedly until the expression becomes stable.
    fn fold_to_stable(&self, expr: &Expr<Index>) -> (Expr<Index>, Option<Domain>) {
        const MAX_ITERATIONS: usize = 1024;

        let mut old_expr = expr.clone();
        let mut old_domain = None;
        for _ in 0..MAX_ITERATIONS {
            let (new_expr, new_domain) = self.fold_once(&old_expr);

            if new_expr == old_expr {
                return (new_expr, new_domain);
            }
            old_expr = new_expr;
            old_domain = new_domain;
        }

        error!("maximum iterations reached while folding expression");

        (old_expr, old_domain)
    }

    /// Fold expression by one step, specifically, by reducing expression by domain calculation and then
    /// folding the function calls whose all arguments are constants.
    fn fold_once(&self, expr: &Expr<Index>) -> (Expr<Index>, Option<Domain>) {
        let (new_expr, domain) = match expr {
            Expr::Constant {
                scalar, data_type, ..
            } => (expr.clone(), Some(scalar.as_ref().domain(data_type))),
            Expr::ColumnRef {
                span,
                id,
                data_type,
                ..
            } => {
                let domain = &self.input_domains[id];
                let expr = domain
                    .as_singleton()
                    .map(|scalar| Expr::Constant {
                        span: *span,
                        scalar,
                        data_type: data_type.clone(),
                    })
                    .unwrap_or_else(|| expr.clone());
                (expr, Some(domain.clone()))
            }
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => {
                let (inner_expr, inner_domain) = self.fold_once(expr);

                let new_domain = if *is_try {
                    inner_domain.and_then(|inner_domain| {
                        self.calculate_try_cast(*span, expr.data_type(), dest_type, &inner_domain)
                    })
                } else {
                    inner_domain.and_then(|inner_domain| {
                        self.calculate_cast(*span, expr.data_type(), dest_type, &inner_domain)
                    })
                };

                let cast_expr = Expr::Cast {
                    span: *span,
                    is_try: *is_try,
                    expr: Box::new(inner_expr.clone()),
                    dest_type: dest_type.clone(),
                };

                if inner_expr.as_constant().is_some() {
                    let block = DataBlock::empty();
                    let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
                    // Since we know the expression is constant, it'll be safe to change its column index type.
                    let cast_expr = cast_expr.project_column_ref(|_| unreachable!());
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&cast_expr) {
                        return (
                            Expr::Constant {
                                span: *span,
                                scalar,
                                data_type: dest_type.clone(),
                            },
                            None,
                        );
                    }
                }

                (
                    new_domain
                        .as_ref()
                        .and_then(Domain::as_singleton)
                        .map(|scalar| Expr::Constant {
                            span: *span,
                            scalar,
                            data_type: dest_type.clone(),
                        })
                        .unwrap_or(cast_expr),
                    new_domain,
                )
            }
            Expr::FunctionCall {
                span,
                id,
                function,
                generics,
                args,
                return_type,
            } if function.signature.name == "and_filters" => {
                if args.len() > MAX_FUNCTION_ARGS_TO_FOLD {
                    return (expr.clone(), None);
                }

                let mut args_expr = Vec::new();
                let mut result_domain = Some(BooleanDomain {
                    has_true: true,
                    has_false: true,
                });

                for arg in args {
                    let (expr, domain) = self.fold_once(arg);
                    // A temporary hack to make `and_filters` shortcut on false.
                    // TODO(andylokandy): make it a rule in the optimizer.
                    if let Expr::Constant {
                        scalar: Scalar::Boolean(false),
                        ..
                    } = &expr
                    {
                        return (
                            Expr::Constant {
                                span: *span,
                                scalar: Scalar::Boolean(false),
                                data_type: DataType::Boolean,
                            },
                            None,
                        );
                    }
                    args_expr.push(expr);

                    result_domain = result_domain.zip(domain).map(|(func_domain, domain)| {
                        let (domain_has_true, domain_has_false) = match &domain {
                            Domain::Boolean(boolean_domain) => {
                                (boolean_domain.has_true, boolean_domain.has_false)
                            }
                            Domain::Nullable(nullable_domain) => match &nullable_domain.value {
                                Some(inner_domain) => {
                                    let boolean_domain = inner_domain.as_boolean().unwrap();
                                    (
                                        boolean_domain.has_true,
                                        nullable_domain.has_null || boolean_domain.has_false,
                                    )
                                }
                                None => (false, true),
                            },
                            _ => unreachable!(),
                        };
                        BooleanDomain {
                            has_true: func_domain.has_true && domain_has_true,
                            has_false: func_domain.has_false || domain_has_false,
                        }
                    });

                    if let Some(Scalar::Boolean(false)) = result_domain
                        .as_ref()
                        .and_then(|domain| Domain::Boolean(*domain).as_singleton())
                    {
                        return (
                            Expr::Constant {
                                span: *span,
                                scalar: Scalar::Boolean(false),
                                data_type: DataType::Boolean,
                            },
                            None,
                        );
                    }
                }

                if let Some(scalar) = result_domain
                    .as_ref()
                    .and_then(|domain| Domain::Boolean(*domain).as_singleton())
                {
                    return (
                        Expr::Constant {
                            span: *span,
                            scalar,
                            data_type: DataType::Boolean,
                        },
                        None,
                    );
                }

                let all_args_is_scalar = args_expr.iter().all(|arg| arg.as_constant().is_some());

                let func_expr = Expr::FunctionCall {
                    span: *span,
                    id: id.clone(),
                    function: function.clone(),
                    generics: generics.clone(),
                    args: args_expr,
                    return_type: return_type.clone(),
                };

                if all_args_is_scalar {
                    let block = DataBlock::empty();
                    let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
                    // Since we know the expression is constant, it'll be safe to change its column index type.
                    let func_expr = func_expr.project_column_ref(|_| unreachable!());
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&func_expr) {
                        return (
                            Expr::Constant {
                                span: *span,
                                scalar,
                                data_type: return_type.clone(),
                            },
                            None,
                        );
                    }
                }

                (func_expr, result_domain.map(Domain::Boolean))
            }
            Expr::FunctionCall {
                span,
                id,
                function,
                generics,
                args,
                return_type,
            } => {
                if args.len() > MAX_FUNCTION_ARGS_TO_FOLD {
                    return (expr.clone(), None);
                }

                let (mut args_expr, mut args_domain) = (
                    Vec::with_capacity(args.len()),
                    Some(Vec::with_capacity(args.len())),
                );
                for arg in args {
                    let (expr, domain) = self.fold_once(arg);
                    args_expr.push(expr);
                    args_domain = args_domain.zip(domain).map(|(mut domains, domain)| {
                        domains.push(domain);
                        domains
                    });
                }
                let all_args_is_scalar = args_expr.iter().all(|arg| arg.as_constant().is_some());

                let func_expr = Expr::FunctionCall {
                    span: *span,
                    id: id.clone(),
                    function: function.clone(),
                    generics: generics.clone(),
                    args: args_expr,
                    return_type: return_type.clone(),
                };

                let calc_domain = match &function.eval {
                    FunctionEval::Scalar { calc_domain, .. } => calc_domain,
                    FunctionEval::SRF { .. } => {
                        return (func_expr, None);
                    }
                };

                let func_domain =
                    args_domain.and_then(|domains| match (calc_domain)(self.func_ctx, &domains) {
                        FunctionDomain::MayThrow => None,
                        FunctionDomain::Full => Some(Domain::full(return_type)),
                        FunctionDomain::Domain(domain) => Some(domain),
                    });

                if let Some(scalar) = func_domain.as_ref().and_then(Domain::as_singleton) {
                    return (
                        Expr::Constant {
                            span: *span,
                            scalar,
                            data_type: return_type.clone(),
                        },
                        None,
                    );
                }

                if all_args_is_scalar {
                    let block = DataBlock::empty();
                    let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
                    // Since we know the expression is constant, it'll be safe to change its column index type.
                    let func_expr = func_expr.project_column_ref(|_| unreachable!());
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&func_expr) {
                        return (
                            Expr::Constant {
                                span: *span,
                                scalar,
                                data_type: return_type.clone(),
                            },
                            None,
                        );
                    }
                }

                (func_expr, func_domain)
            }
            Expr::LambdaFunctionCall {
                span,
                name,
                args,
                lambda_expr,
                lambda_display,
                return_type,
            } => {
                if args.len() > MAX_FUNCTION_ARGS_TO_FOLD {
                    return (expr.clone(), None);
                }

                let mut args_expr = Vec::with_capacity(args.len());
                for arg in args {
                    let (expr, _) = self.fold_once(arg);
                    args_expr.push(expr);
                }
                let all_args_is_scalar = args_expr.iter().all(|arg| arg.as_constant().is_some());

                let func_expr = Expr::LambdaFunctionCall {
                    span: *span,
                    name: name.clone(),
                    args: args_expr,
                    lambda_expr: lambda_expr.clone(),
                    lambda_display: lambda_display.clone(),
                    return_type: return_type.clone(),
                };

                if all_args_is_scalar {
                    let block = DataBlock::empty();
                    let evaluator = Evaluator::new(&block, self.func_ctx, self.fn_registry);
                    // Since we know the expression is constant, it'll be safe to change its column index type.
                    let func_expr = func_expr.project_column_ref(|_| unreachable!());
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&func_expr) {
                        return (
                            Expr::Constant {
                                span: *span,
                                scalar,
                                data_type: return_type.clone(),
                            },
                            None,
                        );
                    }
                }
                (func_expr, None)
            }
        };

        debug_assert_eq!(expr.data_type(), new_expr.data_type());

        (new_expr, domain)
    }

    fn calculate_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        domain: &Domain,
    ) -> Option<Domain> {
        if src_type == dest_type {
            return Some(domain.clone());
        }

        if let Some(cast_fn) = get_simple_cast_function(false, src_type, dest_type) {
            if let Some(new_domain) =
                self.calculate_simple_cast(span, src_type, dest_type, domain, &cast_fn)
            {
                return new_domain;
            }
        }

        match (src_type, dest_type) {
            (DataType::Null, DataType::Nullable(_)) => Some(domain.clone()),
            (DataType::Nullable(inner_src_ty), DataType::Nullable(inner_dest_ty)) => {
                let domain = domain.as_nullable().unwrap();
                let value = match &domain.value {
                    Some(value) => Some(Box::new(self.calculate_cast(
                        span,
                        inner_src_ty,
                        inner_dest_ty,
                        value,
                    )?)),
                    None => None,
                };
                Some(Domain::Nullable(NullableDomain {
                    has_null: domain.has_null,
                    value,
                }))
            }
            (_, DataType::Nullable(inner_dest_ty)) => Some(Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(Box::new(self.calculate_cast(
                    span,
                    src_type,
                    inner_dest_ty,
                    domain,
                )?)),
            })),

            (DataType::EmptyArray, DataType::Array(_)) => Some(domain.clone()),
            (DataType::Array(inner_src_ty), DataType::Array(inner_dest_ty)) => {
                let inner_domain = match domain.as_array().unwrap() {
                    Some(inner_domain) => Some(Box::new(self.calculate_cast(
                        span,
                        inner_src_ty,
                        inner_dest_ty,
                        inner_domain,
                    )?)),
                    None => None,
                };
                Some(Domain::Array(inner_domain))
            }

            (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty))
                if fields_src_ty.len() == fields_dest_ty.len() =>
            {
                Some(Domain::Tuple(
                    domain
                        .as_tuple()
                        .unwrap()
                        .iter()
                        .zip(fields_src_ty)
                        .zip(fields_dest_ty)
                        .map(|((field_domain, src_ty), dest_ty)| {
                            self.calculate_cast(span, src_ty, dest_ty, field_domain)
                        })
                        .collect::<Option<Vec<_>>>()?,
                ))
            }

            _ => None,
        }
    }

    fn calculate_try_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        domain: &Domain,
    ) -> Option<Domain> {
        if src_type == dest_type {
            return Some(domain.clone());
        }

        // The dest_type of `TRY_CAST` must be `Nullable`, which is guaranteed by the type checker.
        let inner_dest_type = &**dest_type.as_nullable().unwrap();

        if let Some(cast_fn) = get_simple_cast_function(true, src_type, inner_dest_type) {
            if let Some(new_domain) =
                self.calculate_simple_cast(span, src_type, dest_type, domain, &cast_fn)
            {
                return new_domain;
            }
        }

        match (src_type, inner_dest_type) {
            (DataType::Null, _) => Some(domain.clone()),
            (DataType::Nullable(inner_src_ty), _) => {
                let nullable_domain = domain.as_nullable().unwrap();
                match &nullable_domain.value {
                    Some(value) => {
                        let new_domain = self
                            .calculate_try_cast(span, inner_src_ty, dest_type, value)?
                            .into_nullable()
                            .unwrap();
                        Some(Domain::Nullable(NullableDomain {
                            has_null: nullable_domain.has_null || new_domain.has_null,
                            value: new_domain.value,
                        }))
                    }
                    None => Some(domain.clone()),
                }
            }
            (src_ty, inner_dest_ty) if src_ty == inner_dest_ty => {
                Some(Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain.clone())),
                }))
            }

            (DataType::EmptyArray, DataType::Array(_)) => Some(Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(Box::new(domain.clone())),
            })),
            (DataType::Array(inner_src_ty), DataType::Array(inner_dest_ty)) => {
                let inner_domain = match domain.as_array().unwrap() {
                    Some(inner_domain) => Some(Box::new(self.calculate_try_cast(
                        span,
                        inner_src_ty,
                        inner_dest_ty,
                        inner_domain,
                    )?)),
                    None => None,
                };
                Some(Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(Domain::Array(inner_domain))),
                }))
            }

            (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty))
                if fields_src_ty.len() == fields_dest_ty.len() =>
            {
                let fields_domain = domain.as_tuple().unwrap();
                let new_fields_domain = fields_domain
                    .iter()
                    .zip(fields_src_ty)
                    .zip(fields_dest_ty)
                    .map(|((domain, src_ty), dest_ty)| {
                        self.calculate_try_cast(span, src_ty, dest_ty, domain)
                    })
                    .collect::<Option<_>>()?;
                Some(Domain::Tuple(new_fields_domain))
            }

            _ => None,
        }
    }

    fn calculate_simple_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        domain: &Domain,
        cast_fn: &str,
    ) -> Option<Option<Domain>> {
        let expr = Expr::ColumnRef {
            span,
            id: 0,
            data_type: src_type.clone(),
            display_name: String::new(),
        };

        let params = if let DataType::Decimal(ty) = dest_type {
            vec![
                Scalar::Number(NumberScalar::Int64(ty.precision() as _)),
                Scalar::Number(NumberScalar::Int64(ty.scale() as _)),
            ]
        } else {
            vec![]
        };
        let cast_expr = check_function(span, cast_fn, &params, &[expr], self.fn_registry).ok()?;

        if cast_expr.data_type() != dest_type {
            return None;
        }

        let (_, output_domain) = ConstantFolder::fold_with_domain(
            &cast_expr,
            &[(0, domain.clone())].into_iter().collect(),
            self.func_ctx,
            self.fn_registry,
        );

        Some(output_domain)
    }
}
