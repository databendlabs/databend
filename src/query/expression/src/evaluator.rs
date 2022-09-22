// Copyright 2022 Datafuse Labs.
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

#[cfg(debug_assertions)]
use std::sync::Mutex;

use common_arrow::arrow::bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use itertools::Itertools;
use num_traits::AsPrimitive;

use crate::chunk::Chunk;
use crate::error::Result;
use crate::expression::Expr;
use crate::expression::Span;
use crate::function::FunctionContext;
use crate::property::Domain;
use crate::types::any::AnyType;
use crate::types::array::ArrayColumn;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableDomain;
use crate::types::number::NumberColumn;
use crate::types::number::NumberDataType;
use crate::types::number::NumberDomain;
use crate::types::number::NumberScalar;
use crate::types::DataType;
use crate::util::constant_bitmap;
use crate::values::Column;
use crate::values::ColumnBuilder;
use crate::values::Scalar;
use crate::values::Value;
use crate::with_number_type;
use crate::ScalarRef;

pub struct Evaluator<'a> {
    input_columns: &'a Chunk,
    #[allow(dead_code)]
    context: FunctionContext,
}

impl<'a> Evaluator<'a> {
    pub fn new(input_columns: &'a Chunk, context: FunctionContext) -> Self {
        Evaluator {
            input_columns,
            context,
        }
    }

    pub fn run(&self, expr: &Expr) -> Result<Value<AnyType>> {
        let result = match expr {
            Expr::Constant { scalar, .. } => Ok(Value::Scalar(scalar.clone())),
            Expr::ColumnRef { id, .. } => Ok(self.input_columns.columns()[*id].clone()),
            Expr::FunctionCall {
                span,
                function,
                args,
                generics,
                ..
            } => {
                let cols = args
                    .iter()
                    .map(|expr| self.run(expr))
                    .collect::<Result<Vec<_>>>()?;
                assert!(
                    cols.iter()
                        .filter_map(|val| match val {
                            Value::Column(col) => Some(col.len()),
                            Value::Scalar(_) => None,
                        })
                        .all_equal()
                );
                let cols_ref = cols.iter().map(Value::as_ref).collect::<Vec<_>>();
                (function.eval)(cols_ref.as_slice(), generics, self.input_columns.num_rows()).map_err(|msg| (span.clone(), msg))
            }
            Expr::Cast {
                span,
                expr,
                dest_type,
            } => {
                let value = self.run(expr)?;
                match value {
                    Value::Scalar(scalar) => Ok(Value::Scalar(self.run_cast_scalar(
                        span.clone(),
                        scalar,
                        dest_type,
                    )?)),
                    Value::Column(col) => Ok(Value::Column(self.run_cast_column(
                        span.clone(),
                        col,
                        dest_type,
                    )?)),
                }
            }
            Expr::TryCast {
                span,
                expr,
                dest_type,
            } => {
                let value = self.run(expr)?;
                match value {
                    Value::Scalar(scalar) => Ok(Value::Scalar(self.run_try_cast_scalar(
                        span.clone(),
                        scalar,
                        dest_type,
                    ))),
                    Value::Column(col) => Ok(Value::Column(self.run_try_cast_column(
                        span.clone(),
                        col,
                        dest_type,
                    ))),
                }
            }
        };

        #[cfg(debug_assertions)]
        if result.is_err() {
            static RECURSING: Mutex<bool> = Mutex::new(false);
            if !*RECURSING.lock().unwrap() {
                *RECURSING.lock().unwrap() = true;
                assert_eq!(
                    ConstantFolder::new(&self.input_columns.domains(), FunctionContext::default())
                        .fold(expr)
                        .1,
                    None,
                    "domain calculation should not return any domain for expressions that are possible to fail"
                );
                *RECURSING.lock().unwrap() = false;
            }
        }

        result
    }

    pub fn run_cast_scalar(
        &self,
        span: Span,
        scalar: Scalar,
        dest_type: &DataType,
    ) -> Result<Scalar> {
        match (scalar, dest_type) {
            (Scalar::Null, DataType::Nullable(_)) => Ok(Scalar::Null),
            (Scalar::EmptyArray, DataType::Array(dest_ty)) => {
                let new_column = ColumnBuilder::with_capacity(dest_ty, 0).build();
                Ok(Scalar::Array(new_column))
            }
            (scalar, DataType::Nullable(dest_ty)) => self.run_cast_scalar(span, scalar, dest_ty),
            (Scalar::Array(array), DataType::Array(dest_ty)) => {
                let new_array = self.run_cast_column(span, array, dest_ty)?;
                Ok(Scalar::Array(new_array))
            }
            (Scalar::Tuple(fields), DataType::Tuple(fields_ty)) => {
                let new_fields = fields
                    .into_iter()
                    .zip(fields_ty.iter())
                    .map(|(field, dest_ty)| self.run_cast_scalar(span.clone(), field, dest_ty))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Scalar::Tuple(new_fields))
            }

            (Scalar::Number(num), DataType::Number(dest_ty)) => {
                let new_number = with_number_type!(|SRC_TYPE| match num {
                    NumberScalar::SRC_TYPE(value) => {
                        with_number_type!(|DEST_TYPE| match dest_ty {
                            NumberDataType::DEST_TYPE => {
                                if NumberDataType::SRC_TYPE.can_lossless_cast_to(*dest_ty) {
                                    NumberScalar::DEST_TYPE(value.as_())
                                } else {
                                    let value = num_traits::cast::cast(value).ok_or_else(|| {
                                        (
                                            span.clone(),
                                            format!(
                                                "unable to cast {} to {}",
                                                ScalarRef::Number(num),
                                                stringify!(DEST_TYPE)
                                            ),
                                        )
                                    })?;
                                    NumberScalar::DEST_TYPE(value)
                                }
                            }
                        })
                    }
                });
                Ok(Scalar::Number(new_number))
            }

            // identical types
            (scalar @ Scalar::Null, DataType::Null)
            | (scalar @ Scalar::EmptyArray, DataType::EmptyArray)
            | (scalar @ Scalar::Boolean(_), DataType::Boolean)
            | (scalar @ Scalar::String(_), DataType::String)
            | (scalar @ Scalar::Timestamp(_), DataType::Timestamp) => Ok(scalar),

            (scalar, dest_ty) => Err((
                span,
                (format!("unable to cast {} to {dest_ty}", scalar.as_ref())),
            )),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn run_cast_column(
        &self,
        span: Span,
        column: Column,
        dest_type: &DataType,
    ) -> Result<Column> {
        match (column, dest_type) {
            (Column::Null { len }, DataType::Nullable(_)) => {
                let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                for _ in 0..len {
                    builder.push_default();
                }
                Ok(builder.build())
            }
            (Column::EmptyArray { len }, DataType::Array(_)) => {
                let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                for _ in 0..len {
                    builder.push_default();
                }
                Ok(builder.build())
            }
            (Column::Nullable(box col), DataType::Nullable(dest_ty)) => {
                let column = self.run_cast_column(span, col.column, dest_ty)?;
                Ok(Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: col.validity,
                })))
            }
            (col, DataType::Nullable(dest_ty)) => {
                let column = self.run_cast_column(span, col, dest_ty)?;
                Ok(Column::Nullable(Box::new(NullableColumn {
                    validity: constant_bitmap(true, column.len()).into(),
                    column,
                })))
            }
            (Column::Array(col), DataType::Array(dest_ty)) => {
                let values = self.run_cast_column(span, col.values, dest_ty)?;
                Ok(Column::Array(Box::new(ArrayColumn {
                    values,
                    offsets: col.offsets,
                })))
            }
            (Column::Tuple { fields, len }, DataType::Tuple(fields_ty)) => {
                let new_fields = fields
                    .into_iter()
                    .zip(fields_ty)
                    .map(|(field, field_ty)| self.run_cast_column(span.clone(), field, field_ty))
                    .collect::<Result<_>>()?;
                Ok(Column::Tuple {
                    fields: new_fields,
                    len,
                })
            }

            (Column::Number(col), DataType::Number(dest_ty)) => {
                let new_column = with_number_type!(|SRC_TYPE| match col {
                    NumberColumn::SRC_TYPE(col) => {
                        with_number_type!(|DEST_TYPE| match dest_ty {
                            NumberDataType::DEST_TYPE => {
                                if NumberDataType::SRC_TYPE.can_lossless_cast_to(*dest_ty) {
                                    let new_col = col.iter().map(|x| x.as_()).collect::<Vec<_>>();
                                    NumberColumn::DEST_TYPE(new_col.into())
                                } else {
                                    let mut new_col = Vec::with_capacity(col.len());
                                    for &val in col.iter() {
                                        let new_val =
                                            num_traits::cast::cast(val).ok_or_else(|| {
                                                (
                                                    span.clone(),
                                                    format!(
                                                        "unable to cast {} to {}",
                                                        val,
                                                        stringify!(DEST_TYPE)
                                                    ),
                                                )
                                            })?;
                                        new_col.push(new_val);
                                    }
                                    NumberColumn::DEST_TYPE(new_col.into())
                                }
                            }
                        })
                    }
                });
                Ok(Column::Number(new_column))
            }

            // identical types
            (col @ Column::Null { .. }, DataType::Null)
            | (col @ Column::EmptyArray { .. }, DataType::EmptyArray)
            | (col @ Column::Boolean(_), DataType::Boolean)
            | (col @ Column::String { .. }, DataType::String)
            | (col @ Column::Timestamp { .. }, DataType::Timestamp) => Ok(col),

            (col, dest_ty) => Err((span, (format!("unable to cast {col:?} to {dest_ty}")))),
        }
    }

    pub fn run_try_cast_scalar(&self, span: Span, scalar: Scalar, dest_type: &DataType) -> Scalar {
        let inner_type: &DataType = dest_type.as_nullable().unwrap();
        self.run_cast_scalar(span, scalar, inner_type)
            .unwrap_or(Scalar::Null)
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn run_try_cast_column(&self, span: Span, column: Column, dest_type: &DataType) -> Column {
        let inner_type: &DataType = dest_type.as_nullable().unwrap();
        match (column, inner_type) {
            (_, DataType::Null | DataType::Nullable(_)) => {
                unreachable!("inner type can not be nullable")
            }
            (Column::Null { len }, _) => {
                let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                for _ in 0..len {
                    builder.push_default();
                }
                builder.build()
            }
            (Column::EmptyArray { len }, DataType::Array(_)) => {
                let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                for _ in 0..len {
                    builder.push_default();
                }
                builder.build()
            }
            (Column::Nullable(box col), _) => {
                let new_col = *self
                    .run_try_cast_column(span, col.column, dest_type)
                    .into_nullable()
                    .unwrap();
                Column::Nullable(Box::new(NullableColumn {
                    column: new_col.column,
                    validity: bitmap::or(&col.validity, &new_col.validity),
                }))
            }
            (Column::Array(col), DataType::Array(dest_ty)) => {
                let new_values = self.run_try_cast_column(span, col.values, dest_ty);
                let new_col = Column::Array(Box::new(ArrayColumn {
                    values: new_values,
                    offsets: col.offsets,
                }));
                Column::Nullable(Box::new(NullableColumn {
                    validity: constant_bitmap(true, new_col.len()).into(),
                    column: new_col,
                }))
            }
            (Column::Tuple { fields, len }, DataType::Tuple(fields_ty)) => {
                let new_fields = fields
                    .into_iter()
                    .zip(fields_ty)
                    .map(|(field, field_ty)| {
                        self.run_try_cast_column(span.clone(), field, field_ty)
                    })
                    .collect();
                let new_col = Column::Tuple {
                    fields: new_fields,
                    len,
                };
                Column::Nullable(Box::new(NullableColumn {
                    validity: constant_bitmap(true, len).into(),
                    column: new_col,
                }))
            }

            (Column::Number(col), DataType::Number(dest_ty)) => {
                with_number_type!(|SRC_TYPE| match &col {
                    NumberColumn::SRC_TYPE(col) => {
                        with_number_type!(|DEST_TYPE| match dest_ty {
                            NumberDataType::DEST_TYPE => {
                                if NumberDataType::SRC_TYPE.can_lossless_cast_to(*dest_ty) {
                                    let new_col = col.iter().map(|x| x.as_()).collect::<Vec<_>>();
                                    Column::Nullable(Box::new(NullableColumn {
                                        validity: constant_bitmap(true, new_col.len()).into(),
                                        column: Column::Number(NumberColumn::DEST_TYPE(
                                            new_col.into(),
                                        )),
                                    }))
                                } else {
                                    let mut new_col = Vec::with_capacity(col.len());
                                    let mut validity = MutableBitmap::with_capacity(col.len());
                                    for &val in col.iter() {
                                        if let Some(new_val) = num_traits::cast::cast(val) {
                                            new_col.push(new_val);
                                            validity.push(true);
                                        } else {
                                            new_col.push(Default::default());
                                            validity.push(false);
                                        }
                                    }
                                    Column::Nullable(Box::new(NullableColumn {
                                        validity: validity.into(),
                                        column: Column::Number(NumberColumn::DEST_TYPE(
                                            new_col.into(),
                                        )),
                                    }))
                                }
                            }
                        })
                    }
                })
            }

            // identical types
            (column @ Column::Boolean(_), DataType::Boolean)
            | (column @ Column::String { .. }, DataType::String)
            | (column @ Column::EmptyArray { .. }, DataType::EmptyArray)
            | (column @ Column::Timestamp { .. }, DataType::Timestamp) => {
                Column::Nullable(Box::new(NullableColumn {
                    validity: constant_bitmap(true, column.len()).into(),
                    column,
                }))
            }

            // failure cases
            (col, _) => {
                let len = col.len();
                let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                for _ in 0..len {
                    builder.push_default();
                }
                builder.build()
            }
        }
    }
}

pub struct ConstantFolder<'a> {
    input_domains: &'a [Domain],
    context: FunctionContext,
}

impl<'a> ConstantFolder<'a> {
    pub fn new(input_domains: &'a [Domain], context: FunctionContext) -> Self {
        ConstantFolder {
            input_domains,
            context,
        }
    }

    pub fn fold(&self, expr: &Expr) -> (Expr, Option<Domain>) {
        match expr {
            Expr::Constant { scalar, .. } => (expr.clone(), Some(scalar.as_ref().domain())),
            Expr::ColumnRef { span, id } => {
                let domain = &self.input_domains[*id];
                let expr = domain
                    .as_singleton()
                    .map(|scalar| Expr::Constant {
                        span: span.clone(),
                        scalar,
                    })
                    .unwrap_or_else(|| expr.clone());
                (expr, Some(domain.clone()))
            }
            Expr::Cast {
                span,
                expr,
                dest_type,
            } => {
                let (inner_expr, inner_domain) = self.fold(expr);
                let cast_domain = inner_domain.and_then(|inner_domain| {
                    self.calculate_cast(span.clone(), &inner_domain, dest_type)
                });
                let cast_expr = cast_domain
                    .as_ref()
                    .and_then(Domain::as_singleton)
                    .map(|scalar| Expr::Constant {
                        span: span.clone(),
                        scalar,
                    })
                    .unwrap_or_else(|| Expr::Cast {
                        span: span.clone(),
                        expr: Box::new(inner_expr),
                        dest_type: dest_type.clone(),
                    });
                (cast_expr, cast_domain)
            }
            Expr::TryCast {
                span,
                expr,
                dest_type,
            } => {
                let (inner_expr, inner_domain) = self.fold(expr);
                let try_cast_domain = inner_domain.map(|inner_domain| {
                    self.calculate_try_cast(span.clone(), &inner_domain, dest_type)
                });
                let try_cast_expr = try_cast_domain
                    .as_ref()
                    .and_then(Domain::as_singleton)
                    .map(|scalar| Expr::Constant {
                        span: span.clone(),
                        scalar,
                    })
                    .unwrap_or_else(|| Expr::TryCast {
                        span: span.clone(),
                        expr: Box::new(inner_expr),
                        dest_type: dest_type.clone(),
                    });
                (try_cast_expr, try_cast_domain)
            }
            Expr::FunctionCall {
                span,
                id,
                function,
                generics,
                args,
            } => {
                let (mut args_expr, mut args_domain) = (Vec::new(), Some(Vec::new()));
                for arg in args {
                    let (expr, domain) = self.fold(arg);
                    args_expr.push(expr);
                    args_domain = args_domain.zip(domain).map(|(mut domains, domain)| {
                        domains.push(domain);
                        domains
                    });
                }

                let func_domain =
                    args_domain.and_then(|domains| (function.calc_domain)(&domains, generics));
                let all_args_is_scalar = args_expr.iter().all(|arg| arg.as_constant().is_some());

                let func_expr = Expr::FunctionCall {
                    span: span.clone(),
                    id: id.clone(),
                    function: function.clone(),
                    generics: generics.clone(),
                    args: args_expr,
                };

                if let Some(scalar) = func_domain.as_ref().and_then(Domain::as_singleton) {
                    return (
                        Expr::Constant {
                            span: span.clone(),
                            scalar,
                        },
                        func_domain,
                    );
                }

                if all_args_is_scalar {
                    let chunk = Chunk::empty();
                    let evaluator = Evaluator::new(&chunk, self.context.clone());
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&func_expr) {
                        return (
                            Expr::Constant {
                                span: span.clone(),
                                scalar,
                            },
                            func_domain,
                        );
                    }
                }

                (func_expr, func_domain)
            }
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn calculate_cast(
        &self,
        span: Span,
        domain: &Domain,
        dest_type: &DataType,
    ) -> Option<Domain> {
        match (domain, dest_type) {
            (
                Domain::Nullable(NullableDomain { value: None, .. }),
                DataType::Null | DataType::Nullable(_),
            ) => Some(domain.clone()),
            (Domain::Array(None), DataType::EmptyArray | DataType::Array(_)) => {
                Some(Domain::Array(None))
            }
            (
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(value),
                }),
                DataType::Nullable(ty),
            ) => Some(Domain::Nullable(NullableDomain {
                has_null: *has_null,
                value: Some(Box::new(self.calculate_cast(span, value, ty)?)),
            })),
            (domain, DataType::Nullable(ty)) => Some(Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(Box::new(self.calculate_cast(span, domain, ty)?)),
            })),
            (Domain::Array(Some(domain)), DataType::Array(ty)) => Some(Domain::Array(Some(
                Box::new(self.calculate_cast(span, domain, ty)?),
            ))),
            (Domain::Tuple(fields), DataType::Tuple(fields_ty)) => Some(Domain::Tuple(
                fields
                    .iter()
                    .zip(fields_ty)
                    .map(|(field, ty)| self.calculate_cast(span.clone(), field, ty))
                    .collect::<Option<Vec<_>>>()?,
            )),

            (Domain::Number(domain), DataType::Number(dest_ty)) => {
                with_number_type!(|SRC_TYPE| match domain {
                    NumberDomain::SRC_TYPE(domain) => {
                        with_number_type!(|DEST_TYPE| match dest_ty {
                            NumberDataType::DEST_TYPE => {
                                let (domain, overflowing) = domain.overflow_cast();
                                if overflowing {
                                    None
                                } else {
                                    Some(Domain::Number(NumberDomain::DEST_TYPE(domain)))
                                }
                            }
                        })
                    }
                })
            }

            // identical types
            (Domain::Boolean(_), DataType::Boolean)
            | (Domain::String(_), DataType::String)
            | (Domain::Timestamp(_), DataType::Timestamp) => Some(domain.clone()),

            // failure cases
            _ => None,
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn calculate_try_cast(&self, span: Span, domain: &Domain, dest_type: &DataType) -> Domain {
        let inner_type: &DataType = dest_type.as_nullable().unwrap();
        match (domain, inner_type) {
            (_, DataType::Null | DataType::Nullable(_)) => {
                unreachable!("inner type cannot be nullable")
            }
            (Domain::Array(None), DataType::EmptyArray | DataType::Array(_)) => {
                Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(Domain::Array(None))),
                })
            }
            (
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(value),
                }),
                _,
            ) => {
                let inner_domain = self
                    .calculate_try_cast(span, value, dest_type)
                    .into_nullable()
                    .unwrap();
                Domain::Nullable(NullableDomain {
                    has_null: *has_null || inner_domain.has_null,
                    value: inner_domain.value,
                })
            }
            (Domain::Array(Some(domain)), DataType::Array(ty)) => {
                let inner_domain = self.calculate_try_cast(span, domain, ty);
                Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(Domain::Array(Some(Box::new(inner_domain))))),
                })
            }
            (Domain::Tuple(fields), DataType::Tuple(fields_ty)) => {
                let new_fields = fields
                    .iter()
                    .zip(fields_ty)
                    .map(|(field, ty)| self.calculate_try_cast(span.clone(), field, ty))
                    .collect();
                Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(Domain::Tuple(new_fields))),
                })
            }

            (Domain::Number(domain), DataType::Number(dest_ty)) => {
                with_number_type!(|SRC_TYPE| match domain {
                    NumberDomain::SRC_TYPE(domain) => {
                        with_number_type!(|DEST_TYPE| match dest_ty {
                            NumberDataType::DEST_TYPE => {
                                let (domain, overflowing) = domain.overflow_cast();
                                Domain::Nullable(NullableDomain {
                                    has_null: overflowing,
                                    value: Some(Box::new(Domain::Number(NumberDomain::DEST_TYPE(
                                        domain,
                                    )))),
                                })
                            }
                        })
                    }
                })
            }

            // identical types
            (Domain::Boolean(_), DataType::Boolean)
            | (Domain::String(_), DataType::String)
            | (Domain::Timestamp(_), DataType::Timestamp) => Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(Box::new(domain.clone())),
            }),

            // failure cases
            _ => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
        }
    }
}
