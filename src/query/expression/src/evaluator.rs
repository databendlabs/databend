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

use std::collections::HashMap;
#[cfg(debug_assertions)]
use std::sync::Mutex;

use common_arrow::arrow::bitmap;
use itertools::Itertools;

use crate::chunk::Chunk;
use crate::expression::Expr;
use crate::expression::Span;
use crate::function::EvalContext;
use crate::property::Domain;
use crate::type_check::check_simple_cast;
use crate::types::any::AnyType;
use crate::types::array::ArrayColumn;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableDomain;
use crate::types::DataType;
use crate::utils::arrow::constant_bitmap;
use crate::utils::calculate_function_domain;
use crate::utils::eval_function;
use crate::values::Column;
use crate::values::ColumnBuilder;
use crate::values::Scalar;
use crate::values::Value;
use crate::ColumnIndex;
use crate::FunctionContext;
use crate::FunctionDomain;
use crate::FunctionRegistry;
use crate::Result;

pub struct Evaluator<'a, Index: ColumnIndex> {
    input_columns: &'a Chunk<Index>,
    fn_ctx: FunctionContext,
    fn_registry: &'a FunctionRegistry,
}

impl<'a, Index: ColumnIndex> Evaluator<'a, Index> {
    pub fn new(
        input_columns: &'a Chunk<Index>,
        fn_ctx: FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> Self {
        Evaluator {
            input_columns,
            fn_ctx,
            fn_registry,
        }
    }

    pub fn run(&self, expr: &Expr<Index>) -> Result<Value<AnyType>> {
        let result = match expr {
            Expr::Constant { scalar, .. } => Ok(Value::Scalar(scalar.clone())),
            Expr::ColumnRef { id, .. } => Ok(self.input_columns.get_by_id(id).value.clone()),
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
                let ctx = EvalContext {
                    generics,
                    num_rows: self.input_columns.num_rows(),
                    tz: self.fn_ctx.tz,
                };
                (function.eval)(cols_ref.as_slice(), ctx).map_err(|msg| (span.clone(), msg))
            }
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => {
                let value = self.run(expr)?;
                if *is_try {
                    Ok(self.run_try_cast(span.clone(), expr.data_type(), dest_type, value))
                } else {
                    self.run_cast(span.clone(), expr.data_type(), dest_type, value)
                }
            }
        };

        #[cfg(debug_assertions)]
        if result.is_err() {
            static RECURSING: Mutex<bool> = Mutex::new(false);
            if !*RECURSING.lock().unwrap() {
                *RECURSING.lock().unwrap() = true;
                assert_eq!(
                    ConstantFolder::new(
                        self.input_columns.domains(),
                        self.fn_ctx,
                        self.fn_registry
                    )
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

    fn run_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        value: Value<AnyType>,
    ) -> Result<Value<AnyType>> {
        if src_type == dest_type {
            return Ok(value);
        }

        if let Some(cast_fn) = check_simple_cast(false, dest_type) {
            return self.run_simple_cast(span, src_type, dest_type, value, &cast_fn);
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
                Value::Scalar(_) => self.run_cast(span, inner_src_ty, inner_dest_ty, value),
                Value::Column(Column::Nullable(col)) => {
                    let column = self
                        .run_cast(span, inner_src_ty, inner_dest_ty, Value::Column(col.column))?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(Column::Nullable(Box::new(NullableColumn {
                        column,
                        validity: col.validity,
                    }))))
                }
                _ => unreachable!(),
            },
            (_, DataType::Nullable(inner_dest_ty)) => match value {
                Value::Scalar(scalar) => {
                    self.run_cast(span, src_type, inner_dest_ty, Value::Scalar(scalar))
                }
                Value::Column(col) => {
                    let column = self
                        .run_cast(span, src_type, inner_dest_ty, Value::Column(col))?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(Column::Nullable(Box::new(NullableColumn {
                        validity: constant_bitmap(true, column.len()).into(),
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
                _ => unreachable!(),
            },
            (DataType::Array(inner_src_ty), DataType::Array(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::Array(array)) => {
                    let new_array = self
                        .run_cast(span, inner_src_ty, inner_dest_ty, Value::Column(array))?
                        .into_column()
                        .unwrap();
                    Ok(Value::Scalar(Scalar::Array(new_array)))
                }
                Value::Column(Column::Array(col)) => {
                    let new_col = self
                        .run_cast(span, inner_src_ty, inner_dest_ty, Value::Column(col.values))?
                        .into_column()
                        .unwrap();
                    Ok(Value::Column(Column::Array(Box::new(ArrayColumn {
                        values: new_col,
                        offsets: col.offsets,
                    }))))
                }
                _ => unreachable!(),
            },

            (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty)) => match value {
                Value::Scalar(Scalar::Tuple(fields)) => {
                    let new_fields = fields
                        .into_iter()
                        .zip(fields_src_ty.iter())
                        .zip(fields_dest_ty.iter())
                        .map(|((field, src_ty), dest_ty)| {
                            self.run_cast(span.clone(), src_ty, dest_ty, Value::Scalar(field))
                                .map(|val| val.into_scalar().unwrap())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Value::Scalar(Scalar::Tuple(new_fields)))
                }
                Value::Column(Column::Tuple { fields, len }) => {
                    let new_fields = fields
                        .into_iter()
                        .zip(fields_src_ty.iter())
                        .zip(fields_dest_ty.iter())
                        .map(|((field, src_ty), dest_ty)| {
                            self.run_cast(span.clone(), src_ty, dest_ty, Value::Column(field))
                                .map(|val| val.into_column().unwrap())
                        })
                        .collect::<Result<_>>()?;
                    Ok(Value::Column(Column::Tuple {
                        fields: new_fields,
                        len,
                    }))
                }
                _ => unreachable!(),
            },
            _ => Err((span, (format!("unable to cast {src_type} to {dest_type}")))),
        }
    }

    fn run_try_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        value: Value<AnyType>,
    ) -> Value<AnyType> {
        if src_type == dest_type {
            return value;
        }

        // The dest_type of `TRY_CAST` must be `Nullable`, which is guaranteed by the type checker.
        let inner_dest_type = &**dest_type.as_nullable().unwrap();
        if let Some(cast_fn) = check_simple_cast(true, inner_dest_type) {
            return self
                .run_simple_cast(span, src_type, dest_type, value, &cast_fn)
                .unwrap();
        }

        match (src_type, inner_dest_type) {
            (DataType::Null, _) => match value {
                Value::Scalar(Scalar::Null) => Value::Scalar(Scalar::Null),
                Value::Column(Column::Null { len }) => {
                    let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                    for _ in 0..len {
                        builder.push_default();
                    }
                    Value::Column(builder.build())
                }
                _ => unreachable!(),
            },
            (DataType::Nullable(inner_src_ty), _) => match value {
                Value::Scalar(Scalar::Null) => Value::Scalar(Scalar::Null),
                Value::Scalar(_) => self.run_try_cast(span, inner_src_ty, inner_dest_type, value),
                Value::Column(Column::Nullable(col)) => {
                    let new_col = *self
                        .run_try_cast(span, inner_src_ty, dest_type, Value::Column(col.column))
                        .into_column()
                        .unwrap()
                        .into_nullable()
                        .unwrap();
                    Value::Column(Column::Nullable(Box::new(NullableColumn {
                        column: new_col.column,
                        validity: bitmap::or(&col.validity, &new_col.validity),
                    })))
                }
                _ => unreachable!(),
            },

            (DataType::EmptyArray, DataType::Array(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::EmptyArray) => {
                    let new_column = ColumnBuilder::with_capacity(inner_dest_ty, 0).build();
                    Value::Scalar(Scalar::Array(new_column))
                }
                Value::Column(Column::EmptyArray { len }) => {
                    let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                    for _ in 0..len {
                        builder.push_default();
                    }
                    Value::Column(builder.build())
                }
                _ => unreachable!(),
            },
            (DataType::Array(inner_src_ty), DataType::Array(inner_dest_ty)) => match value {
                Value::Scalar(Scalar::Array(array)) => {
                    let new_array = self
                        .run_try_cast(span, inner_src_ty, inner_dest_ty, Value::Column(array))
                        .into_column()
                        .unwrap();
                    Value::Scalar(Scalar::Array(new_array))
                }
                Value::Column(Column::Array(col)) => {
                    let new_values = self
                        .run_try_cast(span, inner_src_ty, inner_dest_ty, Value::Column(col.values))
                        .into_column()
                        .unwrap();
                    let new_col = Column::Array(Box::new(ArrayColumn {
                        values: new_values,
                        offsets: col.offsets,
                    }));
                    Value::Column(Column::Nullable(Box::new(NullableColumn {
                        validity: constant_bitmap(true, new_col.len()).into(),
                        column: new_col,
                    })))
                }
                _ => unreachable!(),
            },

            (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty)) => match value {
                Value::Scalar(Scalar::Tuple(fields)) => {
                    let new_fields = fields
                        .into_iter()
                        .zip(fields_src_ty.iter())
                        .zip(fields_dest_ty.iter())
                        .map(|((field, src_ty), dest_ty)| {
                            self.run_try_cast(span.clone(), src_ty, dest_ty, Value::Scalar(field))
                                .into_scalar()
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    Value::Scalar(Scalar::Tuple(new_fields))
                }
                Value::Column(Column::Tuple { fields, len }) => {
                    let new_fields = fields
                        .into_iter()
                        .zip(fields_src_ty.iter())
                        .zip(fields_dest_ty.iter())
                        .map(|((field, src_ty), dest_ty)| {
                            self.run_try_cast(span.clone(), src_ty, dest_ty, Value::Column(field))
                                .into_column()
                                .unwrap()
                        })
                        .collect();
                    let new_col = Column::Tuple {
                        fields: new_fields,
                        len,
                    };
                    Value::Column(new_col)
                }
                _ => unreachable!(),
            },

            _ => match value {
                Value::Scalar(_) => Value::Scalar(Scalar::Null),
                Value::Column(col) => {
                    let mut builder = ColumnBuilder::with_capacity(dest_type, col.len());
                    for _ in 0..col.len() {
                        builder.push_default();
                    }
                    Value::Column(builder.build())
                }
            },
        }
    }

    fn run_simple_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        value: Value<AnyType>,
        cast_fn: &str,
    ) -> Result<Value<AnyType>> {
        let num_rows = match &value {
            Value::Scalar(_) => 1,
            Value::Column(col) => col.len(),
        };
        let (val, ty) = eval_function(
            span,
            cast_fn,
            [(value, src_type.clone())],
            self.fn_ctx,
            num_rows,
            self.fn_registry,
        )?;
        assert_eq!(&ty, dest_type);
        Ok(val)
    }
}

pub struct ConstantFolder<'a, Index: ColumnIndex> {
    input_domains: HashMap<Index, Domain>,
    fn_ctx: FunctionContext,
    fn_registry: &'a FunctionRegistry,
}

impl<'a, Index: ColumnIndex> ConstantFolder<'a, Index> {
    pub fn new(
        input_domains: HashMap<Index, Domain>,
        fn_ctx: FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> Self {
        ConstantFolder {
            input_domains,
            fn_ctx,
            fn_registry,
        }
    }

    pub fn fold(&self, expr: &Expr<Index>) -> (Expr<Index>, Option<Domain>) {
        let (new_expr, domain) = match expr {
            Expr::Constant { scalar, .. } => (expr.clone(), Some(scalar.as_ref().domain())),
            Expr::ColumnRef {
                span,
                id,
                data_type,
            } => {
                let domain = &self.input_domains[id];
                let expr = domain
                    .as_singleton()
                    .map(|scalar| Expr::Constant {
                        span: span.clone(),
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
                let (inner_expr, inner_domain) = self.fold(expr);

                let new_domain = if *is_try {
                    inner_domain.and_then(|inner_domain| {
                        self.calculate_try_cast(
                            span.clone(),
                            expr.data_type(),
                            dest_type,
                            &inner_domain,
                        )
                    })
                } else {
                    inner_domain.and_then(|inner_domain| {
                        self.calculate_cast(
                            span.clone(),
                            expr.data_type(),
                            dest_type,
                            &inner_domain,
                        )
                    })
                };

                let cast_expr = Expr::Cast {
                    span: span.clone(),
                    is_try: *is_try,
                    expr: Box::new(inner_expr.clone()),
                    dest_type: dest_type.clone(),
                };

                if inner_expr.as_constant().is_some() {
                    let chunk = Chunk::empty();
                    let evaluator = Evaluator::<Index>::new(&chunk, self.fn_ctx, self.fn_registry);
                    // Since we know the expression is constant, it'll be safe to change its column index type.
                    let cast_expr = cast_expr.project_column_ref(|_| unreachable!());
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&cast_expr) {
                        return (
                            Expr::Constant {
                                span: span.clone(),
                                scalar,
                                data_type: dest_type.clone(),
                            },
                            new_domain,
                        );
                    }
                }

                (
                    new_domain
                        .as_ref()
                        .and_then(Domain::as_singleton)
                        .map(|scalar| Expr::Constant {
                            span: span.clone(),
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
                    args_domain.and_then(|domains| match (function.calc_domain)(&domains) {
                        FunctionDomain::MayThrow => None,
                        FunctionDomain::Full => Some(Domain::full(return_type)),
                        FunctionDomain::Domain(domain) => Some(domain),
                    });
                let all_args_is_scalar = args_expr.iter().all(|arg| arg.as_constant().is_some());

                if let Some(scalar) = func_domain.as_ref().and_then(Domain::as_singleton) {
                    return (
                        Expr::Constant {
                            span: span.clone(),
                            scalar,
                            data_type: return_type.clone(),
                        },
                        func_domain,
                    );
                }

                let func_expr = Expr::FunctionCall {
                    span: span.clone(),
                    id: id.clone(),
                    function: function.clone(),
                    generics: generics.clone(),
                    args: args_expr,
                    return_type: return_type.clone(),
                };

                if all_args_is_scalar {
                    let chunk = Chunk::empty();
                    let evaluator = Evaluator::<Index>::new(&chunk, self.fn_ctx, self.fn_registry);
                    // Since we know the expression is constant, it'll be safe to change its column index type.
                    let func_expr = func_expr.project_column_ref(|_| unreachable!());
                    if let Ok(Value::Scalar(scalar)) = evaluator.run(&func_expr) {
                        return (
                            Expr::Constant {
                                span: span.clone(),
                                scalar,
                                data_type: return_type.clone(),
                            },
                            func_domain,
                        );
                    }
                }

                (func_expr, func_domain)
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

        if let Some(cast_fn) = check_simple_cast(false, dest_type) {
            return self
                .calculate_simple_cast(span, src_type, dest_type, domain, &cast_fn)
                .unwrap();
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

            (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty)) => {
                Some(Domain::Tuple(
                    domain
                        .as_tuple()
                        .unwrap()
                        .iter()
                        .zip(fields_src_ty)
                        .zip(fields_dest_ty)
                        .map(|((field_domain, src_ty), dest_ty)| {
                            self.calculate_cast(span.clone(), src_ty, dest_ty, field_domain)
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

        if let Some(cast_fn) = check_simple_cast(true, inner_dest_type) {
            return self
                .calculate_simple_cast(span, src_type, dest_type, domain, &cast_fn)
                .unwrap();
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

            (DataType::Tuple(fields_src_ty), DataType::Tuple(fields_dest_ty)) => {
                let fields_domain = domain.as_tuple().unwrap();
                let new_fields_domain = fields_domain
                    .iter()
                    .zip(fields_src_ty)
                    .zip(fields_dest_ty)
                    .map(|((domain, src_ty), dest_ty)| {
                        self.calculate_try_cast(span.clone(), src_ty, dest_ty, domain)
                    })
                    .collect::<Option<_>>()?;
                Some(Domain::Tuple(new_fields_domain))
            }

            _ => Some(Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            })),
        }
    }

    fn calculate_simple_cast(
        &self,
        span: Span,
        src_type: &DataType,
        dest_type: &DataType,
        domain: &Domain,
        cast_fn: &str,
    ) -> Result<Option<Domain>> {
        let (domain, ty) = calculate_function_domain(
            span,
            cast_fn,
            [(domain.clone(), src_type.clone())],
            self.fn_ctx,
            self.fn_registry,
        )?;
        assert_eq!(&ty, dest_type);
        Ok(domain)
    }
}
