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

use itertools::Itertools;

use crate::chunk::Chunk;
use crate::error::Result;
use crate::expression::Expr;
use crate::expression::Literal;
use crate::expression::Span;
use crate::function::FunctionContext;
use crate::property::BooleanDomain;
use crate::property::Domain;
use crate::property::IntDomain;
use crate::property::NullableDomain;
use crate::property::StringDomain;
use crate::property::UIntDomain;
use crate::types::any::AnyType;
use crate::types::DataType;
use crate::util::constant_bitmap;
use crate::values::Column;
use crate::values::ColumnBuilder;
use crate::values::Scalar;
use crate::values::Value;

pub struct Evaluator {
    pub input_columns: Chunk,
    pub context: FunctionContext,
}

impl Evaluator {
    pub fn run(&self, expr: &Expr) -> Result<Value<AnyType>> {
        match expr {
            Expr::Literal { lit, .. } => Ok(Value::Scalar(self.run_lit(lit))),
            Expr::ColumnRef { id, .. } => {
                Ok(Value::Column(self.input_columns.columns()[*id].clone()))
            }
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
                (function.eval)(cols_ref.as_slice(), generics).map_err(|msg| (span.clone(), msg))
            }
            Expr::Cast {
                span,
                expr,
                dest_type,
            } => {
                let value = self.run(expr)?;
                self.run_cast(value, dest_type, span.clone())
            }
        }
    }

    pub fn run_cast(
        &self,
        input: Value<AnyType>,
        dest_type: &DataType,
        span: Span,
    ) -> Result<Value<AnyType>> {
        match input {
            Value::Scalar(scalar) => match (scalar, dest_type) {
                (Scalar::Null, DataType::Nullable(_)) => Ok(Value::Scalar(Scalar::Null)),
                (Scalar::EmptyArray, DataType::Array(dest_ty)) => {
                    let column = ColumnBuilder::with_capacity(dest_ty, 0).build();
                    Ok(Value::Scalar(Scalar::Array(column)))
                }
                (scalar, DataType::Nullable(dest_ty)) => {
                    self.run_cast(Value::Scalar(scalar), dest_ty, span)
                }
                (Scalar::Array(array), DataType::Array(dest_ty)) => {
                    let array = self
                        .run_cast(Value::Column(array), dest_ty, span)?
                        .into_column()
                        .ok()
                        .unwrap();
                    Ok(Value::Scalar(Scalar::Array(array)))
                }
                (Scalar::UInt8(val), DataType::UInt16) => {
                    Ok(Value::Scalar(Scalar::UInt16(val as u16)))
                }
                (Scalar::Int8(val), DataType::Int16) => {
                    Ok(Value::Scalar(Scalar::Int16(val as i16)))
                }
                (Scalar::UInt8(val), DataType::Int16) => {
                    Ok(Value::Scalar(Scalar::Int16(val as i16)))
                }
                (scalar @ Scalar::Boolean(_), DataType::Boolean)
                | (scalar @ Scalar::String(_), DataType::String)
                | (scalar @ Scalar::UInt8(_), DataType::UInt8)
                | (scalar @ Scalar::Int8(_), DataType::Int8)
                | (scalar @ Scalar::Int16(_), DataType::Int16)
                | (scalar @ Scalar::Null, DataType::Null)
                | (scalar @ Scalar::EmptyArray, DataType::EmptyArray) => Ok(Value::Scalar(scalar)),
                (scalar, dest_ty) => Err((
                    span,
                    (format!("unable to cast {} to {dest_ty}", scalar.as_ref())),
                )),
            },
            Value::Column(col) => match (col, dest_type) {
                (Column::Null { len }, DataType::Nullable(_)) => {
                    let mut builder = ColumnBuilder::with_capacity(dest_type, len);
                    for _ in 0..len {
                        builder.push_default();
                    }
                    Ok(Value::Column(builder.build()))
                }
                (Column::EmptyArray { len }, DataType::Array(dest_ty)) => {
                    Ok(Value::Column(Column::Array {
                        array: Box::new(ColumnBuilder::with_capacity(dest_ty, 0).build()),
                        offsets: vec![0; len + 1].into(),
                    }))
                }
                (Column::Nullable { column, validity }, DataType::Nullable(dest_ty)) => {
                    let column = self
                        .run_cast(Value::Column(*column), dest_ty, span)?
                        .into_column()
                        .ok()
                        .unwrap();
                    Ok(Value::Column(Column::Nullable {
                        column: Box::new(column),
                        validity,
                    }))
                }
                (col, DataType::Nullable(dest_ty)) => {
                    let column = self
                        .run_cast(Value::Column(col), dest_ty, span)?
                        .into_column()
                        .ok()
                        .unwrap();
                    Ok(Value::Column(Column::Nullable {
                        validity: constant_bitmap(true, column.len()).into(),
                        column: Box::new(column),
                    }))
                }
                (Column::Array { array, offsets }, DataType::Array(dest_ty)) => {
                    let array = self
                        .run_cast(Value::Column(*array), dest_ty, span)?
                        .into_column()
                        .ok()
                        .unwrap();
                    Ok(Value::Column(Column::Array {
                        array: Box::new(array),
                        offsets,
                    }))
                }
                (Column::UInt8(column), DataType::UInt16) => Ok(Value::Column(Column::UInt16(
                    column.iter().map(|v| *v as u16).collect(),
                ))),
                (Column::Int8(column), DataType::Int16) => Ok(Value::Column(Column::Int16(
                    column.iter().map(|v| *v as i16).collect(),
                ))),
                (Column::UInt8(column), DataType::Int16) => Ok(Value::Column(Column::Int16(
                    column.iter().map(|v| *v as i16).collect(),
                ))),
                (col @ Column::Boolean(_), DataType::Boolean)
                | (col @ Column::String { .. }, DataType::String)
                | (col @ Column::UInt8(_), DataType::UInt8)
                | (col @ Column::Int8(_), DataType::Int8)
                | (col @ Column::Int16(_), DataType::Int16)
                | (col @ Column::Null { .. }, DataType::Null)
                | (col @ Column::EmptyArray { .. }, DataType::EmptyArray) => Ok(Value::Column(col)),
                (col, dest_ty) => Err((span, (format!("unable to cast {col:?} to {dest_ty}")))),
            },
        }
    }

    pub fn run_lit(&self, lit: &Literal) -> Scalar {
        match lit {
            Literal::Null => Scalar::Null,
            Literal::Int8(val) => Scalar::Int8(*val),
            Literal::Int16(val) => Scalar::Int16(*val),
            Literal::UInt8(val) => Scalar::UInt8(*val),
            Literal::UInt16(val) => Scalar::UInt16(*val),
            Literal::Boolean(val) => Scalar::Boolean(*val),
            Literal::String(val) => Scalar::String(val.clone()),
        }
    }
}

pub struct DomainCalculator {
    pub input_domains: Vec<Domain>,
}

impl DomainCalculator {
    pub fn calculate(&self, expr: &Expr) -> Result<Domain> {
        match expr {
            Expr::Literal { lit, .. } => Ok(self.calculate_literal(lit)),
            Expr::ColumnRef { id, .. } => Ok(self.input_domains[*id].clone()),
            Expr::Cast {
                span,
                expr,
                dest_type,
            } => {
                let domain = self.calculate(expr)?;
                self.calculate_cast(&domain, dest_type, span.clone())
            }
            Expr::FunctionCall {
                function,
                generics,
                args,
                ..
            } => {
                let args_domain = args
                    .iter()
                    .map(|arg| self.calculate(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok((function.calc_domain)(&args_domain, generics))
            }
        }
    }

    pub fn calculate_literal(&self, lit: &Literal) -> Domain {
        match lit {
            Literal::Null => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            Literal::Int8(i) => Domain::Int(IntDomain {
                min: *i as i64,
                max: *i as i64,
            }),
            Literal::Int16(i) => Domain::Int(IntDomain {
                min: *i as i64,
                max: *i as i64,
            }),
            Literal::UInt8(i) => Domain::UInt(UIntDomain {
                min: *i as u64,
                max: *i as u64,
            }),
            Literal::UInt16(i) => Domain::UInt(UIntDomain {
                min: *i as u64,
                max: *i as u64,
            }),
            Literal::Boolean(true) => Domain::Boolean(BooleanDomain {
                has_false: false,
                has_true: true,
            }),
            Literal::Boolean(false) => Domain::Boolean(BooleanDomain {
                has_false: true,
                has_true: false,
            }),
            Literal::String(s) => Domain::String(StringDomain {
                min: s.clone(),
                max: Some(s.clone()),
            }),
        }
    }

    pub fn calculate_cast(
        &self,
        input: &Domain,
        dest_type: &DataType,
        span: Span,
    ) -> Result<Domain> {
        match (input, dest_type) {
            (
                Domain::Nullable(NullableDomain { value: None, .. }),
                DataType::Null | DataType::Nullable(_),
            ) => Ok(input.clone()),
            (Domain::Array(None), DataType::EmptyArray | DataType::Array(_)) => Ok(input.clone()),
            (
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(value),
                }),
                DataType::Nullable(ty),
            ) => Ok(Domain::Nullable(NullableDomain {
                has_null: *has_null,
                value: Some(Box::new(self.calculate_cast(value, ty, span)?)),
            })),
            (domain, DataType::Nullable(ty)) => Ok(Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(Box::new(self.calculate_cast(domain, ty, span)?)),
            })),
            (Domain::Array(Some(domain)), DataType::Array(ty)) => Ok(Domain::Array(Some(
                Box::new(self.calculate_cast(domain, ty, span)?),
            ))),
            (Domain::UInt(UIntDomain { min, max }), DataType::UInt16) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).min(u16::MAX as u64),
                    max: (*max).min(u16::MAX as u64),
                }))
            }
            (Domain::Int(IntDomain { min, max }), DataType::Int16) => Ok(Domain::Int(IntDomain {
                min: (*min).max(i16::MIN as i64).min(i16::MAX as i64),
                max: (*max).max(i16::MIN as i64).min(i16::MAX as i64),
            })),
            (Domain::UInt(UIntDomain { min, max }), DataType::Int16) => {
                Ok(Domain::Int(IntDomain {
                    min: (*min).min(i16::MAX as u64) as i64,
                    max: (*max).min(i16::MAX as u64) as i64,
                }))
            }
            (Domain::Boolean(_), DataType::Boolean)
            | (Domain::String(_), DataType::String)
            | (Domain::UInt(_), DataType::UInt8)
            | (Domain::Int(_), DataType::Int8) => Ok(input.clone()),
            (domain, dest_ty) => Err((span, (format!("unable to cast {domain} to {dest_ty}",)))),
        }
    }
}
