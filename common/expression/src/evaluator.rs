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

use common_arrow::arrow::bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use itertools::Itertools;
use num_traits::ToPrimitive;

use crate::chunk::Chunk;
use crate::error::Result;
use crate::expression::Expr;
use crate::expression::Span;
use crate::function::FunctionContext;
use crate::property::BooleanDomain;
use crate::property::Domain;
use crate::property::FloatDomain;
use crate::property::IntDomain;
use crate::property::NullableDomain;
use crate::property::StringDomain;
use crate::property::UIntDomain;
use crate::types::any::AnyType;
use crate::types::array::ArrayColumn;
use crate::types::nullable::NullableColumn;
use crate::types::DataType;
use crate::util::constant_bitmap;
use crate::values::Column;
use crate::values::ColumnBuilder;
use crate::values::Scalar;
use crate::values::Value;
use crate::with_number_type;

pub struct Evaluator {
    pub input_columns: Chunk,
    pub context: FunctionContext,
}

impl Evaluator {
    pub fn run(&self, expr: &Expr) -> Result<Value<AnyType>> {
        match expr {
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
                (function.eval)(cols_ref.as_slice(), generics).map_err(|msg| (span.clone(), msg))
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
        }
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

            // identical types
            (scalar @ Scalar::Null, DataType::Null)
            | (scalar @ Scalar::EmptyArray, DataType::EmptyArray)
            | (scalar @ Scalar::Boolean(_), DataType::Boolean)
            | (scalar @ Scalar::String(_), DataType::String) => Ok(scalar),

            (scalar, dest_ty) => {
                // number types
                with_number_type!(SRC_TYPE, match scalar {
                    Scalar::SRC_TYPE(value) => {
                        with_number_type!(DEST_TYPE, match dest_ty {
                            DataType::DEST_TYPE => {
                                let src_info = DataType::SRC_TYPE.number_type_info().unwrap();
                                let dest_info = DataType::DEST_TYPE.number_type_info().unwrap();
                                if src_info.can_lossless_cast_to(dest_info) {
                                    return Ok(Scalar::DEST_TYPE(value as _));
                                } else {
                                    let value = num_traits::cast::cast(value).ok_or_else(|| {
                                        (
                                            span.clone(),
                                            format!(
                                                "unable to cast {} to {}",
                                                scalar.as_ref(),
                                                stringify!(DEST_TYPE)
                                            ),
                                        )
                                    })?;
                                    return Ok(Scalar::DEST_TYPE(value));
                                }
                            }
                            _ => (),
                        })
                    }
                    _ => (),
                });

                // failure cases
                Err((
                    span,
                    (format!("unable to cast {} to {dest_ty}", scalar.as_ref())),
                ))
            }
        }
    }

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

            // identical types
            (col @ Column::Null { .. }, DataType::Null)
            | (col @ Column::EmptyArray { .. }, DataType::EmptyArray)
            | (col @ Column::Boolean(_), DataType::Boolean)
            | (col @ Column::String { .. }, DataType::String) => Ok(col),

            (col, dest_ty) => {
                // number types
                with_number_type!(SRC_TYPE, match &col {
                    Column::SRC_TYPE(col) => {
                        with_number_type!(DEST_TYPE, match dest_ty {
                            DataType::DEST_TYPE => {
                                let src_info = DataType::SRC_TYPE.number_type_info().unwrap();
                                let dest_info = DataType::DEST_TYPE.number_type_info().unwrap();
                                if src_info.can_lossless_cast_to(dest_info) {
                                    let new_col = col.iter().map(|x| *x as _).collect::<Vec<_>>();
                                    return Ok(Column::DEST_TYPE(new_col.into()));
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
                                    return Ok(Column::DEST_TYPE(new_col.into()));
                                }
                            }
                            _ => (),
                        })
                    }
                    _ => (),
                });

                // failure cases
                Err((span, (format!("unable to cast {col:?} to {dest_ty}"))))
            }
        }
    }

    pub fn run_try_cast_scalar(&self, span: Span, scalar: Scalar, dest_type: &DataType) -> Scalar {
        let inner_type: &DataType = dest_type.as_nullable().unwrap();
        self.run_cast_scalar(span, scalar, inner_type)
            .unwrap_or(Scalar::Null)
    }

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

            // identical types
            (column @ Column::Boolean(_), DataType::Boolean)
            | (column @ Column::String { .. }, DataType::String)
            | (column @ Column::EmptyArray { .. }, DataType::EmptyArray) => {
                Column::Nullable(Box::new(NullableColumn {
                    validity: constant_bitmap(true, column.len()).into(),
                    column,
                }))
            }

            (col, dest_ty) => {
                // number types
                with_number_type!(SRC_TYPE, match &col {
                    Column::SRC_TYPE(col) => {
                        with_number_type!(DEST_TYPE, match dest_ty {
                            DataType::DEST_TYPE => {
                                let src_info = DataType::SRC_TYPE.number_type_info().unwrap();
                                let dest_info = DataType::DEST_TYPE.number_type_info().unwrap();
                                if src_info.can_lossless_cast_to(dest_info) {
                                    let new_col = col.iter().map(|x| *x as _).collect::<Vec<_>>();
                                    return Column::Nullable(Box::new(NullableColumn {
                                        validity: constant_bitmap(true, new_col.len()).into(),
                                        column: Column::DEST_TYPE(new_col.into()),
                                    }));
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
                                    return Column::Nullable(Box::new(NullableColumn {
                                        validity: validity.into(),
                                        column: Column::DEST_TYPE(new_col.into()),
                                    }));
                                }
                            }
                            _ => (),
                        })
                    }
                    _ => (),
                });

                // failure cases
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

pub struct DomainCalculator {
    input_domains: Vec<Domain>,
}

impl DomainCalculator {
    pub fn new(input_domains: Vec<Domain>) -> Self {
        DomainCalculator { input_domains }
    }

    pub fn calculate(&self, expr: &Expr) -> Result<Domain> {
        match expr {
            Expr::Constant { scalar, .. } => Ok(self.calculate_constant(scalar)),
            Expr::ColumnRef { id, .. } => Ok(self.input_domains[*id].clone()),
            Expr::Cast {
                span,
                expr,
                dest_type,
            } => {
                let domain = self.calculate(expr)?;
                self.calculate_cast(span.clone(), &domain, dest_type)
            }
            Expr::TryCast {
                span,
                expr,
                dest_type,
            } => {
                let domain = self.calculate(expr)?;
                Ok(self.calculate_try_cast(span.clone(), &domain, dest_type))
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

    pub fn calculate_constant(&self, scalar: &Scalar) -> Domain {
        match scalar {
            Scalar::Null => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            Scalar::EmptyArray => Domain::Array(None),
            Scalar::Int8(i) => Domain::Int(IntDomain {
                min: *i as i64,
                max: *i as i64,
            }),
            Scalar::Int16(i) => Domain::Int(IntDomain {
                min: *i as i64,
                max: *i as i64,
            }),
            Scalar::Int32(i) => Domain::Int(IntDomain {
                min: *i as i64,
                max: *i as i64,
            }),
            Scalar::Int64(i) => Domain::Int(IntDomain { min: *i, max: *i }),
            Scalar::UInt8(i) => Domain::UInt(UIntDomain {
                min: *i as u64,
                max: *i as u64,
            }),
            Scalar::UInt16(i) => Domain::UInt(UIntDomain {
                min: *i as u64,
                max: *i as u64,
            }),
            Scalar::UInt32(i) => Domain::UInt(UIntDomain {
                min: *i as u64,
                max: *i as u64,
            }),
            Scalar::UInt64(i) => Domain::UInt(UIntDomain { min: *i, max: *i }),
            Scalar::Float32(i) => Domain::Float(FloatDomain {
                min: *i as f64,
                max: *i as f64,
            }),
            Scalar::Float64(i) => Domain::Float(FloatDomain { min: *i, max: *i }),
            Scalar::Boolean(true) => Domain::Boolean(BooleanDomain {
                has_false: false,
                has_true: true,
            }),
            Scalar::Boolean(false) => Domain::Boolean(BooleanDomain {
                has_false: true,
                has_true: false,
            }),
            Scalar::String(s) => Domain::String(StringDomain {
                min: s.clone(),
                max: Some(s.clone()),
            }),
            Scalar::Array(array) => Domain::Array(Some(Box::new(array.domain()))),
            Scalar::Tuple(fields) => Domain::Tuple(
                fields
                    .iter()
                    .map(|field| self.calculate_constant(field))
                    .collect(),
            ),
        }
    }

    pub fn calculate_cast(
        &self,
        span: Span,
        domain: &Domain,
        dest_type: &DataType,
    ) -> Result<Domain> {
        match (domain, dest_type) {
            (
                Domain::Nullable(NullableDomain { value: None, .. }),
                DataType::Null | DataType::Nullable(_),
            ) => Ok(domain.clone()),
            (Domain::Array(None), DataType::EmptyArray | DataType::Array(_)) => {
                Ok(Domain::Array(None))
            }
            (
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(value),
                }),
                DataType::Nullable(ty),
            ) => Ok(Domain::Nullable(NullableDomain {
                has_null: *has_null,
                value: Some(Box::new(self.calculate_cast(span, value, ty)?)),
            })),
            (domain, DataType::Nullable(ty)) => Ok(Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(Box::new(self.calculate_cast(span, domain, ty)?)),
            })),
            (Domain::Array(Some(domain)), DataType::Array(ty)) => Ok(Domain::Array(Some(
                Box::new(self.calculate_cast(span, domain, ty)?),
            ))),
            (Domain::Tuple(fields), DataType::Tuple(fields_ty)) => Ok(Domain::Tuple(
                fields
                    .iter()
                    .zip(fields_ty)
                    .map(|(field, ty)| self.calculate_cast(span.clone(), field, ty))
                    .collect::<Result<Vec<_>>>()?,
            )),

            // identical types
            (Domain::Boolean(_), DataType::Boolean) | (Domain::String(_), DataType::String) => {
                Ok(domain.clone())
            }

            // number types
            (Domain::UInt(UIntDomain { min, max }), DataType::UInt8) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).min(u8::MAX as u64),
                    max: (*max).min(u8::MAX as u64),
                }))
            }
            (Domain::UInt(UIntDomain { min, max }), DataType::UInt16) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).min(u16::MAX as u64),
                    max: (*max).min(u16::MAX as u64),
                }))
            }
            (Domain::UInt(UIntDomain { min, max }), DataType::UInt32) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).min(u32::MAX as u64),
                    max: (*max).min(u32::MAX as u64),
                }))
            }
            (Domain::UInt(_), DataType::UInt64) => Ok(domain.clone()),
            (Domain::UInt(UIntDomain { min, max }), DataType::Int8) => Ok(Domain::Int(IntDomain {
                min: (*min).min(i8::MAX as u64) as i64,
                max: (*max).min(i8::MAX as u64) as i64,
            })),
            (Domain::UInt(UIntDomain { min, max }), DataType::Int16) => {
                Ok(Domain::Int(IntDomain {
                    min: (*min).min(i16::MAX as u64) as i64,
                    max: (*max).min(i16::MAX as u64) as i64,
                }))
            }
            (Domain::UInt(UIntDomain { min, max }), DataType::Int32) => {
                Ok(Domain::Int(IntDomain {
                    min: (*min).min(i32::MAX as u64) as i64,
                    max: (*max).min(i32::MAX as u64) as i64,
                }))
            }
            (Domain::UInt(UIntDomain { min, max }), DataType::Int64) => {
                Ok(Domain::Int(IntDomain {
                    min: (*min).min(i64::MAX as u64) as i64,
                    max: (*max).min(i64::MAX as u64) as i64,
                }))
            }
            (Domain::UInt(UIntDomain { min, max }), DataType::Float32) => {
                // Cast to f32 and then to f64 to round to the nearest f32 value.
                Ok(Domain::Float(FloatDomain {
                    min: *min as f32 as f64,
                    max: *max as f32 as f64,
                }))
            }
            (Domain::UInt(UIntDomain { min, max }), DataType::Float64) => {
                Ok(Domain::Float(FloatDomain {
                    min: *min as f64,
                    max: *max as f64,
                }))
            }

            (Domain::Int(IntDomain { min, max }), DataType::UInt8) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).clamp(0, u8::MAX as i64) as u64,
                    max: (*max).clamp(0, u8::MAX as i64) as u64,
                }))
            }
            (Domain::Int(IntDomain { min, max }), DataType::UInt16) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).clamp(0, u16::MAX as i64) as u64,
                    max: (*max).clamp(0, u16::MAX as i64) as u64,
                }))
            }
            (Domain::Int(IntDomain { min, max }), DataType::UInt32) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).clamp(0, u32::MAX as i64) as u64,
                    max: (*max).clamp(0, u32::MAX as i64) as u64,
                }))
            }
            (Domain::Int(IntDomain { min, max }), DataType::UInt64) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).max(0) as u64,
                    max: (*max).max(0) as u64,
                }))
            }
            (Domain::Int(IntDomain { min, max }), DataType::Int8) => Ok(Domain::Int(IntDomain {
                min: (*min).clamp(i8::MIN as i64, i8::MAX as i64),
                max: (*max).clamp(i8::MIN as i64, i8::MAX as i64),
            })),
            (Domain::Int(IntDomain { min, max }), DataType::Int16) => Ok(Domain::Int(IntDomain {
                min: (*min).clamp(i16::MIN as i64, i16::MAX as i64),
                max: (*max).clamp(i16::MIN as i64, i16::MAX as i64),
            })),
            (Domain::Int(IntDomain { min, max }), DataType::Int32) => Ok(Domain::Int(IntDomain {
                min: (*min).clamp(i32::MIN as i64, i32::MAX as i64),
                max: (*max).clamp(i32::MIN as i64, i32::MAX as i64),
            })),
            (Domain::Int(_), DataType::Int64) => Ok(domain.clone()),
            (Domain::Int(IntDomain { min, max }), DataType::Float32) => {
                // Cast to f32 and then to f64 to round to the nearest f32 value.
                Ok(Domain::Float(FloatDomain {
                    min: (*min) as f32 as f64,
                    max: (*max) as f32 as f64,
                }))
            }
            (Domain::Int(IntDomain { min, max }), DataType::Float64) => {
                Ok(Domain::Float(FloatDomain {
                    min: (*min) as f64,
                    max: (*max) as f64,
                }))
            }

            (Domain::Float(FloatDomain { min, max }), DataType::UInt8) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).round().clamp(0.0, u8::MAX as f64) as u64,
                    max: (*max).round().clamp(0.0, u8::MAX as f64) as u64,
                }))
            }
            (Domain::Float(FloatDomain { min, max }), DataType::UInt16) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).round().clamp(0.0, u16::MAX as f64) as u64,
                    max: (*max).round().clamp(0.0, u16::MAX as f64) as u64,
                }))
            }
            (Domain::Float(FloatDomain { min, max }), DataType::UInt32) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).round().clamp(0.0, u32::MAX as f64) as u64,
                    max: (*max).round().clamp(0.0, u32::MAX as f64) as u64,
                }))
            }
            (Domain::Float(FloatDomain { min, max }), DataType::UInt64) => {
                Ok(Domain::UInt(UIntDomain {
                    min: (*min).round().clamp(0.0, u64::MAX as f64) as u64,
                    max: (*max).round().clamp(0.0, u64::MAX as f64) as u64,
                }))
            }
            (Domain::Float(FloatDomain { min, max }), DataType::Int8) => {
                Ok(Domain::Int(IntDomain {
                    min: (*min).round().clamp(i8::MIN as f64, i8::MAX as f64) as i64,
                    max: (*max).round().clamp(i8::MIN as f64, i8::MAX as f64) as i64,
                }))
            }
            (Domain::Float(FloatDomain { min, max }), DataType::Int16) => {
                Ok(Domain::Int(IntDomain {
                    min: (*min).round().clamp(i16::MIN as f64, i16::MAX as f64) as i64,
                    max: (*max).round().clamp(i16::MIN as f64, i16::MAX as f64) as i64,
                }))
            }
            (Domain::Float(FloatDomain { min, max }), DataType::Int32) => {
                Ok(Domain::Int(IntDomain {
                    min: (*min).round().clamp(i32::MIN as f64, i32::MAX as f64) as i64,
                    max: (*max).round().clamp(i32::MIN as f64, i32::MAX as f64) as i64,
                }))
            }
            (Domain::Float(FloatDomain { min, max }), DataType::Int64) => {
                Ok(Domain::Int(IntDomain {
                    min: (*min).round().clamp(i64::MIN as f64, i64::MAX as f64) as i64,
                    max: (*max).round().clamp(i64::MIN as f64, i64::MAX as f64) as i64,
                }))
            }
            (Domain::Float(FloatDomain { min, max }), DataType::Float32) => {
                Ok(Domain::Float(FloatDomain {
                    // Cast to f32 and back to f64 to round to the nearest f32 value.
                    min: (*min) as f32 as f64,
                    max: (*max) as f32 as f64,
                }))
            }
            (Domain::Float(_), DataType::Float64) => Ok(domain.clone()),

            // failure cases
            (domain, dest_ty) => Err((span, (format!("unable to cast {domain} to {dest_ty}",)))),
        }
    }

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

            // identical types
            (Domain::Boolean(_), DataType::Boolean) | (Domain::String(_), DataType::String) => {
                Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain.clone())),
                })
            }

            // numeric types
            (
                Domain::UInt(_),
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64,
            ) => {
                let new_domain = self.calculate_cast(span, domain, inner_type).unwrap();
                Domain::Nullable(NullableDomain {
                    has_null: *domain != new_domain,
                    value: Some(Box::new(new_domain)),
                })
            }
            (
                Domain::UInt(UIntDomain { min, max }),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) => {
                let new_domain = self
                    .calculate_cast(span, domain, inner_type)
                    .unwrap()
                    .into_int()
                    .unwrap();
                let has_null = min.to_i64().filter(|min| *min == new_domain.min).is_none()
                    || max.to_i64().filter(|max| *max == new_domain.max).is_none();
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(Box::new(Domain::Int(new_domain))),
                })
            }
            (Domain::UInt(UIntDomain { min, max }), DataType::Float32 | DataType::Float64) => {
                let new_domain = self
                    .calculate_cast(span, domain, inner_type)
                    .unwrap()
                    .into_float()
                    .unwrap();
                let has_null = (*min)
                    .to_f64()
                    .filter(|min| *min == new_domain.min)
                    .is_none()
                    || (*max)
                        .to_f64()
                        .filter(|max| *max == new_domain.max)
                        .is_none();
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(Box::new(Domain::Float(new_domain))),
                })
            }
            (
                Domain::Int(IntDomain { min, max }),
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64,
            ) => {
                let new_domain = self
                    .calculate_cast(span, domain, inner_type)
                    .unwrap()
                    .into_u_int()
                    .unwrap();
                let has_null = min.to_u64().filter(|min| *min == new_domain.min).is_none()
                    || max.to_u64().filter(|max| *max == new_domain.max).is_none();
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(Box::new(Domain::UInt(new_domain))),
                })
            }
            (
                Domain::Int(_),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) => {
                let new_domain = self.calculate_cast(span, domain, inner_type).unwrap();
                Domain::Nullable(NullableDomain {
                    has_null: *domain != new_domain,
                    value: Some(Box::new(new_domain)),
                })
            }
            (Domain::Int(IntDomain { min, max }), DataType::Float32 | DataType::Float64) => {
                let new_domain = self
                    .calculate_cast(span, domain, inner_type)
                    .unwrap()
                    .into_float()
                    .unwrap();
                let has_null = (*min)
                    .to_f64()
                    .filter(|min| *min == new_domain.min)
                    .is_none()
                    || (*max)
                        .to_f64()
                        .filter(|max| *max == new_domain.max)
                        .is_none();
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(Box::new(Domain::Float(new_domain))),
                })
            }
            (
                Domain::Float(FloatDomain { min, max }),
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64,
            ) => {
                let new_domain = self
                    .calculate_cast(span, domain, inner_type)
                    .unwrap()
                    .into_u_int()
                    .unwrap();
                let has_null = (*min)
                    .to_u64()
                    .filter(|min| *min == new_domain.min)
                    .is_none()
                    || (*max)
                        .to_u64()
                        .filter(|max| *max == new_domain.max)
                        .is_none();
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(Box::new(Domain::UInt(new_domain))),
                })
            }
            (
                Domain::Float(FloatDomain { min, max }),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) => {
                let new_domain = self
                    .calculate_cast(span, domain, inner_type)
                    .unwrap()
                    .into_int()
                    .unwrap();
                let has_null = (*min)
                    .to_i64()
                    .filter(|min| *min == new_domain.min)
                    .is_none()
                    || (*max)
                        .to_i64()
                        .filter(|max| *max == new_domain.max)
                        .is_none();
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(Box::new(Domain::Int(new_domain))),
                })
            }
            (Domain::Float(_), DataType::Float32 | DataType::Float64) => {
                let new_domain = self.calculate_cast(span, domain, inner_type).unwrap();
                Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(new_domain)),
                })
            }

            // failure cases
            _ => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
        }
    }
}
