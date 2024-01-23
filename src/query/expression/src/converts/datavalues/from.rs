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

use databend_common_datavalues::DataTypeImpl;
use databend_common_datavalues::DataValue;

use crate::types::number::NumberScalar;
use crate::types::*;
use crate::ColumnBuilder;
use crate::Scalar;
use crate::TableDataType;
use crate::TableField;
use crate::TableSchema;

pub fn can_convert(datatype: &DataTypeImpl) -> bool {
    !matches!(
        datatype,
        DataTypeImpl::VariantArray(_) | DataTypeImpl::VariantObject(_)
    )
}

pub fn from_type(datatype: &DataTypeImpl) -> TableDataType {
    match datatype {
        DataTypeImpl::Int8(_) => TableDataType::Number(NumberDataType::Int8),
        DataTypeImpl::Int16(_) => TableDataType::Number(NumberDataType::Int16),
        DataTypeImpl::Int32(_) => TableDataType::Number(NumberDataType::Int32),
        DataTypeImpl::Int64(_) => TableDataType::Number(NumberDataType::Int64),
        DataTypeImpl::UInt8(_) => TableDataType::Number(NumberDataType::UInt8),
        DataTypeImpl::UInt16(_) => TableDataType::Number(NumberDataType::UInt16),
        DataTypeImpl::UInt32(_) => TableDataType::Number(NumberDataType::UInt32),
        DataTypeImpl::UInt64(_) => TableDataType::Number(NumberDataType::UInt64),
        DataTypeImpl::Float32(_) => TableDataType::Number(NumberDataType::Float32),
        DataTypeImpl::Float64(_) => TableDataType::Number(NumberDataType::Float64),

        DataTypeImpl::Null(_) => TableDataType::Null,
        DataTypeImpl::Nullable(v) => TableDataType::Nullable(Box::new(from_type(v.inner_type()))),
        DataTypeImpl::Boolean(_) => TableDataType::Boolean,
        DataTypeImpl::Timestamp(_) => TableDataType::Timestamp,
        DataTypeImpl::Date(_) => TableDataType::Date,
        DataTypeImpl::String(_) => TableDataType::String,
        DataTypeImpl::Struct(fields) => {
            let fields_name = fields.names().clone().unwrap_or_else(|| {
                (0..fields.types().len())
                    .map(|i| format!("{}", i + 1))
                    .collect()
            });
            let fields_type = fields.types().iter().map(from_type).collect();
            TableDataType::Tuple {
                fields_name,
                fields_type,
            }
        }
        DataTypeImpl::Array(ty) => TableDataType::Array(Box::new(from_type(ty.inner_type()))),
        DataTypeImpl::Variant(_)
        | DataTypeImpl::VariantArray(_)
        | DataTypeImpl::VariantObject(_) => TableDataType::Variant,

        // NOTE: No Interval type is ever stored in meta-service.
        //       This variant should never be matched.
        //       Thus it is safe for this conversion to map it to any type.
        DataTypeImpl::Interval(_) => TableDataType::Null,
    }
}

pub fn from_schema(schema: &databend_common_datavalues::DataSchema) -> TableSchema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let ty = from_type(f.data_type());
            TableField::new(f.name(), ty).with_default_expr(f.default_expr().cloned())
        })
        .collect();
    TableSchema::new_from(fields, schema.meta().clone())
}

pub fn from_scalar(datavalue: &DataValue, datatype: &DataType) -> Scalar {
    if datavalue.is_null() {
        return Scalar::Null;
    }

    let datatype = datatype.remove_nullable();
    match datatype {
        DataType::Null => Scalar::Null,
        DataType::Boolean => Scalar::Boolean(*datavalue.as_boolean().unwrap()),
        DataType::Number(NumberDataType::Int8) => {
            Scalar::Number(NumberScalar::Int8(*datavalue.as_int64().unwrap() as i8))
        }
        DataType::Number(NumberDataType::Int16) => {
            Scalar::Number(NumberScalar::Int16(*datavalue.as_int64().unwrap() as i16))
        }
        DataType::Number(NumberDataType::Int32) => {
            Scalar::Number(NumberScalar::Int32(*datavalue.as_int64().unwrap() as i32))
        }
        DataType::Number(NumberDataType::Int64) => {
            Scalar::Number(NumberScalar::Int64(*datavalue.as_int64().unwrap()))
        }
        DataType::Number(NumberDataType::UInt8) => {
            Scalar::Number(NumberScalar::UInt8(*datavalue.as_u_int64().unwrap() as u8))
        }
        DataType::Number(NumberDataType::UInt16) => {
            Scalar::Number(NumberScalar::UInt16(*datavalue.as_u_int64().unwrap() as u16))
        }
        DataType::Number(NumberDataType::UInt32) => {
            Scalar::Number(NumberScalar::UInt32(*datavalue.as_u_int64().unwrap() as u32))
        }
        DataType::Number(NumberDataType::UInt64) => {
            Scalar::Number(NumberScalar::UInt64(*datavalue.as_u_int64().unwrap()))
        }
        DataType::Number(NumberDataType::Float32) => Scalar::Number(NumberScalar::Float32(
            (*datavalue.as_float64().unwrap() as f32).into(),
        )),
        DataType::Number(NumberDataType::Float64) => Scalar::Number(NumberScalar::Float64(
            (*datavalue.as_float64().unwrap()).into(),
        )),
        DataType::Timestamp => Scalar::Timestamp(*datavalue.as_int64().unwrap()),
        DataType::Date => Scalar::Date(*datavalue.as_int64().unwrap() as i32),
        DataType::String => {
            Scalar::String(String::from_utf8_lossy(datavalue.as_string().unwrap()).into_owned())
        }
        DataType::Variant => match datavalue {
            DataValue::String(x) => Scalar::Variant(x.clone()),
            DataValue::Variant(x) => {
                let v: Vec<u8> = serde_json::to_vec(x).unwrap();
                Scalar::Variant(v)
            }
            _ => unreachable!(),
        },
        DataType::Geometry => Scalar::Geometry((*datavalue.as_geometry().unwrap()).clone()),
        DataType::Tuple(types) => {
            let values = match datavalue {
                DataValue::Struct(x) => x,
                _ => unreachable!(),
            };
            let inners = types
                .iter()
                .zip(values.iter())
                .map(|(ty, v)| from_scalar(v, ty))
                .collect();

            Scalar::Tuple(inners)
        }
        DataType::Array(ty) => {
            let values = match datavalue {
                DataValue::Array(x) => x,
                _ => unreachable!(),
            };

            let mut builder = ColumnBuilder::with_capacity(ty.as_ref(), values.len());

            for value in values.iter() {
                let scalar = from_scalar(value, ty.as_ref());
                builder.push(scalar.as_ref());
            }
            let col = builder.build();
            Scalar::Array(col)
        }
        _ => unreachable!(),
    }
}
