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

use common_datavalues::ColumnRef;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_datavalues::remove_nullable;

use crate::Column;
use crate::ColumnBuilder;
use crate::Scalar;
use crate::Value;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::NumberDataType;
use crate::types::number::NumberScalar;
use crate::types::timestamp::Timestamp;
use crate::with_number_type;

pub fn can_convert(datatype: &DataTypeImpl) -> bool {
    match datatype {
        DataTypeImpl::Date(_) | DataTypeImpl::Interval(_) => false,
        _ => true,
    }
}

pub fn convert_type(datatype: &DataTypeImpl) -> DataType {
    with_number_type!(|TYPE| match datatype {
        DataTypeImpl::TYPE(_) => DataType::Number(NumberDataType::TYPE),

        DataTypeImpl::Null(_) => DataType::Null,
        DataTypeImpl::Nullable(v) => DataType::Nullable(Box::new(convert_type(v.inner_type()))),
        DataTypeImpl::Boolean(_) => DataType::Boolean,
        DataTypeImpl::Timestamp(_) => DataType::Timestamp,
        DataTypeImpl::String(_) => DataType::String,
        DataTypeImpl::Struct(ty) => {
            let inners = ty.types().iter().map(convert_type).collect();
            DataType::Tuple(inners)
        }
        DataTypeImpl::Array(ty) => DataType::Array(Box::new(convert_type(ty.inner_type()))),
        DataTypeImpl::Variant(_)
        | DataTypeImpl::VariantArray(_)
        | DataTypeImpl::VariantObject(_) => DataType::Variant,
        DataTypeImpl::Date(_) => unreachable!(),
        DataTypeImpl::Interval(_) => unreachable!(),
    })
}

pub fn convert_scalar(datavalue: &DataValue, datatype: &DataTypeImpl) -> Scalar {
    if datavalue.is_null() {
        return Scalar::Null;
    }

    let datatype = remove_nullable(datatype);
    match datatype {
        DataTypeImpl::Null(_) => Scalar::Null,
        DataTypeImpl::Boolean(_) =>  Scalar::Boolean(datavalue.as_bool().unwrap()),
        DataTypeImpl::Int8(_) =>  Scalar::Number(NumberScalar::Int8(datavalue.as_i64().unwrap() as i8)),
        DataTypeImpl::Int16(_) => Scalar::Number(NumberScalar::Int16(datavalue.as_i64().unwrap() as i16)),
        DataTypeImpl::Int32(_) => Scalar::Number(NumberScalar::Int32(datavalue.as_i64().unwrap() as i32)),
        DataTypeImpl::Int64(_) => Scalar::Number(NumberScalar::Int64(datavalue.as_i64().unwrap() as i64)),
        DataTypeImpl::UInt8(_) =>   Scalar::Number(NumberScalar::UInt8(datavalue.as_u64().unwrap() as u8)),
        DataTypeImpl::UInt16(_) =>  Scalar::Number(NumberScalar::UInt16(datavalue.as_u64().unwrap() as u16)),
        DataTypeImpl::UInt32(_) =>  Scalar::Number(NumberScalar::UInt32(datavalue.as_u64().unwrap() as u32)),
        DataTypeImpl::UInt64(_) =>  Scalar::Number(NumberScalar::UInt64(datavalue.as_u64().unwrap() as u64)),
        DataTypeImpl::Float32(_) => Scalar::Number(NumberScalar::Float32((datavalue.as_f64().unwrap() as f32).into())),
        DataTypeImpl::Float64(_) => Scalar::Number(NumberScalar::Float64(datavalue.as_f64().unwrap().into())),
        DataTypeImpl::Timestamp(t) => Scalar::Timestamp(Timestamp { ts: datavalue.as_i64().unwrap(),  precision: t.precision() as u8 }),

        DataTypeImpl::String(_) => Scalar::String(datavalue.as_string().unwrap()),
        DataTypeImpl::Struct(types) => {
            let values = match datavalue {
                DataValue::Struct(x) => x,
                _=> unreachable!()
            };
            let inners = types
                .types()
                .iter()
                .zip(values.iter())
                .map(|(ty, v)| convert_scalar(v, ty))
                .collect();

            Scalar::Tuple(inners)
        },
        DataTypeImpl::Array(ty) => {
            let values = match datavalue {
                DataValue::Array(x) => x,
                _=> unreachable!()
            };

            let new_type = convert_type(ty.inner_type());
            let mut builder = ColumnBuilder::with_capacity(&new_type, values.len());

            for value in values.iter() {
                let scalar = convert_scalar(value, ty.inner_type());
                builder.push(scalar.as_ref());
            }
            let col = builder.build();
            Scalar::Array(col)
        },

        DataTypeImpl::Variant(_) |  DataTypeImpl::VariantArray(_)  |   DataTypeImpl::VariantObject(_)  => {
               let value = match datavalue {
                DataValue::Variant(x) => x,
                _=> unreachable!()
            };
            let v: Vec<u8> = serde_json::to_vec(value).unwrap();
            Scalar::Variant(v)
        },
        DataTypeImpl::Date(_) => todo!(),
        DataTypeImpl::Interval(_) => todo!(),
        _ => unreachable!()
    }
}


pub fn convert_column(column: &ColumnRef, logical_type: &DataTypeImpl) -> Value<AnyType> {
    if column.is_const() {
        let value = column.get(0);
        let scalar = convert_scalar(&value, logical_type);
        return Value::Scalar(scalar);
    }

    let arrow_column = column.as_arrow_array(logical_type.clone());
    let new_column = Column::from_arrow(arrow_column.as_ref());
    Value::Column(new_column)
}


// pub fn convert_block(datablock: &DataBlock)