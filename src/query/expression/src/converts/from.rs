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

use common_datavalues::remove_nullable;
use common_datavalues::ColumnRef;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use itertools::Itertools;

use crate::types::number::NumberScalar;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::NumberDataType;
use crate::with_number_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Scalar;
use crate::TableDataType;
use crate::TableField;
use crate::TableSchema;
use crate::Value;

pub fn can_convert(datatype: &DataTypeImpl) -> bool {
    !matches!(
        datatype,
        DataTypeImpl::Date(_) | DataTypeImpl::VariantArray(_) | DataTypeImpl::VariantObject(_)
    )
}

pub fn from_type(datatype: &DataTypeImpl) -> TableDataType {
    with_number_type!(|TYPE| match datatype {
        DataTypeImpl::TYPE(_) => TableDataType::Number(NumberDataType::TYPE),

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
        DataTypeImpl::Interval(_) => unimplemented!(),
    })
}

pub fn from_schema(schema: &common_datavalues::DataSchema) -> TableSchema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let ty = from_type(f.data_type());
            TableField::new(f.name(), ty)
        })
        .collect();
    TableSchema::new_from(fields, schema.meta().clone())
}

pub fn from_scalar(datavalue: &DataValue, datatype: &DataTypeImpl) -> Scalar {
    if datavalue.is_null() {
        return Scalar::Null;
    }

    let datatype = remove_nullable(datatype);
    match datatype {
        DataTypeImpl::Null(_) => Scalar::Null,
        DataTypeImpl::Boolean(_) => Scalar::Boolean(datavalue.as_bool().unwrap()),
        DataTypeImpl::Int8(_) => {
            Scalar::Number(NumberScalar::Int8(datavalue.as_i64().unwrap() as i8))
        }
        DataTypeImpl::Int16(_) => {
            Scalar::Number(NumberScalar::Int16(datavalue.as_i64().unwrap() as i16))
        }
        DataTypeImpl::Int32(_) => {
            Scalar::Number(NumberScalar::Int32(datavalue.as_i64().unwrap() as i32))
        }
        DataTypeImpl::Int64(_) => Scalar::Number(NumberScalar::Int64(datavalue.as_i64().unwrap())),
        DataTypeImpl::UInt8(_) => {
            Scalar::Number(NumberScalar::UInt8(datavalue.as_u64().unwrap() as u8))
        }
        DataTypeImpl::UInt16(_) => {
            Scalar::Number(NumberScalar::UInt16(datavalue.as_u64().unwrap() as u16))
        }
        DataTypeImpl::UInt32(_) => {
            Scalar::Number(NumberScalar::UInt32(datavalue.as_u64().unwrap() as u32))
        }
        DataTypeImpl::UInt64(_) => {
            Scalar::Number(NumberScalar::UInt64(datavalue.as_u64().unwrap()))
        }
        DataTypeImpl::Float32(_) => Scalar::Number(NumberScalar::Float32(
            (datavalue.as_f64().unwrap() as f32).into(),
        )),
        DataTypeImpl::Float64(_) => {
            Scalar::Number(NumberScalar::Float64(datavalue.as_f64().unwrap().into()))
        }
        DataTypeImpl::Timestamp(_) => Scalar::Timestamp(datavalue.as_i64().unwrap()),
        DataTypeImpl::Date(_) => Scalar::Date(datavalue.as_i64().unwrap() as i32),
        DataTypeImpl::String(_) => Scalar::String(datavalue.as_string().unwrap()),
        DataTypeImpl::Variant(_) => match datavalue {
            DataValue::String(x) => Scalar::Variant(x.clone()),
            DataValue::Variant(x) => {
                let v: Vec<u8> = serde_json::to_vec(x).unwrap();
                Scalar::Variant(v)
            }
            _ => unreachable!(),
        },
        DataTypeImpl::Struct(types) => {
            let values = match datavalue {
                DataValue::Struct(x) => x,
                _ => unreachable!(),
            };
            let inners = types
                .types()
                .iter()
                .zip(values.iter())
                .map(|(ty, v)| from_scalar(v, ty))
                .collect();

            Scalar::Tuple(inners)
        }
        DataTypeImpl::Array(ty) => {
            let values = match datavalue {
                DataValue::Array(x) => x,
                _ => unreachable!(),
            };

            let new_type = from_type(ty.inner_type());
            let mut builder = ColumnBuilder::with_capacity(&(&new_type).into(), values.len());

            for value in values.iter() {
                let scalar = from_scalar(value, ty.inner_type());
                builder.push(scalar.as_ref());
            }
            let col = builder.build();
            Scalar::Array(col)
        }
        _ => unreachable!(),
    }
}

pub fn convert_column(column: &ColumnRef, logical_type: &DataTypeImpl) -> Value<AnyType> {
    if column.is_const() {
        let value = column.get(0);
        let scalar = from_scalar(&value, logical_type);
        return Value::Scalar(scalar);
    }

    let arrow_column = column.as_arrow_array(logical_type.clone());
    let new_column = Column::from_arrow(arrow_column.as_ref());
    Value::Column(new_column)
}

pub fn from_block(datablock: &common_datablocks::DataBlock) -> DataBlock {
    let columns = datablock
        .columns()
        .iter()
        .zip(datablock.schema().fields().iter())
        .map(|(c, f)| BlockEntry {
            data_type: DataType::from(&from_type(f.data_type())),
            value: convert_column(c, f.data_type()),
        })
        .collect_vec();

    DataBlock::new(columns, datablock.num_rows())
}
