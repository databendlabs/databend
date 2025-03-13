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

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use databend_common_column::binary::BinaryColumn;
use databend_common_column::binview::StringColumn;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::ARROW_EXT_TYPE_BITMAP;
use super::ARROW_EXT_TYPE_EMPTY_ARRAY;
use super::ARROW_EXT_TYPE_EMPTY_MAP;
use super::ARROW_EXT_TYPE_GEOGRAPHY;
use super::ARROW_EXT_TYPE_GEOMETRY;
use super::ARROW_EXT_TYPE_INTERVAL;
use super::ARROW_EXT_TYPE_VARIANT;
use super::EXTENSION_KEY;
use crate::types::AnyType;
use crate::types::ArrayColumn;
use crate::types::DataType;
use crate::types::DecimalColumn;
use crate::types::DecimalDataType;
use crate::types::DecimalSize;
use crate::types::GeographyColumn;
use crate::types::NullableColumn;
use crate::types::NumberColumn;
use crate::types::NumberDataType;
use crate::Column;
use crate::DataBlock;
use crate::DataField;
use crate::DataSchema;
use crate::Scalar;
use crate::TableDataType;
use crate::TableField;
use crate::TableSchema;
use crate::Value;

impl TryFrom<&Field> for DataField {
    type Error = ErrorCode;
    fn try_from(arrow_f: &Field) -> Result<DataField> {
        Ok(DataField::from(&TableField::try_from(arrow_f)?))
    }
}

impl TryFrom<&Field> for TableField {
    type Error = ErrorCode;
    fn try_from(arrow_f: &Field) -> Result<TableField> {
        let mut data_type = match arrow_f
            .metadata()
            .get(EXTENSION_KEY)
            .map(|x| x.as_str())
            .unwrap_or("")
        {
            ARROW_EXT_TYPE_EMPTY_ARRAY => TableDataType::EmptyArray,
            ARROW_EXT_TYPE_EMPTY_MAP => TableDataType::EmptyMap,
            ARROW_EXT_TYPE_BITMAP => TableDataType::Bitmap,
            ARROW_EXT_TYPE_VARIANT => TableDataType::Variant,
            ARROW_EXT_TYPE_GEOMETRY => TableDataType::Geometry,
            ARROW_EXT_TYPE_GEOGRAPHY => TableDataType::Geography,
            ARROW_EXT_TYPE_INTERVAL => TableDataType::Interval,
            _ => match arrow_f.data_type() {
                ArrowDataType::Null => TableDataType::Null,
                ArrowDataType::Boolean => TableDataType::Boolean,
                ArrowDataType::Int8 => TableDataType::Number(NumberDataType::Int8),
                ArrowDataType::Int16 => TableDataType::Number(NumberDataType::Int16),
                ArrowDataType::Int32 => TableDataType::Number(NumberDataType::Int32),
                ArrowDataType::Int64 => TableDataType::Number(NumberDataType::Int64),
                ArrowDataType::UInt8 => TableDataType::Number(NumberDataType::UInt8),
                ArrowDataType::UInt16 => TableDataType::Number(NumberDataType::UInt16),
                ArrowDataType::UInt32 => TableDataType::Number(NumberDataType::UInt32),
                ArrowDataType::UInt64 => TableDataType::Number(NumberDataType::UInt64),
                ArrowDataType::Float32 => TableDataType::Number(NumberDataType::Float32),
                ArrowDataType::Float64 => TableDataType::Number(NumberDataType::Float64),

                ArrowDataType::FixedSizeBinary(_)
                | ArrowDataType::Binary
                | ArrowDataType::LargeBinary => TableDataType::Binary,
                ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
                    TableDataType::String
                }
                ArrowDataType::Decimal128(precision, scale) => {
                    TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                        precision: *precision,
                        scale: *scale as u8,
                    }))
                }
                ArrowDataType::Decimal256(precision, scale) => {
                    TableDataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                        precision: *precision,
                        scale: *scale as u8,
                    }))
                }
                ArrowDataType::Timestamp(_, _) => TableDataType::Timestamp,
                ArrowDataType::Date32 => TableDataType::Date,
                ArrowDataType::Date64 => TableDataType::Date,
                ArrowDataType::List(field) => {
                    let inner_type = TableField::try_from(field.as_ref())?;
                    TableDataType::Array(Box::new(inner_type.data_type))
                }
                ArrowDataType::LargeList(field) => {
                    let inner_type = TableField::try_from(field.as_ref())?;
                    TableDataType::Array(Box::new(inner_type.data_type))
                }
                ArrowDataType::Map(field, _) => {
                    if let ArrowDataType::Struct(fields) = field.data_type() {
                        let fields_name: Vec<String> =
                            fields.iter().map(|f| f.name().clone()).collect();
                        let fields_type: Vec<TableDataType> = fields
                            .iter()
                            .map(|f| TableField::try_from(f.as_ref()).map(|f| f.data_type))
                            .collect::<Result<Vec<_>>>()?;
                        TableDataType::Map(Box::new(TableDataType::Tuple {
                            fields_name,
                            fields_type,
                        }))
                    } else {
                        return Err(ErrorCode::Internal(format!(
                            "Invalid map field type: {:?}",
                            field.data_type()
                        )));
                    }
                }
                ArrowDataType::Struct(fields) => {
                    let fields_name: Vec<String> =
                        fields.iter().map(|f| f.name().clone()).collect();
                    let fields_type: Vec<TableDataType> = fields
                        .iter()
                        .map(|f| TableField::try_from(f.as_ref()).map(|f| f.data_type))
                        .collect::<Result<Vec<_>>>()?;
                    TableDataType::Tuple {
                        fields_name,
                        fields_type,
                    }
                }
                ArrowDataType::Dictionary(_, b) => {
                    let inner_f =
                        Field::new(arrow_f.name(), b.as_ref().clone(), arrow_f.is_nullable());
                    return Self::try_from(&inner_f);
                }
                arrow_type => {
                    return Err(ErrorCode::Internal(format!(
                        "Unsupported Arrow type: {:?}",
                        arrow_type
                    )));
                }
            },
        };
        if arrow_f.is_nullable() {
            data_type = data_type.wrap_nullable();
        }
        Ok(TableField::new(arrow_f.name(), data_type))
    }
}

impl TryFrom<&Schema> for DataSchema {
    type Error = ErrorCode;
    fn try_from(schema: &Schema) -> Result<DataSchema> {
        let fields = schema
            .fields()
            .iter()
            .map(|arrow_f| DataField::try_from(arrow_f.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Ok(DataSchema::new_from(
            fields,
            schema.metadata().clone().into_iter().collect(),
        ))
    }
}

impl TryFrom<&Schema> for TableSchema {
    type Error = ErrorCode;
    fn try_from(schema: &Schema) -> Result<TableSchema> {
        let fields = schema
            .fields()
            .iter()
            .map(|arrow_f| TableField::try_from(arrow_f.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Ok(TableSchema::new_from(
            fields,
            schema.metadata().clone().into_iter().collect(),
        ))
    }
}

impl DataBlock {
    pub fn from_record_batch(
        schema: &DataSchema,
        batch: &RecordBatch,
    ) -> Result<(Self, DataSchema)> {
        assert_eq!(
            schema.num_fields(),
            batch.num_columns(),
            "expect schema: {:?}, actual schema: {:?}",
            schema.fields,
            batch.schema().fields
        );

        if schema.fields().len() != batch.num_columns() {
            return Err(ErrorCode::Internal(format!(
                "conversion from RecordBatch to DataBlock failed, schema fields len: {}, RecordBatch columns len: {}",
                schema.fields().len(),
                batch.num_columns()
            )));
        }

        if batch.num_columns() == 0 {
            return Ok((DataBlock::new(vec![], batch.num_rows()), schema.clone()));
        }

        let mut columns = Vec::with_capacity(batch.columns().len());
        for (array, field) in batch.columns().iter().zip(schema.fields()) {
            columns.push(Column::from_arrow_rs(array.clone(), field.data_type())?)
        }

        Ok((DataBlock::new_from_columns(columns), schema.clone()))
    }
}

impl Value<AnyType> {
    pub fn from_arrow_rs(array: ArrayRef, data_type: &DataType) -> Result<Self> {
        if array.null_count() == array.len() {
            return Ok(Value::Scalar(Scalar::Null));
        }
        Ok(Value::Column(Column::from_arrow_rs(array, data_type)?))
    }
}

impl Column {
    pub fn arrow_field(&self) -> Field {
        let f = DataField::new("DUMMY", self.data_type());
        Field::from(&f)
    }

    pub fn from_arrow_rs(mut array: ArrayRef, data_type: &DataType) -> Result<Self> {
        if let ArrowDataType::Dictionary(_, v) = array.data_type() {
            array = arrow_cast::cast(array.as_ref(), v.as_ref())?;
        }

        let column = match data_type {
            DataType::Null => Column::Null { len: array.len() },
            DataType::EmptyArray => Column::EmptyArray { len: array.len() },
            DataType::EmptyMap => Column::EmptyMap { len: array.len() },
            DataType::Number(_ty) => {
                let col = NumberColumn::try_from_arrow_data(array.to_data())?;
                Column::Number(col)
            }
            DataType::Boolean => Column::Boolean(Bitmap::from_array_data(array.to_data())),
            DataType::String => Column::String(try_to_string_column(array)?),
            DataType::Decimal(_) => {
                Column::Decimal(DecimalColumn::try_from_arrow_data(array.to_data())?)
            }
            DataType::Timestamp => {
                let array = arrow_cast::cast(
                    array.as_ref(),
                    &ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                )?;
                let buffer: Buffer<i64> = array.to_data().buffers()[0].clone().into();
                Column::Timestamp(buffer)
            }
            DataType::Date => {
                let array = arrow_cast::cast(array.as_ref(), &ArrowDataType::Date32)?;
                let buffer: Buffer<i32> = array.to_data().buffers()[0].clone().into();
                Column::Date(buffer)
            }
            DataType::Interval => {
                let array = arrow_cast::cast(array.as_ref(), &ArrowDataType::Decimal128(38, 0))?;
                let buffer: Buffer<months_days_micros> =
                    array.to_data().buffers()[0].clone().into();
                Column::Interval(buffer)
            }
            DataType::Nullable(_) => {
                let validity = match array.nulls() {
                    Some(nulls) => Bitmap::from_null_buffer(nulls.clone()),
                    None => Bitmap::new_constant(true, array.len()),
                };
                let column = Column::from_arrow_rs(array, &data_type.remove_nullable())?;
                NullableColumn::new_column(column, validity)
            }
            DataType::Array(inner) => {
                let f = DataField::new("DUMMY", *inner.clone());
                let inner_f = Field::from(&f);
                let array =
                    arrow_cast::cast(array.as_ref(), &ArrowDataType::LargeList(inner_f.into()))?;

                let array = array
                    .as_any()
                    .downcast_ref::<arrow_array::LargeListArray>()
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Cannot downcast to LargeListArray from array: {:?}",
                            array
                        ))
                    })?;
                let values = Column::from_arrow_rs(array.values().clone(), inner.as_ref())?;
                let offsets: Buffer<u64> = array.offsets().inner().inner().clone().into();

                let inner_col = ArrayColumn { values, offsets };
                Column::Array(Box::new(inner_col))
            }
            DataType::Map(inner) => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow_array::MapArray>()
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Cannot downcast to MapArray from array: {:?}",
                            array
                        ))
                    })?;
                let entries = Arc::new(array.entries().clone());
                let values = Column::from_arrow_rs(entries, inner.as_ref())?;
                let offsets: Buffer<i32> = array.offsets().inner().inner().clone().into();
                let offsets = offsets.into_iter().map(|x| x as u64).collect();

                let inner_col = ArrayColumn { values, offsets };
                Column::Map(Box::new(inner_col))
            }
            DataType::Tuple(ts) => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow_array::StructArray>()
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Cannot downcast to StructArray from array: {:?}",
                            array
                        ))
                    })?;
                let columns = array
                    .columns()
                    .iter()
                    .zip(ts.iter())
                    .map(|(array, ty)| Column::from_arrow_rs(array.clone(), ty))
                    .collect::<Result<Vec<_>>>()?;
                Column::Tuple(columns)
            }

            DataType::Binary => Column::Binary(try_to_binary_column(array)?),
            DataType::Bitmap => Column::Bitmap(try_to_binary_column(array)?),
            DataType::Variant => Column::Variant(try_to_binary_column(array)?),
            DataType::Geometry => Column::Geometry(try_to_binary_column(array)?),
            DataType::Geography => Column::Geography(GeographyColumn(try_to_binary_column(array)?)),
            DataType::Generic(_) => unreachable!("Generic type is not supported"),
        };

        Ok(column)
    }
}

// Convert from `ArrayData` into BinaryColumn ignores the validity
fn try_to_binary_column(array: ArrayRef) -> Result<BinaryColumn> {
    let array = if !matches!(array.data_type(), ArrowDataType::LargeBinary) {
        arrow_cast::cast(array.as_ref(), &ArrowDataType::LargeBinary)?
    } else {
        array
    };

    let data = array.to_data();
    let offsets = data.buffers()[0].clone();
    let values = data.buffers()[1].clone();

    Ok(BinaryColumn::new(values.into(), offsets.into()))
}

// Convert from `ArrayData` into BinaryColumn ignores the validity
fn try_to_string_column(array: ArrayRef) -> Result<StringColumn> {
    let array = if !matches!(array.data_type(), ArrowDataType::Utf8View) {
        arrow_cast::cast(array.as_ref(), &ArrowDataType::Utf8View)?
    } else {
        array
    };

    let data = array.to_data();
    Ok(data.into())
}
