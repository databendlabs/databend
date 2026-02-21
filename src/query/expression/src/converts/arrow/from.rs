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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use databend_common_column::binary::BinaryColumn;
use databend_common_column::binary::BinaryColumnBuilder;
use databend_common_column::binview::StringColumn;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::metrics_inc_variant_shredding_unshred_milliseconds;
use jsonb::Number as JsonbNumber;
use jsonb::Value as JsonbValue;

use super::ARROW_EXT_TYPE_BITMAP;
use super::ARROW_EXT_TYPE_EMPTY_ARRAY;
use super::ARROW_EXT_TYPE_EMPTY_MAP;
use super::ARROW_EXT_TYPE_GEOGRAPHY;
use super::ARROW_EXT_TYPE_GEOMETRY;
use super::ARROW_EXT_TYPE_INTERVAL;
use super::ARROW_EXT_TYPE_OPAQUE;
use super::ARROW_EXT_TYPE_TIMESTAMP_TIMEZONE;
use super::ARROW_EXT_TYPE_VARIANT;
use super::ARROW_EXT_TYPE_VECTOR;
use super::EXTENSION_KEY;
use crate::Column;
use crate::DataBlock;
use crate::DataField;
use crate::DataSchema;
use crate::Scalar;
use crate::TableDataType;
use crate::TableField;
use crate::TableSchema;
use crate::Value;
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
use crate::types::VectorColumn;
use crate::types::VectorDataType;
use crate::types::opaque::OpaqueColumn;

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
            ARROW_EXT_TYPE_VECTOR => match arrow_f.data_type() {
                ArrowDataType::FixedSizeList(field, dimension) => {
                    let vector_ty = match field.data_type() {
                        ArrowDataType::Int8 => VectorDataType::Int8(*dimension as u64),
                        ArrowDataType::Float32 => VectorDataType::Float32(*dimension as u64),
                        _ => {
                            return Err(ErrorCode::Internal(format!(
                                "Unsupported FixedSizeList Arrow type: {:?}",
                                field.data_type()
                            )));
                        }
                    };
                    TableDataType::Vector(vector_ty)
                }
                arrow_type => {
                    return Err(ErrorCode::Internal(format!(
                        "Unsupported Arrow type: {:?}",
                        arrow_type
                    )));
                }
            },
            ARROW_EXT_TYPE_TIMESTAMP_TIMEZONE => TableDataType::TimestampTz,
            ARROW_EXT_TYPE_OPAQUE => {
                let ArrowDataType::FixedSizeList(_, size) = arrow_f.data_type() else {
                    unreachable!()
                };
                TableDataType::Opaque(*size as _)
            }
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
                ArrowDataType::Decimal64(precision, scale) if *scale >= 0 => {
                    TableDataType::Decimal(DecimalDataType::Decimal64(DecimalSize::new(
                        *precision,
                        *scale as _,
                    )?))
                }
                ArrowDataType::Decimal128(precision, scale) if *scale >= 0 => {
                    TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize::new(
                        *precision,
                        *scale as _,
                    )?))
                }
                ArrowDataType::Decimal256(precision, scale) if *scale >= 0 => {
                    TableDataType::Decimal(DecimalDataType::Decimal256(DecimalSize::new(
                        *precision,
                        *scale as _,
                    )?))
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
                    if is_parquet_variant_struct(arrow_f) {
                        TableDataType::Variant
                    } else {
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

fn is_parquet_variant_struct(field: &Field) -> bool {
    let ArrowDataType::Struct(fields) = field.data_type() else {
        return false;
    };
    let mut has_metadata = false;
    let mut has_value = false;
    for child in fields {
        match child.name().as_str() {
            "metadata" => {
                has_metadata = matches!(
                    child.data_type(),
                    ArrowDataType::Binary
                        | ArrowDataType::LargeBinary
                        | ArrowDataType::FixedSizeBinary(_)
                );
            }
            "value" => {
                has_value = matches!(
                    child.data_type(),
                    ArrowDataType::Binary
                        | ArrowDataType::LargeBinary
                        | ArrowDataType::FixedSizeBinary(_)
                );
            }
            _ => {}
        }
    }
    has_metadata && has_value
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
    pub fn from_record_batch(schema: &DataSchema, batch: &RecordBatch) -> Result<Self> {
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
            return Ok(DataBlock::new(vec![], batch.num_rows()));
        }

        let mut columns = Vec::with_capacity(batch.columns().len());
        for (array, field) in batch.columns().iter().zip(schema.fields()) {
            columns.push(Column::from_arrow_rs(array.clone(), field.data_type())?)
        }

        Ok(DataBlock::new_from_columns(columns))
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
                let col = DecimalColumn::try_from_arrow_data(array.to_data())?;
                Column::Decimal(col.strict_decimal())
            }
            DataType::Timestamp => {
                let array = arrow_cast::cast(
                    array.as_ref(),
                    &ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                )?;
                let buffer: Buffer<i64> = array.to_data().buffers()[0].clone().into();
                Column::Timestamp(buffer)
            }
            DataType::TimestampTz => {
                let array = arrow_cast::cast(array.as_ref(), &ArrowDataType::Decimal128(38, 0))?;
                let buffer: Buffer<timestamp_tz> = array.to_data().buffers()[0].clone().into();
                Column::TimestampTz(buffer)
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

                let inner_col = ArrayColumn::new(values, offsets);
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

                let inner_col = ArrayColumn::new(values, offsets);
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
            DataType::Opaque(size) => Column::Opaque(try_to_opaque_column(array, *size)?),

            DataType::Bitmap => Column::Bitmap(try_to_binary_column(array)?),
            DataType::Variant => Column::Variant(try_to_variant_column(array)?),
            DataType::Geometry => Column::Geometry(try_to_binary_column(array)?),
            DataType::Geography => Column::Geography(GeographyColumn(try_to_binary_column(array)?)),
            DataType::Vector(ty) => {
                let (num_ty, inner_ty, dimension) = match ty {
                    VectorDataType::Int8(dimension) => {
                        (NumberDataType::Int8, ArrowDataType::Int8, *dimension as i32)
                    }
                    VectorDataType::Float32(dimension) => (
                        NumberDataType::Float32,
                        ArrowDataType::Float32,
                        *dimension as i32,
                    ),
                };
                let inner_field = Arc::new(Field::new_list_field(inner_ty, false));
                let list_type = ArrowDataType::FixedSizeList(inner_field, dimension);
                let array = arrow_cast::cast(array.as_ref(), &list_type)?;

                let array = array
                    .as_any()
                    .downcast_ref::<arrow_array::FixedSizeListArray>()
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Cannot downcast to FixedSizeListArray from array: {:?}",
                            array
                        ))
                    })?;
                let col = Column::from_arrow_rs(array.values().clone(), &DataType::Number(num_ty))?;
                let num_values = col.as_number().unwrap();
                match ty {
                    VectorDataType::Int8(dimension) => {
                        let values = num_values.as_int8().unwrap();
                        Column::Vector(VectorColumn::Int8((values.clone(), *dimension as usize)))
                    }
                    VectorDataType::Float32(dimension) => {
                        let values = num_values.as_float32().unwrap();
                        Column::Vector(VectorColumn::Float32((values.clone(), *dimension as usize)))
                    }
                }
            }
            DataType::Generic(_) => unreachable!("Generic type is not supported"),
            DataType::StageLocation => unreachable!("StageLocation type is not supported"),
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

fn try_to_variant_column(array: ArrayRef) -> Result<BinaryColumn> {
    if matches!(array.data_type(), ArrowDataType::Struct(_)) {
        return try_to_variant_column_from_struct(array);
    }
    try_to_binary_column(array)
}

fn try_to_variant_column_from_struct(array: ArrayRef) -> Result<BinaryColumn> {
    let start = std::time::Instant::now();
    let struct_array = array
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Cannot downcast to StructArray from array: {:?}",
                array
            ))
        })?;

    let metadata_col = struct_array
        .column_by_name("metadata")
        .ok_or_else(|| {
            ErrorCode::Internal("Variant struct array must contain a 'metadata' field".to_string())
        })?
        .clone();
    let metadata_col = arrow_cast::cast(metadata_col.as_ref(), &ArrowDataType::LargeBinary)?;
    let metadata_col = metadata_col
        .as_any()
        .downcast_ref::<arrow_array::LargeBinaryArray>()
        .ok_or_else(|| {
            ErrorCode::Internal("Variant struct 'metadata' field must be binary".to_string())
        })?;

    let value_col = struct_array.column_by_name("value").map(|col| {
        let col = arrow_cast::cast(col.as_ref(), &ArrowDataType::LargeBinary)?;
        col.as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .ok_or_else(|| {
                ErrorCode::Internal("Variant struct 'value' field must be binary".to_string())
            })
            .cloned()
    });
    let value_col = match value_col {
        Some(col) => Some(col?),
        None => None,
    };

    let typed_value_col = struct_array.column_by_name("typed_value").cloned();

    let mut builder = BinaryColumnBuilder::with_capacity(struct_array.len(), 0);
    for row in 0..struct_array.len() {
        if struct_array.is_null(row) || metadata_col.is_null(row) {
            builder.put_slice(crate::types::variant::JSONB_NULL);
            builder.commit_row();
            continue;
        }
        let mut value = match &value_col {
            Some(col) if !col.is_null(row) => {
                let bytes = col.value(row);
                if bytes.is_empty() { None } else { Some(bytes) }
            }
            _ => None,
        };
        if value.is_none() {
            if let Some(typed_value) = typed_value_col.as_ref() {
                if let Some(jsonb) = typed_value_to_jsonb_bytes(typed_value, row)? {
                    builder.put_slice(&jsonb);
                    builder.commit_row();
                    continue;
                }
            }
        }
        let metadata = metadata_col.value(row);
        let value_bytes = value.unwrap_or(&[]);
        if value_bytes.is_empty() {
            builder.put_slice(crate::types::variant::JSONB_NULL);
            builder.commit_row();
            continue;
        }
        let jsonb = crate::types::variant_parquet::parquet_variant_to_jsonb(metadata, value_bytes)?;
        builder.put_slice(&jsonb);
        builder.commit_row();
    }
    metrics_inc_variant_shredding_unshred_milliseconds(start.elapsed().as_millis() as u64);
    Ok(builder.build())
}

fn typed_value_to_jsonb_bytes(typed_value: &ArrayRef, row: usize) -> Result<Option<Vec<u8>>> {
    if typed_value.is_null(row) {
        return Ok(None);
    }
    let value = typed_value_to_jsonb_value(typed_value, row)?;
    let mut buf = Vec::new();
    value.write_to_vec(&mut buf);
    Ok(Some(buf))
}

fn typed_value_to_jsonb_value(typed_value: &ArrayRef, row: usize) -> Result<JsonbValue<'static>> {
    match typed_value.data_type() {
        ArrowDataType::Boolean => Ok(JsonbValue::Bool(typed_value.as_boolean().value(row))),
        ArrowDataType::Int8 => Ok(JsonbValue::Number(JsonbNumber::Int64(
            typed_value
                .as_primitive::<arrow_array::types::Int8Type>()
                .value(row) as i64,
        ))),
        ArrowDataType::Int16 => Ok(JsonbValue::Number(JsonbNumber::Int64(
            typed_value
                .as_primitive::<arrow_array::types::Int16Type>()
                .value(row) as i64,
        ))),
        ArrowDataType::Int32 => Ok(JsonbValue::Number(JsonbNumber::Int64(
            typed_value
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(row) as i64,
        ))),
        ArrowDataType::Int64 => Ok(JsonbValue::Number(JsonbNumber::Int64(
            typed_value
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row),
        ))),
        ArrowDataType::UInt8 => Ok(JsonbValue::Number(JsonbNumber::UInt64(
            typed_value
                .as_primitive::<arrow_array::types::UInt8Type>()
                .value(row) as u64,
        ))),
        ArrowDataType::UInt16 => Ok(JsonbValue::Number(JsonbNumber::UInt64(
            typed_value
                .as_primitive::<arrow_array::types::UInt16Type>()
                .value(row) as u64,
        ))),
        ArrowDataType::UInt32 => Ok(JsonbValue::Number(JsonbNumber::UInt64(
            typed_value
                .as_primitive::<arrow_array::types::UInt32Type>()
                .value(row) as u64,
        ))),
        ArrowDataType::UInt64 => Ok(JsonbValue::Number(JsonbNumber::UInt64(
            typed_value
                .as_primitive::<arrow_array::types::UInt64Type>()
                .value(row),
        ))),
        ArrowDataType::Float32 => Ok(JsonbValue::Number(JsonbNumber::Float64(
            typed_value
                .as_primitive::<arrow_array::types::Float32Type>()
                .value(row) as f64,
        ))),
        ArrowDataType::Float64 => Ok(JsonbValue::Number(JsonbNumber::Float64(
            typed_value
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(row),
        ))),
        ArrowDataType::Utf8 => Ok(JsonbValue::String(Cow::Owned(
            typed_value.as_string::<i32>().value(row).to_string(),
        ))),
        ArrowDataType::Utf8View => Ok(JsonbValue::String(Cow::Owned(
            typed_value.as_string_view().value(row).to_string(),
        ))),
        ArrowDataType::LargeUtf8 => Ok(JsonbValue::String(Cow::Owned(
            typed_value.as_string::<i64>().value(row).to_string(),
        ))),
        ArrowDataType::Struct(_) => typed_value_struct_to_jsonb_value(typed_value, row),
        other => Err(ErrorCode::Unimplemented(format!(
            "Unsupported typed_value type for variant decoding: {other:?}"
        ))),
    }
}

fn typed_value_struct_to_jsonb_value(
    typed_value: &ArrayRef,
    row: usize,
) -> Result<JsonbValue<'static>> {
    let struct_array = typed_value.as_struct();
    let mut map = BTreeMap::new();
    for (idx, field) in struct_array.fields().iter().enumerate() {
        let field_array = struct_array.column(idx);
        if field_array.is_null(row) {
            continue;
        }
        let value_array = if let Some(field_struct) = field_array.as_struct_opt() {
            field_struct
                .column_by_name("typed_value")
                .cloned()
                .unwrap_or_else(|| field_array.clone())
        } else {
            field_array.clone()
        };
        if value_array.is_null(row) {
            continue;
        }
        let value = typed_value_to_jsonb_value(&value_array, row)?;
        map.insert(field.name().to_string(), value);
    }
    Ok(JsonbValue::Object(map))
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

fn try_to_opaque_column(array: ArrayRef, size: usize) -> Result<OpaqueColumn> {
    let expected_type = ArrowDataType::FixedSizeList(
        Field::new_list_field(ArrowDataType::UInt64, false).into(),
        size as _,
    );
    let array = if array.data_type() != &expected_type {
        arrow_cast::cast(array.as_ref(), &expected_type)?
    } else {
        array
    };

    let array = array
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeListArray>()
        .ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Cannot downcast to FixedSizeListArray from array: {:?}",
                array
            ))
        })?;

    let data = array.values().to_data();
    OpaqueColumn::try_from_arrow_data(data, size)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_array::LargeBinaryArray;
    use arrow_array::StringArray;
    use arrow_array::StructArray;
    use arrow_buffer::NullBuffer;
    use arrow_schema::Field;
    use arrow_schema::Fields;

    use super::*;
    use crate::TableDataType;
    use crate::types::DataType;
    use crate::types::variant_parquet::jsonb_to_parquet_variant_parts;

    #[test]
    fn test_variant_struct_to_column() -> Result<()> {
        let json_text = br#"{"a":1}"#;
        let (metadata, value) = jsonb_to_parquet_variant_parts(json_text)?;

        let metadata_array =
            LargeBinaryArray::from(vec![Some(metadata.as_slice()), Some(metadata.as_slice())]);
        let value_array = LargeBinaryArray::from(vec![Some(value.as_slice()), None]);

        let fields = Fields::from(vec![
            Field::new("metadata", ArrowDataType::LargeBinary, false),
            Field::new("value", ArrowDataType::LargeBinary, true),
        ]);
        let nulls = NullBuffer::from(vec![true, false]);
        let struct_array = StructArray::new(
            fields,
            vec![
                Arc::new(metadata_array) as ArrayRef,
                Arc::new(value_array) as ArrayRef,
            ],
            Some(nulls),
        );

        let column = Column::from_arrow_rs(Arc::new(struct_array), &DataType::Variant)?;
        let Column::Variant(col) = column else {
            return Err(ErrorCode::Internal(
                "Expected variant column from struct conversion".to_string(),
            ));
        };

        let value =
            jsonb::from_slice(col.value(0)).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let expected =
            jsonb::from_slice(json_text).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        assert_eq!(value, expected);
        assert_eq!(col.value(1), crate::types::variant::JSONB_NULL);
        Ok(())
    }

    #[test]
    fn test_variant_struct_typed_value_to_column() -> Result<()> {
        let json_text = br#"42"#;
        let (metadata, _value) = jsonb_to_parquet_variant_parts(json_text)?;

        let metadata_array = LargeBinaryArray::from(vec![Some(metadata.as_slice())]);
        let value_array = LargeBinaryArray::from(vec![None]);
        let typed_value_array = Int64Array::from(vec![Some(42)]);

        let fields = Fields::from(vec![
            Field::new("metadata", ArrowDataType::LargeBinary, false),
            Field::new("value", ArrowDataType::LargeBinary, true),
            Field::new("typed_value", ArrowDataType::Int64, true),
        ]);
        let struct_array = StructArray::new(
            fields,
            vec![
                Arc::new(metadata_array) as ArrayRef,
                Arc::new(value_array) as ArrayRef,
                Arc::new(typed_value_array) as ArrayRef,
            ],
            None,
        );

        let column = Column::from_arrow_rs(Arc::new(struct_array), &DataType::Variant)?;
        let Column::Variant(col) = column else {
            return Err(ErrorCode::Internal(
                "Expected variant column from struct conversion".to_string(),
            ));
        };

        let value =
            jsonb::from_slice(col.value(0)).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let expected =
            jsonb::from_slice(json_text).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        assert_eq!(value, expected);
        Ok(())
    }

    #[test]
    fn test_variant_struct_typed_value_object_to_column() -> Result<()> {
        let json_text = br#"{"a":1,"b":"x"}"#;
        let (metadata, _value) = jsonb_to_parquet_variant_parts(json_text)?;

        let metadata_array = LargeBinaryArray::from(vec![Some(metadata.as_slice())]);
        let value_array = LargeBinaryArray::from(vec![None]);

        let a_values = Int64Array::from(vec![Some(1)]);
        let a_struct = StructArray::new(
            Fields::from(vec![Field::new("typed_value", ArrowDataType::Int64, true)]),
            vec![Arc::new(a_values) as ArrayRef],
            None,
        );

        let b_values = StringArray::from(vec![Some("x")]);
        let b_struct = StructArray::new(
            Fields::from(vec![Field::new("typed_value", ArrowDataType::Utf8, true)]),
            vec![Arc::new(b_values) as ArrayRef],
            None,
        );

        let typed_value = StructArray::new(
            Fields::from(vec![
                Field::new("a", a_struct.data_type().clone(), true),
                Field::new("b", b_struct.data_type().clone(), true),
            ]),
            vec![
                Arc::new(a_struct) as ArrayRef,
                Arc::new(b_struct) as ArrayRef,
            ],
            None,
        );

        let struct_array = StructArray::new(
            Fields::from(vec![
                Field::new("metadata", ArrowDataType::LargeBinary, false),
                Field::new("value", ArrowDataType::LargeBinary, true),
                Field::new("typed_value", typed_value.data_type().clone(), true),
            ]),
            vec![
                Arc::new(metadata_array) as ArrayRef,
                Arc::new(value_array) as ArrayRef,
                Arc::new(typed_value) as ArrayRef,
            ],
            None,
        );

        let column = Column::from_arrow_rs(Arc::new(struct_array), &DataType::Variant)?;
        let Column::Variant(col) = column else {
            return Err(ErrorCode::Internal(
                "Expected variant column from struct conversion".to_string(),
            ));
        };

        let value =
            jsonb::from_slice(col.value(0)).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let expected =
            jsonb::from_slice(json_text).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        assert_eq!(value, expected);
        Ok(())
    }

    #[test]
    fn test_variant_struct_typed_value_object_without_wrapper() -> Result<()> {
        let json_text = br#"{"a":1,"b":"x"}"#;
        let (metadata, _value) = jsonb_to_parquet_variant_parts(json_text)?;

        let metadata_array = LargeBinaryArray::from(vec![Some(metadata.as_slice())]);
        let value_array = LargeBinaryArray::from(vec![None]);

        let a_values = Int64Array::from(vec![Some(1)]);
        let b_values = StringArray::from(vec![Some("x")]);
        let typed_value = StructArray::new(
            Fields::from(vec![
                Field::new("a", ArrowDataType::Int64, true),
                Field::new("b", ArrowDataType::Utf8, true),
            ]),
            vec![
                Arc::new(a_values) as ArrayRef,
                Arc::new(b_values) as ArrayRef,
            ],
            None,
        );

        let struct_array = StructArray::new(
            Fields::from(vec![
                Field::new("metadata", ArrowDataType::LargeBinary, false),
                Field::new("value", ArrowDataType::LargeBinary, true),
                Field::new("typed_value", typed_value.data_type().clone(), true),
            ]),
            vec![
                Arc::new(metadata_array) as ArrayRef,
                Arc::new(value_array) as ArrayRef,
                Arc::new(typed_value) as ArrayRef,
            ],
            None,
        );

        let column = Column::from_arrow_rs(Arc::new(struct_array), &DataType::Variant)?;
        let Column::Variant(col) = column else {
            return Err(ErrorCode::Internal(
                "Expected variant column from struct conversion".to_string(),
            ));
        };

        let value =
            jsonb::from_slice(col.value(0)).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let expected =
            jsonb::from_slice(json_text).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        assert_eq!(value, expected);
        Ok(())
    }

    #[test]
    fn test_variant_struct_field_without_extension() -> Result<()> {
        let fields = Fields::from(vec![
            Field::new("metadata", ArrowDataType::LargeBinary, false),
            Field::new("value", ArrowDataType::LargeBinary, true),
        ]);
        let struct_field = Field::new("v", ArrowDataType::Struct(fields), true);
        let table_field = TableField::try_from(&struct_field)?;
        assert_eq!(
            table_field.data_type(),
            &TableDataType::Nullable(Box::new(TableDataType::Variant))
        );
        Ok(())
    }

    #[test]
    fn test_struct_missing_value_field_is_not_variant() -> Result<()> {
        let fields = Fields::from(vec![Field::new(
            "metadata",
            ArrowDataType::LargeBinary,
            true,
        )]);
        let struct_field = Field::new("v", ArrowDataType::Struct(fields), true);
        let table_field = TableField::try_from(&struct_field)?;
        assert_eq!(
            table_field.data_type(),
            &TableDataType::Nullable(Box::new(TableDataType::Tuple {
                fields_name: vec!["metadata".to_string()],
                fields_type: vec![TableDataType::Nullable(Box::new(TableDataType::Binary))],
            }))
        );
        Ok(())
    }
}
