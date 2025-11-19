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
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::*;
use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::Fields;
use arrow_schema::Schema;
use arrow_schema::TimeUnit;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::buffer_to_array_data;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

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
use crate::infer_table_schema;
use crate::types::DataType;
use crate::types::DecimalColumn;
use crate::types::DecimalDataType;
use crate::types::GeographyColumn;
use crate::types::NumberDataType;
use crate::types::VectorColumn;
use crate::types::VectorDataType;
use crate::with_number_type;
use crate::Column;
use crate::DataBlock;
use crate::DataField;
use crate::DataSchema;
use crate::TableDataType;
use crate::TableField;
use crate::TableSchema;

impl From<&DataSchema> for Schema {
    fn from(schema: &DataSchema) -> Self {
        let fields = schema.fields().iter().map(Field::from).collect::<Vec<_>>();
        let metadata = schema
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Schema::new(fields).with_metadata(metadata)
    }
}

impl From<&TableSchema> for Schema {
    fn from(schema: &TableSchema) -> Self {
        let fields = schema.fields().iter().map(Field::from).collect::<Vec<_>>();
        let metadata = schema
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Schema::new(fields).with_metadata(metadata)
    }
}

impl From<&DataType> for ArrowDataType {
    fn from(ty: &DataType) -> Self {
        let fields = DataField::new("dummy", ty.clone());
        let f = Field::from(&fields);
        f.data_type().clone()
    }
}

impl From<&TableField> for Field {
    fn from(f: &TableField) -> Self {
        let mut metadata = HashMap::new();

        let ty = match &f.data_type {
            TableDataType::Null => ArrowDataType::Null,
            TableDataType::EmptyArray => {
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_EMPTY_ARRAY.to_string(),
                );
                ArrowDataType::Boolean
            }
            TableDataType::EmptyMap => {
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_EMPTY_MAP.to_string(),
                );
                ArrowDataType::Boolean
            }
            TableDataType::Boolean => ArrowDataType::Boolean,
            TableDataType::Binary => ArrowDataType::LargeBinary,
            TableDataType::String => ArrowDataType::Utf8View,

            TableDataType::Opaque(size) => {
                metadata.insert(EXTENSION_KEY.to_string(), ARROW_EXT_TYPE_OPAQUE.to_string());
                ArrowDataType::FixedSizeList(
                    Field::new_list_field(ArrowDataType::UInt64, false).into(),
                    *size as _,
                )
            }

            TableDataType::Number(ty) => with_number_type!(|TYPE| match ty {
                NumberDataType::TYPE => ArrowDataType::TYPE,
            }),
            TableDataType::Decimal(
                DecimalDataType::Decimal64(size) | DecimalDataType::Decimal128(size),
            ) => ArrowDataType::Decimal128(size.precision(), size.scale() as i8),
            TableDataType::Decimal(DecimalDataType::Decimal256(size)) => {
                ArrowDataType::Decimal256(size.precision(), size.scale() as i8)
            }
            TableDataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            TableDataType::TimestampTz => {
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_TIMESTAMP_TIMEZONE.to_string(),
                );
                ArrowDataType::Decimal128(38, 0)
            }
            TableDataType::Date => ArrowDataType::Date32,
            TableDataType::Nullable(ty) => {
                let mut f = f.clone();
                f.data_type = *ty.clone();
                return Field::from(&f).with_nullable(true);
            }
            TableDataType::Array(ty) => {
                let f = TableField::new("_array", *ty.clone());
                let arrow_f = Field::from(&f);
                ArrowDataType::LargeList(Arc::new(arrow_f))
            }
            TableDataType::Map(ty) => {
                let inner_ty = match ty.as_ref() {
                    TableDataType::Tuple {
                        fields_name,
                        fields_type,
                    } => {
                        let key = TableField::new(&fields_name[0], fields_type[0].clone());
                        let arrow_key = Field::from(&key);

                        let value = TableField::new(&fields_name[1], fields_type[1].clone());
                        let arrow_value = Field::from(&value);

                        ArrowDataType::Struct(Fields::from(vec![arrow_key, arrow_value]))
                    }
                    _ => unreachable!(),
                };
                ArrowDataType::Map(
                    Arc::new(Field::new("entries", inner_ty, ty.is_nullable())),
                    false,
                )
            }
            TableDataType::Bitmap => {
                metadata.insert(EXTENSION_KEY.to_string(), ARROW_EXT_TYPE_BITMAP.to_string());
                ArrowDataType::LargeBinary
            }
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                let fields: Vec<Field> = fields_name
                    .iter()
                    .zip(fields_type)
                    .map(|(name, ty)| {
                        let f = TableField::new(name, ty.clone());
                        let f = Field::from(&f);
                        f.with_nullable(ty.is_nullable_or_null())
                    })
                    .collect();
                ArrowDataType::Struct(Fields::from(fields))
            }
            TableDataType::Variant => {
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_VARIANT.to_string(),
                );
                ArrowDataType::LargeBinary
            }
            TableDataType::Geometry => {
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_GEOMETRY.to_string(),
                );
                ArrowDataType::LargeBinary
            }
            TableDataType::Geography => {
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_GEOGRAPHY.to_string(),
                );
                ArrowDataType::LargeBinary
            }
            TableDataType::Interval => {
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_INTERVAL.to_string(),
                );
                ArrowDataType::Decimal128(38, 0)
            }
            TableDataType::Vector(ty) => {
                metadata.insert(EXTENSION_KEY.to_string(), ARROW_EXT_TYPE_VECTOR.to_string());
                let (inner_ty, dimension) = match ty {
                    VectorDataType::Int8(dimension) => (ArrowDataType::Int8, *dimension as i32),
                    VectorDataType::Float32(dimension) => {
                        (ArrowDataType::Float32, *dimension as i32)
                    }
                };
                let inner_field = Arc::new(Field::new_list_field(inner_ty, false));
                ArrowDataType::FixedSizeList(inner_field, dimension)
            }
            TableDataType::StageLocation => {
                unreachable!("TableDataType::StageLocation only for UDFServer/UDAFServer")
            }
        };

        Field::new(f.name(), ty, f.is_nullable_or_null()).with_metadata(metadata)
    }
}

impl From<&DataField> for Field {
    fn from(f: &DataField) -> Self {
        Field::from(&TableField::from(f))
    }
}

impl DataBlock {
    // Notice this function may loss some struct tuples as we are using infer_schema
    pub fn to_record_batch_with_dataschema(self, data_schema: &DataSchema) -> Result<RecordBatch> {
        let table_schema = infer_table_schema(data_schema)?;
        self.to_record_batch(&table_schema)
    }

    pub fn to_record_batch(self, table_schema: &TableSchema) -> Result<RecordBatch> {
        if self.columns().len() != table_schema.num_fields() {
            return Err(ErrorCode::Internal(format!(
                "The number of columns in the data block does not match the number of fields in the table schema, block_columns: {}, table_schema_fields: {}",
                self.columns().len(),
                table_schema.num_fields()
            )));
        }

        if table_schema.num_fields() == 0 {
            return Ok(RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::default().with_row_count(Some(self.num_rows())),
            )?);
        }

        let arrow_schema = Schema::from(table_schema);
        let mut arrays = Vec::with_capacity(self.columns().len());
        for (entry, arrow_field) in self.take_columns().into_iter().zip(arrow_schema.fields()) {
            let array = entry.to_column().maybe_gc().into_arrow_rs();

            // Adjust struct array names
            arrays.push(Self::adjust_nested_array(array, arrow_field.as_ref()));
        }
        Ok(RecordBatch::try_new(Arc::new(arrow_schema), arrays)?)
    }

    fn adjust_nested_array(array: Arc<dyn Array>, arrow_field: &Field) -> Arc<dyn Array> {
        if let ArrowDataType::Struct(fs) = arrow_field.data_type() {
            let array = array.as_ref().as_struct();
            let inner_arrays = array
                .columns()
                .iter()
                .zip(fs.iter())
                .map(|(array, arrow_field)| {
                    Self::adjust_nested_array(array.clone(), arrow_field.as_ref())
                })
                .collect();

            let array = StructArray::new(fs.clone(), inner_arrays, array.nulls().cloned());
            Arc::new(array) as _
        } else if let ArrowDataType::LargeList(f) = arrow_field.data_type() {
            let array = array.as_ref().as_list::<i64>();
            let values = Self::adjust_nested_array(array.values().clone(), f.as_ref());
            let array = LargeListArray::new(
                f.clone(),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            );
            Arc::new(array) as _
        } else if let ArrowDataType::Map(f, ordered) = arrow_field.data_type() {
            let array = array.as_ref().as_map();

            let entry = Arc::new(array.entries().clone()) as Arc<dyn Array>;
            let entry = Self::adjust_nested_array(entry, f.as_ref());

            let array = MapArray::new(
                f.clone(),
                array.offsets().clone(),
                entry.as_struct().clone(),
                array.nulls().cloned(),
                *ordered,
            );
            Arc::new(array) as _
        } else {
            array
        }
    }
}

impl From<&Column> for ArrayData {
    fn from(value: &Column) -> Self {
        let arrow_type = ArrowDataType::from(&value.data_type());
        match value {
            Column::Null { len } => {
                let builder = ArrayDataBuilder::new(arrow_type).len(*len);
                unsafe { builder.build_unchecked() }
            }
            Column::EmptyArray { len } => Bitmap::new_constant(true, *len).into(),
            Column::EmptyMap { len } => Bitmap::new_constant(true, *len).into(),
            Column::Boolean(col) => col.into(),
            Column::Number(c) => c.arrow_data(arrow_type),
            Column::Decimal(c) => {
                let c = c.clone().strict_decimal();
                let arrow_type = match c {
                    DecimalColumn::Decimal64(_, size) | DecimalColumn::Decimal128(_, size) => {
                        ArrowDataType::Decimal128(size.precision(), size.scale() as _)
                    }
                    DecimalColumn::Decimal256(_, size) => {
                        ArrowDataType::Decimal256(size.precision(), size.scale() as _)
                    }
                };
                c.arrow_data(arrow_type)
            }
            Column::String(col) => col.clone().into(),
            Column::Timestamp(col) => buffer_to_array_data((col.clone(), arrow_type)),
            Column::TimestampTz(col) => buffer_to_array_data((col.clone(), arrow_type)),
            Column::Date(col) => buffer_to_array_data((col.clone(), arrow_type)),
            Column::Interval(col) => buffer_to_array_data((col.clone(), arrow_type)),
            Column::Array(col) => {
                let child_data = ArrayData::from(&col.underlying_column());
                let builder = ArrayDataBuilder::new(arrow_type)
                    .len(value.len())
                    .buffers(vec![col.underlying_offsets().into()])
                    .child_data(vec![child_data]);

                unsafe { builder.build_unchecked() }
            }
            Column::Nullable(col) => {
                let data = ArrayData::from(&col.column);
                let builder = data.into_builder();
                let nulls = col.validity.clone().into();
                unsafe { builder.nulls(Some(nulls)).build_unchecked() }
            }
            Column::Map(col) => {
                let child_data = ArrayData::from(&col.underlying_column());
                let offsets: Vec<i32> = col
                    .underlying_offsets()
                    .into_iter()
                    .map(|x| x as i32)
                    .collect();
                let builder = ArrayDataBuilder::new(arrow_type)
                    .len(value.len())
                    .buffers(vec![offsets.into()])
                    .child_data(vec![child_data]);
                unsafe { builder.build_unchecked() }
            }
            Column::Tuple(fields) => {
                let child_data = fields.iter().map(ArrayData::from).collect::<Vec<_>>();
                let builder = ArrayDataBuilder::new(arrow_type)
                    .len(value.len())
                    .child_data(child_data);

                unsafe { builder.build_unchecked() }
            }
            Column::Vector(col) => {
                let child_builder = match col {
                    VectorColumn::Int8((values, _)) => ArrayData::builder(ArrowDataType::Int8)
                        .len(values.len())
                        .add_buffer(values.clone().into()),
                    VectorColumn::Float32((values, _)) => {
                        ArrayData::builder(ArrowDataType::Float32)
                            .len(values.len())
                            .add_buffer(values.clone().into())
                    }
                };
                let child_data = unsafe { child_builder.build_unchecked() };
                let builder = ArrayDataBuilder::new(arrow_type)
                    .len(value.len())
                    .child_data(vec![child_data]);

                unsafe { builder.build_unchecked() }
            }
            Column::Binary(col)
            | Column::Bitmap(col)
            | Column::Variant(col)
            | Column::Geometry(col)
            | Column::Geography(GeographyColumn(col)) => col.clone().into(),
            Column::Opaque(col) => col.arrow_data(),
        }
    }
}

impl From<&Column> for Arc<dyn arrow_array::Array> {
    fn from(col: &Column) -> Self {
        let data = ArrayData::from(col);
        arrow_array::make_array(data)
    }
}

impl Column {
    pub fn into_arrow_rs(self) -> Arc<dyn arrow_array::Array> {
        (&self).into()
    }
}

#[cfg(test)]
mod tests {
    use databend_common_column::buffer::Buffer;

    use super::*;
    use crate::types::AnyType;
    use crate::types::ArrayColumn;

    #[test]
    fn test_to_record_batch_with_null_array() {
        let array =
            ArrayColumn::<AnyType>::new(Column::Null { len: 3 }, Buffer::from(vec![0_u64, 3]));
        let block = DataBlock::new_from_columns(vec![Column::Array(Box::new(array))]);
        let schema = DataSchema::new(vec![DataField::new(
            "arr",
            DataType::Array(Box::new(DataType::Null)),
        )]);

        assert!(block.to_record_batch_with_dataschema(&schema).is_ok());
    }
}
