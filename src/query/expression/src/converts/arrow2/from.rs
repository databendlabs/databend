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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use databend_common_arrow::arrow::datatypes::Field as ArrowField;
use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::arrow::datatypes::TimeUnit;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::ARROW_EXT_TYPE_BITMAP;
use super::ARROW_EXT_TYPE_EMPTY_ARRAY;
use super::ARROW_EXT_TYPE_EMPTY_MAP;
use super::ARROW_EXT_TYPE_GEOMETRY;
use super::ARROW_EXT_TYPE_VARIANT;
use crate::types::array::ArrayColumn;
use crate::types::binary::BinaryColumn;
use crate::types::decimal::DecimalColumn;
use crate::types::geography::GeographyColumn;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumn;
use crate::types::DataType;
use crate::types::DecimalDataType;
use crate::types::DecimalSize;
use crate::types::NumberColumn;
use crate::types::NumberDataType;
use crate::types::F32;
use crate::types::F64;
use crate::with_number_type;
use crate::Column;
use crate::DataField;
use crate::DataSchema;
use crate::TableDataType;
use crate::TableField;
use crate::TableSchema;

impl TryFrom<&ArrowSchema> for TableSchema {
    type Error = ErrorCode;

    fn try_from(schema: &ArrowSchema) -> Result<Self> {
        let fields = schema
            .fields
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(TableSchema::new_from(fields, schema.metadata.clone()))
    }
}

impl TryFrom<&ArrowSchema> for DataSchema {
    type Error = ErrorCode;

    fn try_from(schema: &ArrowSchema) -> Result<Self> {
        let fields = schema
            .fields
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(DataSchema::new_from(fields, schema.metadata.clone()))
    }
}

impl TryFrom<&ArrowField> for TableField {
    type Error = ErrorCode;

    fn try_from(f: &ArrowField) -> Result<Self> {
        let ty = arrow_type_to_table_type(&f.data_type, f.is_nullable)?;
        Ok(TableField::new(&f.name, ty))
    }
}

impl TryFrom<&ArrowField> for DataField {
    type Error = ErrorCode;

    fn try_from(f: &ArrowField) -> Result<Self> {
        Ok(DataField::from(&TableField::try_from(f)?))
    }
}

fn arrow_type_to_table_type(ty: &ArrowDataType, is_nullable: bool) -> Result<TableDataType> {
    let ty = with_number_type!(|TYPE| match ty {
        ArrowDataType::TYPE => TableDataType::Number(NumberDataType::TYPE),

        ArrowDataType::Decimal(precision, scale) =>
            TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                precision: *precision as u8,
                scale: *scale as u8,
            })),
        ArrowDataType::Decimal256(precision, scale) =>
            TableDataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                precision: *precision as u8,
                scale: *scale as u8,
            })),

        ArrowDataType::Null => return Ok(TableDataType::Null),
        ArrowDataType::Boolean => TableDataType::Boolean,

        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::FixedSizeList(f, _) => TableDataType::Array(Box::new(
            arrow_type_to_table_type(&f.data_type, f.is_nullable)?
        )),

        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::FixedSizeBinary(_) =>
            TableDataType::Binary,

        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => TableDataType::String,

        ArrowDataType::Timestamp(_, _) => TableDataType::Timestamp,
        ArrowDataType::Date32 | ArrowDataType::Date64 => TableDataType::Date,
        ArrowDataType::Map(f, _) => {
            let inner_ty = arrow_type_to_table_type(&f.data_type, f.is_nullable)?;
            TableDataType::Map(Box::new(inner_ty))
        }
        ArrowDataType::Struct(fields) => {
            let mut fields_name = vec![];
            let mut fields_type = vec![];
            for f in fields {
                fields_name.push(f.name.to_string());
                fields_type.push(arrow_type_to_table_type(&f.data_type, f.is_nullable)?);
            }
            TableDataType::Tuple {
                fields_name,
                fields_type,
            }
        }
        ArrowDataType::Extension(custom_name, data_type, _) => match custom_name.as_str() {
            ARROW_EXT_TYPE_EMPTY_ARRAY => TableDataType::EmptyArray,
            ARROW_EXT_TYPE_EMPTY_MAP => TableDataType::EmptyMap,
            ARROW_EXT_TYPE_BITMAP => TableDataType::Bitmap,
            ARROW_EXT_TYPE_VARIANT => TableDataType::Variant,
            ARROW_EXT_TYPE_GEOMETRY => TableDataType::Geometry,
            _ => arrow_type_to_table_type(data_type, is_nullable)?,
        },
        _ => {
            return Err(ErrorCode::UnknownFormat(format!(
                "unsupported arrow data type: {:?}",
                ty
            )));
        }
    });

    if is_nullable {
        Ok(TableDataType::Nullable(Box::new(ty)))
    } else {
        Ok(ty)
    }
}

impl Column {
    pub fn from_arrow(
        arrow_col: &dyn databend_common_arrow::arrow::array::Array,
        data_type: &DataType,
    ) -> Result<Column> {
        fn from_arrow_with_arrow_type(
            arrow_col: &dyn databend_common_arrow::arrow::array::Array,
            arrow_type: &ArrowDataType,
            data_type: &DataType,
        ) -> Result<Column> {
            let column = match (data_type, arrow_type) {
                (DataType::Null, ArrowDataType::Null) => Column::Null {
                    len: arrow_col.len(),
                },
                (DataType::EmptyArray, ArrowDataType::Extension(name, _, _))
                    if name == ARROW_EXT_TYPE_EMPTY_ARRAY =>
                {
                    Column::EmptyArray {
                        len: arrow_col.len(),
                    }
                }
                (DataType::EmptyMap, ArrowDataType::Extension(name, _, _))
                    if name == ARROW_EXT_TYPE_EMPTY_MAP =>
                {
                    Column::EmptyMap {
                        len: arrow_col.len(),
                    }
                }
                (DataType::Number(NumberDataType::UInt8), ArrowDataType::UInt8) => {
                    Column::Number(NumberColumn::UInt8(
                        arrow_col
                            .as_any()
                            .downcast_ref::<databend_common_arrow::arrow::array::UInt8Array>()
                            .expect("fail to read `UInt8` from arrow: array should be `UInt8Array`")
                            .values()
                            .clone(),
                    ))
                }
                (DataType::Number(NumberDataType::UInt16), ArrowDataType::UInt16) => {
                    Column::Number(NumberColumn::UInt16(
                        arrow_col
                            .as_any()
                            .downcast_ref::<databend_common_arrow::arrow::array::UInt16Array>()
                            .expect(
                                "fail to read `UInt16` from arrow: array should be `UInt16Array`",
                            )
                            .values()
                            .clone(),
                    ))
                }
                (DataType::Number(NumberDataType::UInt32), ArrowDataType::UInt32) => {
                    Column::Number(NumberColumn::UInt32(
                        arrow_col
                            .as_any()
                            .downcast_ref::<databend_common_arrow::arrow::array::UInt32Array>()
                            .expect(
                                "fail to read `UInt32` from arrow: array should be `UInt32Array`",
                            )
                            .values()
                            .clone(),
                    ))
                }
                (DataType::Number(NumberDataType::UInt64), ArrowDataType::UInt64) => {
                    Column::Number(NumberColumn::UInt64(
                        arrow_col
                            .as_any()
                            .downcast_ref::<databend_common_arrow::arrow::array::UInt64Array>()
                            .expect(
                                "fail to read `UInt64` from arrow: array should be `UInt64Array`",
                            )
                            .values()
                            .clone(),
                    ))
                }
                (DataType::Number(NumberDataType::Int8), ArrowDataType::Int8) => {
                    Column::Number(NumberColumn::Int8(
                        arrow_col
                            .as_any()
                            .downcast_ref::<databend_common_arrow::arrow::array::Int8Array>()
                            .expect("fail to read `Int8` from arrow: array should be `Int8Array`")
                            .values()
                            .clone(),
                    ))
                }
                (DataType::Number(NumberDataType::Int16), ArrowDataType::Int16) => {
                    Column::Number(NumberColumn::Int16(
                        arrow_col
                            .as_any()
                            .downcast_ref::<databend_common_arrow::arrow::array::Int16Array>()
                            .expect("fail to read `Int16` from arrow: array should be `Int16Array`")
                            .values()
                            .clone(),
                    ))
                }
                (DataType::Number(NumberDataType::Int32), ArrowDataType::Int32) => {
                    Column::Number(NumberColumn::Int32(
                        arrow_col
                            .as_any()
                            .downcast_ref::<databend_common_arrow::arrow::array::Int32Array>()
                            .expect("fail to read `Int32` from arrow: array should be `Int32Array`")
                            .values()
                            .clone(),
                    ))
                }
                (DataType::Number(NumberDataType::Int64), ArrowDataType::Int64) => {
                    Column::Number(NumberColumn::Int64(
                        arrow_col
                            .as_any()
                            .downcast_ref::<databend_common_arrow::arrow::array::Int64Array>()
                            .expect("fail to read `Int64` from arrow: array should be `Int64Array`")
                            .values()
                            .clone(),
                    ))
                }
                (DataType::Number(NumberDataType::Float32), ArrowDataType::Float32) => {
                    let col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::Float32Array>()
                        .expect("fail to read `Float32` from arrow: array should be `Float32Array`")
                        .values()
                        .clone();
                    let col = unsafe { std::mem::transmute::<Buffer<f32>, Buffer<F32>>(col) };
                    Column::Number(NumberColumn::Float32(col))
                }
                (DataType::Number(NumberDataType::Float64), ArrowDataType::Float64) => {
                    let col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::Float64Array>()
                        .expect("fail to read `Float64` from arrow: array should be `Float64Array`")
                        .values()
                        .clone();
                    let col = unsafe { std::mem::transmute::<Buffer<f64>, Buffer<F64>>(col) };
                    Column::Number(NumberColumn::Float64(col))
                }
                (
                    DataType::Decimal(DecimalDataType::Decimal128(size)),
                    ArrowDataType::Decimal(precision, scale),
                ) if size.precision as usize == *precision && size.scale as usize == *scale => {
                    let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<databend_common_arrow::arrow::array::PrimitiveArray<i128>>()
                    .expect("fail to read `Decimal128` from arrow: array should be `PrimitiveArray<i128>`");
                    Column::Decimal(DecimalColumn::Decimal128(
                        arrow_col.values().clone(),
                        DecimalSize {
                            precision: *precision as u8,
                            scale: *scale as u8,
                        },
                    ))
                }
                (
                    DataType::Decimal(DecimalDataType::Decimal256(size)),
                    ArrowDataType::Decimal256(precision, scale),
                ) if size.precision as usize == *precision && size.scale as usize == *scale => {
                    let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<databend_common_arrow::arrow::array::PrimitiveArray<
                        databend_common_arrow::arrow::types::i256,
                    >>()
                    .expect("fail to read `Decimal256` from arrow: array should be `PrimitiveArray<i256>`");
                    let values = unsafe {
                        std::mem::transmute::<
                            Buffer<databend_common_arrow::arrow::types::i256>,
                            Buffer<ethnum::I256>,
                        >(arrow_col.values().clone())
                    };
                    Column::Decimal(DecimalColumn::Decimal256(values, DecimalSize {
                        precision: *precision as u8,
                        scale: *scale as u8,
                    }))
                }
                (DataType::Boolean, ArrowDataType::Boolean) => Column::Boolean(
                    arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BooleanArray>()
                        .expect("fail to read `Boolean` from arrow: array should be `BooleanArray`")
                        .values()
                        .clone(),
                ),
                (DataType::Binary, ArrowDataType::Binary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i32>>()
                        .expect(
                            "fail to read `Binary` from arrow: array should be `BinaryArray<i32>`",
                        );
                    let offsets = arrow_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    Column::Binary(BinaryColumn::new(
                        arrow_col.values().clone(),
                        offsets.into(),
                    ))
                }
                (DataType::Binary, ArrowDataType::LargeBinary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i64>>()
                        .expect(
                            "fail to read `Binary` from arrow: array should be `BinaryArray<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    Column::Binary(BinaryColumn::new(arrow_col.values().clone(), offsets))
                }
                (DataType::Binary, ArrowDataType::FixedSizeBinary(size)) => {
                    let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<databend_common_arrow::arrow::array::FixedSizeBinaryArray>()
                    .expect(
                        "fail to read `Binary` from arrow: array should be `FixedSizeBinaryArray`",
                    );
                    let offsets = (0..arrow_col.len() as u64 + 1)
                        .map(|x| x * (*size) as u64)
                        .collect::<Vec<_>>();
                    Column::Binary(BinaryColumn::new(
                        arrow_col.values().clone(),
                        offsets.into(),
                    ))
                }
                (DataType::Binary, ArrowDataType::Utf8) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::Utf8Array<i32>>()
                        .expect(
                            "fail to read `Binary` from arrow: array should be `Utf8Array<i32>`",
                        );
                    let offsets = arrow_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    Column::Binary(BinaryColumn::new(
                        arrow_col.values().clone(),
                        offsets.into(),
                    ))
                }
                (DataType::Binary, ArrowDataType::LargeUtf8) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::Utf8Array<i64>>()
                        .expect(
                            "fail to read `Binary` from arrow: array should be `Utf8Array<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    Column::Binary(BinaryColumn::new(arrow_col.values().clone(), offsets))
                }
                (DataType::String, ArrowDataType::Binary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i32>>()
                        .expect(
                            "fail to read `String` from arrow: array should be `BinaryArray<i32>`",
                        );
                    let offsets = arrow_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    let column = StringColumn::new(arrow_col.values().clone(), offsets.into());
                    Column::String(column)
                }
                (DataType::String, ArrowDataType::LargeBinary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i64>>()
                        .expect(
                            "fail to read `String` from arrow: array should be `BinaryArray<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    let column = StringColumn::new(arrow_col.values().clone(), offsets);
                    Column::String(column)
                }
                (DataType::String, ArrowDataType::FixedSizeBinary(size)) => {
                    let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<databend_common_arrow::arrow::array::FixedSizeBinaryArray>()
                    .expect(
                        "fail to read `String` from arrow: array should be `FixedSizeBinaryArray`",
                    );
                    let offsets = (0..arrow_col.len() as u64 + 1)
                        .map(|x| x * (*size) as u64)
                        .collect::<Vec<_>>();
                    let column = StringColumn::new(arrow_col.values().clone(), offsets.into());
                    Column::String(column)
                }
                (DataType::String, ArrowDataType::Utf8) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::Utf8Array<i32>>()
                        .expect(
                            "fail to read `String` from arrow: array should be `Utf8Array<i32>`",
                        );
                    let offsets = arrow_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    unsafe {
                        Column::String(StringColumn::new_unchecked(
                            arrow_col.values().clone(),
                            offsets.into(),
                        ))
                    }
                }
                (DataType::String, ArrowDataType::LargeUtf8) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::Utf8Array<i64>>()
                        .expect(
                            "fail to read `String` from arrow: array should be `Utf8Array<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    unsafe {
                        Column::String(StringColumn::new_unchecked(
                            arrow_col.values().clone(),
                            offsets,
                        ))
                    }
                }
                (DataType::Timestamp, ArrowDataType::Timestamp(uint, _)) => {
                    let values = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::Int64Array>()
                        .expect("fail to read `Timestamp` from arrow: array should be `Int64Array`")
                        .values();
                    let convert = match uint {
                        TimeUnit::Second => (1_000_000, 1),
                        TimeUnit::Millisecond => (1_000, 1),
                        TimeUnit::Microsecond => (1, 1),
                        TimeUnit::Nanosecond => (1, 1_000),
                    };
                    let values = if convert.0 == 1 && convert.1 == 1 {
                        values.clone()
                    } else {
                        let values = values
                            .iter()
                            .map(|x| x * convert.0 / convert.1)
                            .collect::<Vec<_>>();
                        values.into()
                    };
                    Column::Timestamp(values)
                }
                (DataType::Date, ArrowDataType::Date32) => Column::Date(
                    arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::Int32Array>()
                        .expect("fail to read `Date` from arrow: array should be `Int32Array`")
                        .values()
                        .clone(),
                ),
                (
                    DataType::Variant,
                    ArrowDataType::Extension(name, box ArrowDataType::Binary, None),
                ) if name == ARROW_EXT_TYPE_VARIANT => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i32>>()
                        .expect("fail to read from arrow: array should be `BinaryArray<i32>`");
                    let offsets = arrow_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    Column::Variant(BinaryColumn::new(
                        arrow_col.values().clone(),
                        offsets.into(),
                    ))
                }
                (DataType::Variant, ArrowDataType::Binary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i32>>()
                        .expect("fail to read from arrow: array should be `BinaryArray<i32>`");
                    let offsets = arrow_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    Column::Variant(BinaryColumn::new(
                        arrow_col.values().clone(),
                        offsets.into(),
                    ))
                }
                (
                    DataType::Variant,
                    ArrowDataType::Extension(name, box ArrowDataType::LargeBinary, None),
                ) if name == ARROW_EXT_TYPE_VARIANT => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i64>>()
                        .expect(
                            "fail to read `Variant` from arrow: array should be `BinaryArray<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    Column::Variant(BinaryColumn::new(arrow_col.values().clone(), offsets))
                }
                (DataType::Variant, ArrowDataType::LargeBinary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i64>>()
                        .expect(
                            "fail to read `Variant` from arrow: array should be `BinaryArray<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    Column::Variant(BinaryColumn::new(arrow_col.values().clone(), offsets))
                }
                (DataType::Array(ty), ArrowDataType::List(_)) => {
                    let values_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::ListArray<i32>>()
                        .expect(
                            "fail to read `Array` from arrow: array should be `ListArray<i32>`",
                        );
                    let values = Column::from_arrow(&**values_col.values(), ty)?;
                    let offsets = values_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    Column::Array(Box::new(ArrayColumn {
                        values,
                        offsets: offsets.into(),
                    }))
                }
                (DataType::Array(ty), ArrowDataType::LargeList(_)) => {
                    let values_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::ListArray<i64>>()
                        .expect(
                            "fail to read `Array` from arrow: array should be `ListArray<i64>`",
                        );
                    let values = Column::from_arrow(&**values_col.values(), ty)?;
                    let offsets = values_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    Column::Array(Box::new(ArrayColumn { values, offsets }))
                }
                (DataType::Map(ty), ArrowDataType::Map(_, _)) => {
                    let map_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::MapArray>()
                        .expect("fail to read `Map` from arrow: array should be `MapArray`");
                    let values = Column::from_arrow(&**map_col.field(), ty)?;
                    let offsets = map_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    Column::Map(Box::new(ArrayColumn {
                        values,
                        offsets: offsets.into(),
                    }))
                }
                (DataType::Tuple(fields), ArrowDataType::Struct(_)) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::StructArray>()
                        .expect("fail to read from arrow: array should be `StructArray`");
                    let field_cols = arrow_col
                        .values()
                        .iter()
                        .zip(fields)
                        .map(|(field_col, f)| Column::from_arrow(&**field_col, f))
                        .collect::<Result<Vec<_>>>()?;
                    Column::Tuple(field_cols)
                }
                (
                    DataType::Bitmap,
                    ArrowDataType::Extension(name, box ArrowDataType::Binary, None),
                ) if name == ARROW_EXT_TYPE_BITMAP => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i32>>()
                        .expect(
                            "fail to read `Bitmap` from arrow: array should be `BinaryArray<i32>`",
                        );
                    let offsets = arrow_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    Column::Bitmap(BinaryColumn::new(
                        arrow_col.values().clone(),
                        offsets.into(),
                    ))
                }
                (
                    DataType::Bitmap,
                    ArrowDataType::Extension(name, box ArrowDataType::LargeBinary, None),
                ) if name == ARROW_EXT_TYPE_BITMAP => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i64>>()
                        .expect(
                            "fail to read `Bitmap` from arrow: array should be `BinaryArray<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    Column::Bitmap(BinaryColumn::new(arrow_col.values().clone(), offsets))
                }
                (DataType::Bitmap, ArrowDataType::Binary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i32>>()
                        .expect(
                            "fail to read `Bitmap` from arrow: array should be `BinaryArray<i32>`",
                        );
                    let offsets = arrow_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    Column::Bitmap(BinaryColumn::new(
                        arrow_col.values().clone(),
                        offsets.into(),
                    ))
                }
                (DataType::Bitmap, ArrowDataType::LargeBinary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i64>>()
                        .expect(
                            "fail to read `Bitmap` from arrow: array should be `BinaryArray<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    Column::Bitmap(BinaryColumn::new(arrow_col.values().clone(), offsets))
                }
                (
                    DataType::Geometry,
                    ArrowDataType::Extension(name, box ArrowDataType::Binary, None),
                ) if name == ARROW_EXT_TYPE_GEOMETRY => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i32>>()
                        .expect(
                            "fail to read `Geometry` from arrow: array should be `BinaryArray<i32>`",
                        );
                    let offsets = arrow_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    Column::Geometry(BinaryColumn::new(
                        arrow_col.values().clone(),
                        offsets.into(),
                    ))
                }
                (
                    DataType::Geometry,
                    ArrowDataType::Extension(name, box ArrowDataType::LargeBinary, None),
                ) if name == ARROW_EXT_TYPE_GEOMETRY => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i64>>()
                        .expect(
                            "fail to read `Geometry` from arrow: array should be `BinaryArray<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    Column::Geometry(BinaryColumn::new(arrow_col.values().clone(), offsets))
                }
                (DataType::Geometry, ArrowDataType::Binary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i32>>()
                        .expect(
                            "fail to read `Geometry` from arrow: array should be `BinaryArray<i32>`",
                        );
                    let offsets = arrow_col
                        .offsets()
                        .buffer()
                        .iter()
                        .map(|x| *x as u64)
                        .collect::<Vec<_>>();
                    Column::Geometry(BinaryColumn::new(
                        arrow_col.values().clone(),
                        offsets.into(),
                    ))
                }
                (DataType::Geometry, ArrowDataType::LargeBinary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i64>>()
                        .expect(
                            "fail to read `Geometry` from arrow: array should be `BinaryArray<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    Column::Geometry(BinaryColumn::new(arrow_col.values().clone(), offsets))
                }
                (DataType::Geography, ArrowDataType::LargeBinary) => {
                    let arrow_col = arrow_col
                        .as_any()
                        .downcast_ref::<databend_common_arrow::arrow::array::BinaryArray<i64>>()
                        .expect(
                            "fail to read `Geography` from arrow: array should be `BinaryArray<i64>`",
                        );
                    let offsets = arrow_col.offsets().clone().into_inner();
                    let offsets =
                        unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                    Column::Geography(GeographyColumn(BinaryColumn::new(
                        arrow_col.values().clone(),
                        offsets,
                    )))
                }
                (data_type, ArrowDataType::Extension(_, arrow_type, _)) => {
                    from_arrow_with_arrow_type(arrow_col, arrow_type, data_type)?
                }
                (DataType::Nullable(ty), _) => {
                    let column = Column::from_arrow(arrow_col, ty)?;
                    let validity = arrow_col
                        .validity()
                        .cloned()
                        .unwrap_or_else(|| Bitmap::new_constant(true, arrow_col.len()));
                    NullableColumn::new_column(column, validity)
                }
                (ty, arrow_ty) => {
                    return Err(ErrorCode::Unimplemented(format!(
                        "conversion from arrow type {arrow_ty:?} to {ty:?} is not supported"
                    )));
                }
            };
            Ok(column)
        }

        from_arrow_with_arrow_type(arrow_col, arrow_col.data_type(), data_type)
    }
}
