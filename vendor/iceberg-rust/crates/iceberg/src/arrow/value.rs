// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, FieldRef};
use uuid::Uuid;

use super::get_field_id_from_metadata;
use crate::spec::{
    ListType, Literal, Map, MapType, NestedField, PartnerAccessor, PrimitiveLiteral, PrimitiveType,
    SchemaWithPartnerVisitor, Struct, StructType, Type, visit_struct_with_partner,
    visit_type_with_partner,
};
use crate::{Error, ErrorKind, Result};

struct ArrowArrayToIcebergStructConverter;

impl SchemaWithPartnerVisitor<ArrayRef> for ArrowArrayToIcebergStructConverter {
    type T = Vec<Option<Literal>>;

    fn schema(
        &mut self,
        _schema: &crate::spec::Schema,
        _partner: &ArrayRef,
        value: Vec<Option<Literal>>,
    ) -> Result<Vec<Option<Literal>>> {
        Ok(value)
    }

    fn field(
        &mut self,
        field: &crate::spec::NestedFieldRef,
        _partner: &ArrayRef,
        value: Vec<Option<Literal>>,
    ) -> Result<Vec<Option<Literal>>> {
        // Make there is no null value if the field is required
        if field.required && value.iter().any(Option::is_none) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The field is required but has null value",
            )
            .with_context("field_id", field.id.to_string())
            .with_context("field_name", &field.name));
        }
        Ok(value)
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        array: &ArrayRef,
        results: Vec<Vec<Option<Literal>>>,
    ) -> Result<Vec<Option<Literal>>> {
        let row_len = results.first().map(|column| column.len()).unwrap_or(0);
        if let Some(col) = results.iter().find(|col| col.len() != row_len) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The struct columns have different row length",
            )
            .with_context("first col length", row_len.to_string())
            .with_context("actual col length", col.len().to_string()));
        }

        let mut struct_literals = Vec::with_capacity(row_len);
        let mut columns_iters = results
            .into_iter()
            .map(|column| column.into_iter())
            .collect::<Vec<_>>();

        for i in 0..row_len {
            let mut literals = Vec::with_capacity(columns_iters.len());
            for column_iter in columns_iters.iter_mut() {
                literals.push(column_iter.next().unwrap());
            }
            if array.is_null(i) {
                struct_literals.push(None);
            } else {
                struct_literals.push(Some(Literal::Struct(Struct::from_iter(literals))));
            }
        }

        Ok(struct_literals)
    }

    fn list(
        &mut self,
        list: &ListType,
        array: &ArrayRef,
        elements: Vec<Option<Literal>>,
    ) -> Result<Vec<Option<Literal>>> {
        if list.element_field.required && elements.iter().any(Option::is_none) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The list should not have null value",
            ));
        }
        match array.data_type() {
            DataType::List(_) => {
                let offset = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a list array")
                    })?
                    .offsets();
                // combine the result according to the offset
                let mut result = Vec::with_capacity(offset.len() - 1);
                for i in 0..offset.len() - 1 {
                    let start = offset[i] as usize;
                    let end = offset[i + 1] as usize;
                    result.push(Some(Literal::List(elements[start..end].to_vec())));
                }
                Ok(result)
            }
            DataType::LargeList(_) => {
                let offset = array
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a large list array",
                        )
                    })?
                    .offsets();
                // combine the result according to the offset
                let mut result = Vec::with_capacity(offset.len() - 1);
                for i in 0..offset.len() - 1 {
                    let start = offset[i] as usize;
                    let end = offset[i + 1] as usize;
                    result.push(Some(Literal::List(elements[start..end].to_vec())));
                }
                Ok(result)
            }
            DataType::FixedSizeList(_, len) => {
                let mut result = Vec::with_capacity(elements.len() / *len as usize);
                for i in 0..elements.len() / *len as usize {
                    let start = i * *len as usize;
                    let end = (i + 1) * *len as usize;
                    result.push(Some(Literal::List(elements[start..end].to_vec())));
                }
                Ok(result)
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                "The partner is not a list type",
            )),
        }
    }

    fn map(
        &mut self,
        _map: &MapType,
        partner: &ArrayRef,
        key_values: Vec<Option<Literal>>,
        values: Vec<Option<Literal>>,
    ) -> Result<Vec<Option<Literal>>> {
        // Make sure key_value and value have the same row length
        if key_values.len() != values.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The key value and value of map should have the same row length",
            ));
        }

        let offsets = partner
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "The partner is not a map array"))?
            .offsets();
        // combine the result according to the offset
        let mut result = Vec::with_capacity(offsets.len() - 1);
        for i in 0..offsets.len() - 1 {
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            let mut map = Map::new();
            for (key, value) in key_values[start..end].iter().zip(values[start..end].iter()) {
                map.insert(key.clone().unwrap(), value.clone());
            }
            result.push(Some(Literal::Map(map)));
        }
        Ok(result)
    }

    fn primitive(&mut self, p: &PrimitiveType, partner: &ArrayRef) -> Result<Vec<Option<Literal>>> {
        match p {
            PrimitiveType::Boolean => {
                let array = partner
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a boolean array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::bool)).collect())
            }
            PrimitiveType::Int => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a int32 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::int)).collect())
            }
            PrimitiveType::Long => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a int64 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::long)).collect())
            }
            PrimitiveType::Float => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a float32 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::float)).collect())
            }
            PrimitiveType::Double => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a float64 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::double)).collect())
            }
            PrimitiveType::Decimal { precision, scale } => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a decimal128 array",
                        )
                    })?;
                if let DataType::Decimal128(arrow_precision, arrow_scale) = array.data_type()
                    && (*arrow_precision as u32 != *precision || *arrow_scale as u32 != *scale)
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "The precision or scale ({arrow_precision},{arrow_scale}) of arrow decimal128 array is not compatible with iceberg decimal type ({precision},{scale})"
                        ),
                    ));
                }
                Ok(array.iter().map(|v| v.map(Literal::decimal)).collect())
            }
            PrimitiveType::Date => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a date32 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::date)).collect())
            }
            PrimitiveType::Time => {
                let array = partner
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a time64 array")
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::time)).collect())
            }
            PrimitiveType::Timestamp => {
                let array = partner
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a timestamp array",
                        )
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::timestamp)).collect())
            }
            PrimitiveType::Timestamptz => {
                let array = partner
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a timestamptz array",
                        )
                    })?;
                Ok(array.iter().map(|v| v.map(Literal::timestamptz)).collect())
            }
            PrimitiveType::TimestampNs => {
                let array = partner
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a timestamp_ns array",
                        )
                    })?;
                Ok(array
                    .iter()
                    .map(|v| v.map(Literal::timestamp_nano))
                    .collect())
            }
            PrimitiveType::TimestamptzNs => {
                let array = partner
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a timestamptz_ns array",
                        )
                    })?;
                Ok(array
                    .iter()
                    .map(|v| v.map(Literal::timestamptz_nano))
                    .collect())
            }
            PrimitiveType::String => {
                if let Some(array) = partner.as_any().downcast_ref::<LargeStringArray>() {
                    Ok(array.iter().map(|v| v.map(Literal::string)).collect())
                } else if let Some(array) = partner.as_any().downcast_ref::<StringArray>() {
                    Ok(array.iter().map(|v| v.map(Literal::string)).collect())
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The partner is not a string array",
                    ))
                }
            }
            PrimitiveType::Uuid => {
                if let Some(array) = partner.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    if array.value_length() != 16 {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "The partner is not a uuid array",
                        ));
                    }
                    Ok(array
                        .iter()
                        .map(|v| {
                            v.map(|v| {
                                Ok(Literal::uuid(Uuid::from_bytes(v.try_into().map_err(
                                    |_| {
                                        Error::new(
                                            ErrorKind::DataInvalid,
                                            "Failed to convert binary to uuid",
                                        )
                                    },
                                )?)))
                            })
                            .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?)
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The partner is not a uuid array",
                    ))
                }
            }
            PrimitiveType::Fixed(len) => {
                let array = partner
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "The partner is not a fixed array")
                    })?;
                if array.value_length() != *len as i32 {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The length of fixed size binary array is not compatible with iceberg fixed type",
                    ));
                }
                Ok(array
                    .iter()
                    .map(|v| v.map(|v| Literal::fixed(v.iter().cloned())))
                    .collect())
            }
            PrimitiveType::Binary => {
                if let Some(array) = partner.as_any().downcast_ref::<LargeBinaryArray>() {
                    Ok(array
                        .iter()
                        .map(|v| v.map(|v| Literal::binary(v.to_vec())))
                        .collect())
                } else if let Some(array) = partner.as_any().downcast_ref::<BinaryArray>() {
                    Ok(array
                        .iter()
                        .map(|v| v.map(|v| Literal::binary(v.to_vec())))
                        .collect())
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The partner is not a binary array",
                    ))
                }
            }
        }
    }
}

/// Defines how Arrow fields are matched with Iceberg fields when converting data.
///
/// This enum provides two strategies for matching fields:
/// - `Id`: Match fields by their ID, which is stored in Arrow field metadata.
/// - `Name`: Match fields by their name, ignoring the field ID.
///
/// The ID matching mode is the default and preferred approach as it's more robust
/// against schema evolution where field names might change but IDs remain stable.
/// The name matching mode can be useful in scenarios where field IDs are not available
/// or when working with systems that don't preserve field IDs.
#[derive(Clone, Copy, Debug)]
pub enum FieldMatchMode {
    /// Match fields by their ID stored in Arrow field metadata
    Id,
    /// Match fields by their name, ignoring field IDs
    Name,
}

impl FieldMatchMode {
    /// Determines if an Arrow field matches an Iceberg field based on the matching mode.
    pub fn match_field(&self, arrow_field: &FieldRef, iceberg_field: &NestedField) -> bool {
        match self {
            FieldMatchMode::Id => get_field_id_from_metadata(arrow_field)
                .map(|id| id == iceberg_field.id)
                .unwrap_or(false),
            FieldMatchMode::Name => arrow_field.name() == &iceberg_field.name,
        }
    }
}

/// Partner type representing accessing and walking arrow arrays alongside iceberg schema
pub struct ArrowArrayAccessor {
    match_mode: FieldMatchMode,
}

impl ArrowArrayAccessor {
    /// Creates a new instance of ArrowArrayAccessor with the default ID matching mode
    pub fn new() -> Self {
        Self {
            match_mode: FieldMatchMode::Id,
        }
    }

    /// Creates a new instance of ArrowArrayAccessor with the specified matching mode
    pub fn new_with_match_mode(match_mode: FieldMatchMode) -> Self {
        Self { match_mode }
    }
}

impl Default for ArrowArrayAccessor {
    fn default() -> Self {
        Self::new()
    }
}

impl PartnerAccessor<ArrayRef> for ArrowArrayAccessor {
    fn struct_partner<'a>(&self, schema_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        if !matches!(schema_partner.data_type(), DataType::Struct(_)) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "The schema partner is not a struct type",
            ));
        }

        Ok(schema_partner)
    }

    fn field_partner<'a>(
        &self,
        struct_partner: &'a ArrayRef,
        field: &NestedField,
    ) -> Result<&'a ArrayRef> {
        let struct_array = struct_partner
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "The struct partner is not a struct array, partner: {struct_partner:?}"
                    ),
                )
            })?;

        let field_pos = struct_array
            .fields()
            .iter()
            .position(|arrow_field| self.match_mode.match_field(arrow_field, field))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Field id {} not found in struct array", field.id),
                )
            })?;

        Ok(struct_array.column(field_pos))
    }

    fn list_element_partner<'a>(&self, list_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        match list_partner.data_type() {
            DataType::List(_) => {
                let list_array = list_partner
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The list partner is not a list array",
                        )
                    })?;
                Ok(list_array.values())
            }
            DataType::LargeList(_) => {
                let list_array = list_partner
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The list partner is not a large list array",
                        )
                    })?;
                Ok(list_array.values())
            }
            DataType::FixedSizeList(_, _) => {
                let list_array = list_partner
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "The list partner is not a fixed size list array",
                        )
                    })?;
                Ok(list_array.values())
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                "The list partner is not a list type",
            )),
        }
    }

    fn map_key_partner<'a>(&self, map_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        let map_array = map_partner
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "The map partner is not a map array")
            })?;
        Ok(map_array.keys())
    }

    fn map_value_partner<'a>(&self, map_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        let map_array = map_partner
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "The map partner is not a map array")
            })?;
        Ok(map_array.values())
    }
}

/// Convert arrow struct array to iceberg struct value array.
/// This function will assume the schema of arrow struct array is the same as iceberg struct type.
pub fn arrow_struct_to_literal(
    struct_array: &ArrayRef,
    ty: &StructType,
) -> Result<Vec<Option<Literal>>> {
    visit_struct_with_partner(
        ty,
        struct_array,
        &mut ArrowArrayToIcebergStructConverter,
        &ArrowArrayAccessor::new(),
    )
}

/// Convert arrow primitive array to iceberg primitive value array.
/// This function will assume the schema of arrow struct array is the same as iceberg struct type.
pub fn arrow_primitive_to_literal(
    primitive_array: &ArrayRef,
    ty: &Type,
) -> Result<Vec<Option<Literal>>> {
    visit_type_with_partner(
        ty,
        primitive_array,
        &mut ArrowArrayToIcebergStructConverter,
        &ArrowArrayAccessor::new(),
    )
}

/// Create a single-element array from a primitive literal.
///
/// This is used for creating constant arrays (Run-End Encoded arrays) where we need
/// a single value that represents all rows.
pub(crate) fn create_primitive_array_single_element(
    data_type: &DataType,
    prim_lit: &Option<PrimitiveLiteral>,
) -> Result<ArrayRef> {
    match (data_type, prim_lit) {
        (DataType::Boolean, Some(PrimitiveLiteral::Boolean(v))) => {
            Ok(Arc::new(BooleanArray::from(vec![*v])))
        }
        (DataType::Boolean, None) => Ok(Arc::new(BooleanArray::from(vec![Option::<bool>::None]))),
        (DataType::Int32, Some(PrimitiveLiteral::Int(v))) => {
            Ok(Arc::new(Int32Array::from(vec![*v])))
        }
        (DataType::Int32, None) => Ok(Arc::new(Int32Array::from(vec![Option::<i32>::None]))),
        (DataType::Date32, Some(PrimitiveLiteral::Int(v))) => {
            Ok(Arc::new(Date32Array::from(vec![*v])))
        }
        (DataType::Date32, None) => Ok(Arc::new(Date32Array::from(vec![Option::<i32>::None]))),
        (DataType::Int64, Some(PrimitiveLiteral::Long(v))) => {
            Ok(Arc::new(Int64Array::from(vec![*v])))
        }
        (DataType::Int64, None) => Ok(Arc::new(Int64Array::from(vec![Option::<i64>::None]))),
        (DataType::Float32, Some(PrimitiveLiteral::Float(v))) => {
            Ok(Arc::new(Float32Array::from(vec![v.0])))
        }
        (DataType::Float32, None) => Ok(Arc::new(Float32Array::from(vec![Option::<f32>::None]))),
        (DataType::Float64, Some(PrimitiveLiteral::Double(v))) => {
            Ok(Arc::new(Float64Array::from(vec![v.0])))
        }
        (DataType::Float64, None) => Ok(Arc::new(Float64Array::from(vec![Option::<f64>::None]))),
        (DataType::Utf8, Some(PrimitiveLiteral::String(v))) => {
            Ok(Arc::new(StringArray::from(vec![v.as_str()])))
        }
        (DataType::Utf8, None) => Ok(Arc::new(StringArray::from(vec![Option::<&str>::None]))),
        (DataType::Binary, Some(PrimitiveLiteral::Binary(v))) => {
            Ok(Arc::new(BinaryArray::from_vec(vec![v.as_slice()])))
        }
        (DataType::Binary, None) => Ok(Arc::new(BinaryArray::from_opt_vec(vec![
            Option::<&[u8]>::None,
        ]))),
        (DataType::Decimal128(precision, scale), Some(PrimitiveLiteral::Int128(v))) => {
            let array = Decimal128Array::from(vec![{ *v }])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Failed to create Decimal128Array with precision {precision} and scale {scale}: {e}"
                        ),
                    )
                })?;
            Ok(Arc::new(array))
        }
        (DataType::Decimal128(precision, scale), Some(PrimitiveLiteral::UInt128(v))) => {
            let array = Decimal128Array::from(vec![*v as i128])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Failed to create Decimal128Array with precision {precision} and scale {scale}: {e}"
                        ),
                    )
                })?;
            Ok(Arc::new(array))
        }
        (DataType::Decimal128(precision, scale), None) => {
            let array = Decimal128Array::from(vec![Option::<i128>::None])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Failed to create Decimal128Array with precision {precision} and scale {scale}: {e}"
                        ),
                    )
                })?;
            Ok(Arc::new(array))
        }
        (DataType::Struct(fields), None) => {
            // Create a single-element StructArray with nulls
            let null_arrays: Vec<ArrayRef> = fields
                .iter()
                .map(|f| {
                    // Recursively create null arrays for struct fields
                    // For primitive fields in structs, use simple null arrays (not REE within struct)
                    match f.data_type() {
                        DataType::Boolean => {
                            Ok(Arc::new(BooleanArray::from(vec![Option::<bool>::None]))
                                as ArrayRef)
                        }
                        DataType::Int32 | DataType::Date32 => {
                            Ok(Arc::new(Int32Array::from(vec![Option::<i32>::None])) as ArrayRef)
                        }
                        DataType::Int64 => {
                            Ok(Arc::new(Int64Array::from(vec![Option::<i64>::None])) as ArrayRef)
                        }
                        DataType::Float32 => {
                            Ok(Arc::new(Float32Array::from(vec![Option::<f32>::None])) as ArrayRef)
                        }
                        DataType::Float64 => {
                            Ok(Arc::new(Float64Array::from(vec![Option::<f64>::None])) as ArrayRef)
                        }
                        DataType::Utf8 => {
                            Ok(Arc::new(StringArray::from(vec![Option::<&str>::None])) as ArrayRef)
                        }
                        DataType::Binary => {
                            Ok(
                                Arc::new(BinaryArray::from_opt_vec(vec![Option::<&[u8]>::None]))
                                    as ArrayRef,
                            )
                        }
                        _ => Err(Error::new(
                            ErrorKind::Unexpected,
                            format!("Unsupported struct field type: {:?}", f.data_type()),
                        )),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(arrow_array::StructArray::new(
                fields.clone(),
                null_arrays,
                Some(arrow_buffer::NullBuffer::new_null(1)),
            )))
        }
        _ => Err(Error::new(
            ErrorKind::Unexpected,
            format!("Unsupported constant type combination: {data_type:?} with {prim_lit:?}"),
        )),
    }
}

/// Create a repeated array from a primitive literal for a given number of rows.
///
/// This is used for creating non-constant arrays where we need the same value
/// repeated for each row.
pub(crate) fn create_primitive_array_repeated(
    data_type: &DataType,
    prim_lit: &Option<PrimitiveLiteral>,
    num_rows: usize,
) -> Result<ArrayRef> {
    Ok(match (data_type, prim_lit) {
        (DataType::Boolean, Some(PrimitiveLiteral::Boolean(value))) => {
            Arc::new(BooleanArray::from(vec![*value; num_rows]))
        }
        (DataType::Boolean, None) => {
            let vals: Vec<Option<bool>> = vec![None; num_rows];
            Arc::new(BooleanArray::from(vals))
        }
        (DataType::Int32, Some(PrimitiveLiteral::Int(value))) => {
            Arc::new(Int32Array::from(vec![*value; num_rows]))
        }
        (DataType::Int32, None) => {
            let vals: Vec<Option<i32>> = vec![None; num_rows];
            Arc::new(Int32Array::from(vals))
        }
        (DataType::Date32, Some(PrimitiveLiteral::Int(value))) => {
            Arc::new(Date32Array::from(vec![*value; num_rows]))
        }
        (DataType::Date32, None) => {
            let vals: Vec<Option<i32>> = vec![None; num_rows];
            Arc::new(Date32Array::from(vals))
        }
        (DataType::Int64, Some(PrimitiveLiteral::Long(value))) => {
            Arc::new(Int64Array::from(vec![*value; num_rows]))
        }
        (DataType::Int64, None) => {
            let vals: Vec<Option<i64>> = vec![None; num_rows];
            Arc::new(Int64Array::from(vals))
        }
        (DataType::Float32, Some(PrimitiveLiteral::Float(value))) => {
            Arc::new(Float32Array::from(vec![value.0; num_rows]))
        }
        (DataType::Float32, None) => {
            let vals: Vec<Option<f32>> = vec![None; num_rows];
            Arc::new(Float32Array::from(vals))
        }
        (DataType::Float64, Some(PrimitiveLiteral::Double(value))) => {
            Arc::new(Float64Array::from(vec![value.0; num_rows]))
        }
        (DataType::Float64, None) => {
            let vals: Vec<Option<f64>> = vec![None; num_rows];
            Arc::new(Float64Array::from(vals))
        }
        (DataType::Utf8, Some(PrimitiveLiteral::String(value))) => {
            Arc::new(StringArray::from(vec![value.clone(); num_rows]))
        }
        (DataType::Utf8, None) => {
            let vals: Vec<Option<String>> = vec![None; num_rows];
            Arc::new(StringArray::from(vals))
        }
        (DataType::Binary, Some(PrimitiveLiteral::Binary(value))) => {
            Arc::new(BinaryArray::from_vec(vec![value; num_rows]))
        }
        (DataType::Binary, None) => {
            let vals: Vec<Option<&[u8]>> = vec![None; num_rows];
            Arc::new(BinaryArray::from_opt_vec(vals))
        }
        (DataType::Decimal128(precision, scale), Some(PrimitiveLiteral::Int128(value))) => {
            Arc::new(
                Decimal128Array::from(vec![*value; num_rows])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Failed to create Decimal128Array with precision {precision} and scale {scale}: {e}"
                            ),
                        )
                    })?,
            )
        }
        (DataType::Decimal128(precision, scale), Some(PrimitiveLiteral::UInt128(value))) => {
            Arc::new(
                Decimal128Array::from(vec![*value as i128; num_rows])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Failed to create Decimal128Array with precision {precision} and scale {scale}: {e}"
                            ),
                        )
                    })?,
            )
        }
        (DataType::Decimal128(precision, scale), None) => {
            let vals: Vec<Option<i128>> = vec![None; num_rows];
            Arc::new(
                Decimal128Array::from(vals)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Failed to create Decimal128Array with precision {precision} and scale {scale}: {e}"
                            ),
                        )
                    })?,
            )
        }
        (DataType::Struct(fields), None) => {
            // Create a StructArray filled with nulls
            let null_arrays: Vec<ArrayRef> = fields
                .iter()
                .map(|field| create_primitive_array_repeated(field.data_type(), &None, num_rows))
                .collect::<Result<Vec<_>>>()?;

            Arc::new(StructArray::new(
                fields.clone(),
                null_arrays,
                Some(NullBuffer::new_null(num_rows)),
            ))
        }
        (DataType::Null, _) => Arc::new(arrow_array::NullArray::new(num_rows)),
        (dt, _) => {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("unexpected target column type {dt}"),
            ));
        }
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StructBuilder};
    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
        Float64Array, Int32Array, Int64Array, StringArray, StructArray, Time64MicrosecondArray,
        TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use arrow_schema::{DataType, Field, Fields, TimeUnit};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;
    use crate::spec::{ListType, Literal, MapType, NestedField, PrimitiveType, StructType, Type};

    #[test]
    fn test_arrow_struct_to_iceberg_struct() {
        let bool_array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let int32_array = Int32Array::from(vec![Some(3), Some(4), None]);
        let int64_array = Int64Array::from(vec![Some(5), Some(6), None]);
        let float32_array = Float32Array::from(vec![Some(1.1), Some(2.2), None]);
        let float64_array = Float64Array::from(vec![Some(3.3), Some(4.4), None]);
        let decimal_array = Decimal128Array::from(vec![Some(1000), Some(2000), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let date_array = Date32Array::from(vec![Some(18628), Some(18629), None]);
        let time_array = Time64MicrosecondArray::from(vec![Some(123456789), Some(987654321), None]);
        let timestamp_micro_array = TimestampMicrosecondArray::from(vec![
            Some(1622548800000000),
            Some(1622635200000000),
            None,
        ]);
        let timestamp_nano_array = TimestampNanosecondArray::from(vec![
            Some(1622548800000000000),
            Some(1622635200000000000),
            None,
        ]);
        let string_array = StringArray::from(vec![Some("a"), Some("b"), None]);
        let binary_array =
            BinaryArray::from(vec![Some(b"abc".as_ref()), Some(b"def".as_ref()), None]);

        let struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(
                    Field::new("bool_field", DataType::Boolean, true).with_metadata(HashMap::from(
                        [(PARQUET_FIELD_ID_META_KEY.to_string(), "0".to_string())],
                    )),
                ),
                Arc::new(bool_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("int32_field", DataType::Int32, true).with_metadata(HashMap::from(
                        [(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())],
                    )),
                ),
                Arc::new(int32_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("int64_field", DataType::Int64, true).with_metadata(HashMap::from(
                        [(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())],
                    )),
                ),
                Arc::new(int64_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("float32_field", DataType::Float32, true).with_metadata(
                        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string())]),
                    ),
                ),
                Arc::new(float32_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("float64_field", DataType::Float64, true).with_metadata(
                        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "5".to_string())]),
                    ),
                ),
                Arc::new(float64_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("decimal_field", DataType::Decimal128(10, 2), true).with_metadata(
                        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "6".to_string())]),
                    ),
                ),
                Arc::new(decimal_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("date_field", DataType::Date32, true).with_metadata(HashMap::from(
                        [(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())],
                    )),
                ),
                Arc::new(date_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("time_field", DataType::Time64(TimeUnit::Microsecond), true)
                        .with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            "8".to_string(),
                        )])),
                ),
                Arc::new(time_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new(
                        "timestamp_micro_field",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "9".to_string(),
                    )])),
                ),
                Arc::new(timestamp_micro_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new(
                        "timestamp_nano_field",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "10".to_string(),
                    )])),
                ),
                Arc::new(timestamp_nano_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("string_field", DataType::Utf8, true).with_metadata(HashMap::from(
                        [(PARQUET_FIELD_ID_META_KEY.to_string(), "11".to_string())],
                    )),
                ),
                Arc::new(string_array) as ArrayRef,
            ),
            (
                Arc::new(
                    Field::new("binary_field", DataType::Binary, true).with_metadata(
                        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "12".to_string())]),
                    ),
                ),
                Arc::new(binary_array) as ArrayRef,
            ),
        ])) as ArrayRef;

        let iceberg_struct_type = StructType::new(vec![
            Arc::new(NestedField::optional(
                0,
                "bool_field",
                Type::Primitive(PrimitiveType::Boolean),
            )),
            Arc::new(NestedField::optional(
                2,
                "int32_field",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                3,
                "int64_field",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                4,
                "float32_field",
                Type::Primitive(PrimitiveType::Float),
            )),
            Arc::new(NestedField::optional(
                5,
                "float64_field",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                6,
                "decimal_field",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                }),
            )),
            Arc::new(NestedField::optional(
                7,
                "date_field",
                Type::Primitive(PrimitiveType::Date),
            )),
            Arc::new(NestedField::optional(
                8,
                "time_field",
                Type::Primitive(PrimitiveType::Time),
            )),
            Arc::new(NestedField::optional(
                9,
                "timestamp_micro_field",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::optional(
                10,
                "timestamp_nao_field",
                Type::Primitive(PrimitiveType::TimestampNs),
            )),
            Arc::new(NestedField::optional(
                11,
                "string_field",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                12,
                "binary_field",
                Type::Primitive(PrimitiveType::Binary),
            )),
        ]);

        let result = arrow_struct_to_literal(&struct_array, &iceberg_struct_type).unwrap();

        assert_eq!(result, vec![
            Some(Literal::Struct(Struct::from_iter(vec![
                Some(Literal::bool(true)),
                Some(Literal::int(3)),
                Some(Literal::long(5)),
                Some(Literal::float(1.1)),
                Some(Literal::double(3.3)),
                Some(Literal::decimal(1000)),
                Some(Literal::date(18628)),
                Some(Literal::time(123456789)),
                Some(Literal::timestamp(1622548800000000)),
                Some(Literal::timestamp_nano(1622548800000000000)),
                Some(Literal::string("a".to_string())),
                Some(Literal::binary(b"abc".to_vec())),
            ]))),
            Some(Literal::Struct(Struct::from_iter(vec![
                Some(Literal::bool(false)),
                Some(Literal::int(4)),
                Some(Literal::long(6)),
                Some(Literal::float(2.2)),
                Some(Literal::double(4.4)),
                Some(Literal::decimal(2000)),
                Some(Literal::date(18629)),
                Some(Literal::time(987654321)),
                Some(Literal::timestamp(1622635200000000)),
                Some(Literal::timestamp_nano(1622635200000000000)),
                Some(Literal::string("b".to_string())),
                Some(Literal::binary(b"def".to_vec())),
            ]))),
            Some(Literal::Struct(Struct::from_iter(vec![
                None, None, None, None, None, None, None, None, None, None, None, None,
            ]))),
        ]);
    }

    #[test]
    fn test_nullable_struct() {
        // test case that partial columns are null
        // [
        //   {a: null, b: null} // child column is null
        //   {a: 1, b: null},   // partial child column is null
        //   null               // parent column is null
        // ]
        let struct_array = {
            let mut builder = StructBuilder::from_fields(
                Fields::from(vec![
                    Field::new("a", DataType::Int32, true).with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "0".to_string(),
                    )])),
                    Field::new("b", DataType::Int32, true).with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "1".to_string(),
                    )])),
                ]),
                3,
            );
            builder
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_null();
            builder
                .field_builder::<Int32Builder>(1)
                .unwrap()
                .append_null();
            builder.append(true);

            builder
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value(1);
            builder
                .field_builder::<Int32Builder>(1)
                .unwrap()
                .append_null();
            builder.append(true);

            builder
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_value(1);
            builder
                .field_builder::<Int32Builder>(1)
                .unwrap()
                .append_value(1);
            builder.append_null();

            Arc::new(builder.finish()) as ArrayRef
        };

        let iceberg_struct_type = StructType::new(vec![
            Arc::new(NestedField::optional(
                0,
                "a",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                1,
                "b",
                Type::Primitive(PrimitiveType::Int),
            )),
        ]);

        let result = arrow_struct_to_literal(&struct_array, &iceberg_struct_type).unwrap();
        assert_eq!(result, vec![
            Some(Literal::Struct(Struct::from_iter(vec![None, None,]))),
            Some(Literal::Struct(Struct::from_iter(vec![
                Some(Literal::int(1)),
                None,
            ]))),
            None,
        ]);
    }

    #[test]
    fn test_empty_struct() {
        let struct_array = Arc::new(StructArray::new_null(Fields::empty(), 3)) as ArrayRef;
        let iceberg_struct_type = StructType::new(vec![]);
        let result = arrow_struct_to_literal(&struct_array, &iceberg_struct_type).unwrap();
        assert_eq!(result, vec![None; 0]);
    }

    #[test]
    fn test_find_field_by_id() {
        // Create Arrow arrays for the nested structure
        let field_a_array = Int32Array::from(vec![Some(42), Some(43), None]);
        let field_b_array = StringArray::from(vec![Some("value1"), Some("value2"), None]);

        // Create the nested struct array with field IDs in metadata
        let nested_struct_array =
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("field_a", DataType::Int32, true).with_metadata(
                        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
                    )),
                    Arc::new(field_a_array) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("field_b", DataType::Utf8, true).with_metadata(
                        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
                    )),
                    Arc::new(field_b_array) as ArrayRef,
                ),
            ])) as ArrayRef;

        let field_c_array = Int32Array::from(vec![Some(100), Some(200), None]);

        // Create the top-level struct array with field IDs in metadata
        let struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(
                    Field::new(
                        "nested_struct",
                        DataType::Struct(Fields::from(vec![
                            Field::new("field_a", DataType::Int32, true).with_metadata(
                                HashMap::from([(
                                    PARQUET_FIELD_ID_META_KEY.to_string(),
                                    "1".to_string(),
                                )]),
                            ),
                            Field::new("field_b", DataType::Utf8, true).with_metadata(
                                HashMap::from([(
                                    PARQUET_FIELD_ID_META_KEY.to_string(),
                                    "2".to_string(),
                                )]),
                            ),
                        ])),
                        true,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "3".to_string(),
                    )])),
                ),
                nested_struct_array,
            ),
            (
                Arc::new(Field::new("field_c", DataType::Int32, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string())]),
                )),
                Arc::new(field_c_array) as ArrayRef,
            ),
        ])) as ArrayRef;

        // Create an ArrowArrayAccessor with ID matching mode
        let accessor = ArrowArrayAccessor::new_with_match_mode(FieldMatchMode::Id);

        // Test finding fields by ID
        let nested_field = NestedField::optional(
            3,
            "nested_struct",
            Type::Struct(StructType::new(vec![
                Arc::new(NestedField::optional(
                    1,
                    "field_a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "field_b",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])),
        );
        let nested_partner = accessor
            .field_partner(&struct_array, &nested_field)
            .unwrap();

        // Verify we can access the nested field
        let field_a = NestedField::optional(1, "field_a", Type::Primitive(PrimitiveType::Int));
        let field_a_partner = accessor.field_partner(nested_partner, &field_a).unwrap();

        // Verify the field has the expected value
        let int_array = field_a_partner
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int_array.value(0), 42);
        assert_eq!(int_array.value(1), 43);
        assert!(int_array.is_null(2));
    }

    #[test]
    fn test_find_field_by_name() {
        // Create Arrow arrays for the nested structure
        let field_a_array = Int32Array::from(vec![Some(42), Some(43), None]);
        let field_b_array = StringArray::from(vec![Some("value1"), Some("value2"), None]);

        // Create the nested struct array WITHOUT field IDs in metadata
        let nested_struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("field_a", DataType::Int32, true)),
                Arc::new(field_a_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("field_b", DataType::Utf8, true)),
                Arc::new(field_b_array) as ArrayRef,
            ),
        ])) as ArrayRef;

        let field_c_array = Int32Array::from(vec![Some(100), Some(200), None]);

        // Create the top-level struct array WITHOUT field IDs in metadata
        let struct_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "nested_struct",
                    DataType::Struct(Fields::from(vec![
                        Field::new("field_a", DataType::Int32, true),
                        Field::new("field_b", DataType::Utf8, true),
                    ])),
                    true,
                )),
                nested_struct_array,
            ),
            (
                Arc::new(Field::new("field_c", DataType::Int32, true)),
                Arc::new(field_c_array) as ArrayRef,
            ),
        ])) as ArrayRef;

        // Create an ArrowArrayAccessor with Name matching mode
        let accessor = ArrowArrayAccessor::new_with_match_mode(FieldMatchMode::Name);

        // Test finding fields by name
        let nested_field = NestedField::optional(
            3,
            "nested_struct",
            Type::Struct(StructType::new(vec![
                Arc::new(NestedField::optional(
                    1,
                    "field_a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "field_b",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])),
        );
        let nested_partner = accessor
            .field_partner(&struct_array, &nested_field)
            .unwrap();

        // Verify we can access the nested field by name
        let field_a = NestedField::optional(1, "field_a", Type::Primitive(PrimitiveType::Int));
        let field_a_partner = accessor.field_partner(nested_partner, &field_a).unwrap();

        // Verify the field has the expected value
        let int_array = field_a_partner
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int_array.value(0), 42);
        assert_eq!(int_array.value(1), 43);
        assert!(int_array.is_null(2));
    }

    #[test]
    fn test_complex_nested() {
        // complex nested type for test
        // <
        //   A: list< struct(a1: int, a2: int) >,
        //   B: list< map<int, int> >,
        //   C: list< list<int> >,
        // >
        let struct_type = StructType::new(vec![
            Arc::new(NestedField::required(
                0,
                "A",
                Type::List(ListType::new(Arc::new(NestedField::required(
                    1,
                    "item",
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::required(
                            2,
                            "a1",
                            Type::Primitive(PrimitiveType::Int),
                        )),
                        Arc::new(NestedField::required(
                            3,
                            "a2",
                            Type::Primitive(PrimitiveType::Int),
                        )),
                    ])),
                )))),
            )),
            Arc::new(NestedField::required(
                4,
                "B",
                Type::List(ListType::new(Arc::new(NestedField::required(
                    5,
                    "item",
                    Type::Map(MapType::new(
                        NestedField::optional(6, "keys", Type::Primitive(PrimitiveType::Int))
                            .into(),
                        NestedField::optional(7, "values", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    )),
                )))),
            )),
            Arc::new(NestedField::required(
                8,
                "C",
                Type::List(ListType::new(Arc::new(NestedField::required(
                    9,
                    "item",
                    Type::List(ListType::new(Arc::new(NestedField::optional(
                        10,
                        "item",
                        Type::Primitive(PrimitiveType::Int),
                    )))),
                )))),
            )),
        ]);

        // Generate a complex nested struct array
        // [
        //   {A: [{a1: 10, a2: 20}, {a1: 11, a2: 21}], B: [{(1,100),(3,300)},{(2,200)}], C: [[100,101,102], [200,201]]},
        //   {A: [{a1: 12, a2: 22}, {a1: 13, a2: 23}], B: [{(3,300)},{(4,400)}], C: [[300,301,302], [400,401]]},
        // ]
        let struct_array =
            {
                let a_struct_a1_builder = Int32Builder::new();
                let a_struct_a2_builder = Int32Builder::new();
                let a_struct_builder =
                    StructBuilder::new(
                        vec![
                            Field::new("a1", DataType::Int32, false).with_metadata(HashMap::from(
                                [(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())],
                            )),
                            Field::new("a2", DataType::Int32, false).with_metadata(HashMap::from(
                                [(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())],
                            )),
                        ],
                        vec![Box::new(a_struct_a1_builder), Box::new(a_struct_a2_builder)],
                    );
                let a_builder = ListBuilder::new(a_struct_builder);

                let map_key_builder = Int32Builder::new();
                let map_value_builder = Int32Builder::new();
                let map_builder = MapBuilder::new(None, map_key_builder, map_value_builder);
                let b_builder = ListBuilder::new(map_builder);

                let inner_list_item_builder = Int32Builder::new();
                let inner_list_builder = ListBuilder::new(inner_list_item_builder);
                let c_builder = ListBuilder::new(inner_list_builder);

                let mut top_struct_builder = {
                    let a_struct_type =
                        DataType::Struct(Fields::from(vec![
                            Field::new("a1", DataType::Int32, false).with_metadata(HashMap::from(
                                [(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())],
                            )),
                            Field::new("a2", DataType::Int32, false).with_metadata(HashMap::from(
                                [(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())],
                            )),
                        ]));
                    let a_type =
                        DataType::List(Arc::new(Field::new("item", a_struct_type.clone(), true)));

                    let b_map_entry_struct = Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("keys", DataType::Int32, false),
                            Field::new("values", DataType::Int32, true),
                        ])),
                        false,
                    );
                    let b_map_type =
                        DataType::Map(Arc::new(b_map_entry_struct), /* sorted_keys = */ false);
                    let b_type =
                        DataType::List(Arc::new(Field::new("item", b_map_type.clone(), true)));

                    let c_inner_list_type =
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
                    let c_type = DataType::List(Arc::new(Field::new(
                        "item",
                        c_inner_list_type.clone(),
                        true,
                    )));
                    StructBuilder::new(
                        Fields::from(vec![
                            Field::new("A", a_type.clone(), false).with_metadata(HashMap::from([
                                (PARQUET_FIELD_ID_META_KEY.to_string(), "0".to_string()),
                            ])),
                            Field::new("B", b_type.clone(), false).with_metadata(HashMap::from([
                                (PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string()),
                            ])),
                            Field::new("C", c_type.clone(), false).with_metadata(HashMap::from([
                                (PARQUET_FIELD_ID_META_KEY.to_string(), "8".to_string()),
                            ])),
                        ]),
                        vec![
                            Box::new(a_builder),
                            Box::new(b_builder),
                            Box::new(c_builder),
                        ],
                    )
                };

                // first row
                // {A: [{a1: 10, a2: 20}, {a1: 11, a2: 21}], B: [{(1,100),(3,300)},{(2,200)}], C: [[100,101,102], [200,201]]},
                {
                    let a_builder = top_struct_builder
                        .field_builder::<ListBuilder<StructBuilder>>(0)
                        .unwrap();
                    let struct_builder = a_builder.values();
                    struct_builder
                        .field_builder::<Int32Builder>(0)
                        .unwrap()
                        .append_value(10);
                    struct_builder
                        .field_builder::<Int32Builder>(1)
                        .unwrap()
                        .append_value(20);
                    struct_builder.append(true);
                    let struct_builder = a_builder.values();
                    struct_builder
                        .field_builder::<Int32Builder>(0)
                        .unwrap()
                        .append_value(11);
                    struct_builder
                        .field_builder::<Int32Builder>(1)
                        .unwrap()
                        .append_value(21);
                    struct_builder.append(true);
                    a_builder.append(true);
                }
                {
                    let b_builder = top_struct_builder
                        .field_builder::<ListBuilder<MapBuilder<Int32Builder, Int32Builder>>>(1)
                        .unwrap();
                    let map_builder = b_builder.values();
                    map_builder.keys().append_value(1);
                    map_builder.values().append_value(100);
                    map_builder.keys().append_value(3);
                    map_builder.values().append_value(300);
                    map_builder.append(true).unwrap();

                    map_builder.keys().append_value(2);
                    map_builder.values().append_value(200);
                    map_builder.append(true).unwrap();

                    b_builder.append(true);
                }
                {
                    let c_builder = top_struct_builder
                        .field_builder::<ListBuilder<ListBuilder<Int32Builder>>>(2)
                        .unwrap();
                    let inner_list_builder = c_builder.values();
                    inner_list_builder.values().append_value(100);
                    inner_list_builder.values().append_value(101);
                    inner_list_builder.values().append_value(102);
                    inner_list_builder.append(true);
                    let inner_list_builder = c_builder.values();
                    inner_list_builder.values().append_value(200);
                    inner_list_builder.values().append_value(201);
                    inner_list_builder.append(true);
                    c_builder.append(true);
                }
                top_struct_builder.append(true);

                // second row
                // {A: [{a1: 12, a2: 22}, {a1: 13, a2: 23}], B: [{(3,300)}], C: [[300,301,302], [400,401]]},
                {
                    let a_builder = top_struct_builder
                        .field_builder::<ListBuilder<StructBuilder>>(0)
                        .unwrap();
                    let struct_builder = a_builder.values();
                    struct_builder
                        .field_builder::<Int32Builder>(0)
                        .unwrap()
                        .append_value(12);
                    struct_builder
                        .field_builder::<Int32Builder>(1)
                        .unwrap()
                        .append_value(22);
                    struct_builder.append(true);
                    let struct_builder = a_builder.values();
                    struct_builder
                        .field_builder::<Int32Builder>(0)
                        .unwrap()
                        .append_value(13);
                    struct_builder
                        .field_builder::<Int32Builder>(1)
                        .unwrap()
                        .append_value(23);
                    struct_builder.append(true);
                    a_builder.append(true);
                }
                {
                    let b_builder = top_struct_builder
                        .field_builder::<ListBuilder<MapBuilder<Int32Builder, Int32Builder>>>(1)
                        .unwrap();
                    let map_builder = b_builder.values();
                    map_builder.keys().append_value(3);
                    map_builder.values().append_value(300);
                    map_builder.append(true).unwrap();

                    b_builder.append(true);
                }
                {
                    let c_builder = top_struct_builder
                        .field_builder::<ListBuilder<ListBuilder<Int32Builder>>>(2)
                        .unwrap();
                    let inner_list_builder = c_builder.values();
                    inner_list_builder.values().append_value(300);
                    inner_list_builder.values().append_value(301);
                    inner_list_builder.values().append_value(302);
                    inner_list_builder.append(true);
                    let inner_list_builder = c_builder.values();
                    inner_list_builder.values().append_value(400);
                    inner_list_builder.values().append_value(401);
                    inner_list_builder.append(true);
                    c_builder.append(true);
                }
                top_struct_builder.append(true);

                Arc::new(top_struct_builder.finish()) as ArrayRef
            };

        let result = arrow_struct_to_literal(&struct_array, &struct_type).unwrap();
        assert_eq!(result, vec![
            Some(Literal::Struct(Struct::from_iter(vec![
                Some(Literal::List(vec![
                    Some(Literal::Struct(Struct::from_iter(vec![
                        Some(Literal::int(10)),
                        Some(Literal::int(20)),
                    ]))),
                    Some(Literal::Struct(Struct::from_iter(vec![
                        Some(Literal::int(11)),
                        Some(Literal::int(21)),
                    ]))),
                ])),
                Some(Literal::List(vec![
                    Some(Literal::Map(Map::from_iter(vec![
                        (Literal::int(1), Some(Literal::int(100))),
                        (Literal::int(3), Some(Literal::int(300))),
                    ]))),
                    Some(Literal::Map(Map::from_iter(vec![(
                        Literal::int(2),
                        Some(Literal::int(200))
                    ),]))),
                ])),
                Some(Literal::List(vec![
                    Some(Literal::List(vec![
                        Some(Literal::int(100)),
                        Some(Literal::int(101)),
                        Some(Literal::int(102)),
                    ])),
                    Some(Literal::List(vec![
                        Some(Literal::int(200)),
                        Some(Literal::int(201)),
                    ])),
                ])),
            ]))),
            Some(Literal::Struct(Struct::from_iter(vec![
                Some(Literal::List(vec![
                    Some(Literal::Struct(Struct::from_iter(vec![
                        Some(Literal::int(12)),
                        Some(Literal::int(22)),
                    ]))),
                    Some(Literal::Struct(Struct::from_iter(vec![
                        Some(Literal::int(13)),
                        Some(Literal::int(23)),
                    ]))),
                ])),
                Some(Literal::List(vec![Some(Literal::Map(Map::from_iter(
                    vec![(Literal::int(3), Some(Literal::int(300))),]
                ))),])),
                Some(Literal::List(vec![
                    Some(Literal::List(vec![
                        Some(Literal::int(300)),
                        Some(Literal::int(301)),
                        Some(Literal::int(302)),
                    ])),
                    Some(Literal::List(vec![
                        Some(Literal::int(400)),
                        Some(Literal::int(401)),
                    ])),
                ])),
            ]))),
        ]);
    }

    #[test]
    fn test_create_decimal_array_respects_precision() {
        // Decimal128Array::from() uses Arrow's default precision (38) instead of the
        // target precision, causing RecordBatch construction to fail when schemas don't match.
        let target_precision = 18u8;
        let target_scale = 10i8;
        let target_type = DataType::Decimal128(target_precision, target_scale);
        let value = PrimitiveLiteral::Int128(10000000000);

        let array = create_primitive_array_single_element(&target_type, &Some(value))
            .expect("Failed to create decimal array");

        match array.data_type() {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(*precision, target_precision);
                assert_eq!(*scale, target_scale);
            }
            other => panic!("Expected Decimal128, got {other:?}"),
        }
    }

    #[test]
    fn test_create_decimal_array_repeated_respects_precision() {
        // Ensure repeated arrays also respect target precision, not Arrow's default.
        let target_precision = 18u8;
        let target_scale = 10i8;
        let target_type = DataType::Decimal128(target_precision, target_scale);
        let value = PrimitiveLiteral::Int128(10000000000);
        let num_rows = 5;

        let array = create_primitive_array_repeated(&target_type, &Some(value), num_rows)
            .expect("Failed to create repeated decimal array");

        match array.data_type() {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(*precision, target_precision);
                assert_eq!(*scale, target_scale);
            }
            other => panic!("Expected Decimal128, got {other:?}"),
        }

        assert_eq!(array.len(), num_rows);
    }
}
