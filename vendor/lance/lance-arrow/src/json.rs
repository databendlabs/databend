// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! JSON support for Apache Arrow.

use std::convert::TryFrom;
use std::sync::Arc;

use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{Array, ArrayRef, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Schema};

use crate::ARROW_EXT_NAME_KEY;

/// Arrow extension type name for JSON data (Lance internal)
pub const JSON_EXT_NAME: &str = "lance.json";

/// Arrow extension type name for JSON data (Arrow official)
pub const ARROW_JSON_EXT_NAME: &str = "arrow.json";

/// Check if a field is a JSON extension field (Lance internal JSONB storage)
pub fn is_json_field(field: &ArrowField) -> bool {
    field.data_type() == &DataType::LargeBinary
        && field
            .metadata()
            .get(ARROW_EXT_NAME_KEY)
            .map(|name| name == JSON_EXT_NAME)
            .unwrap_or_default()
}

/// Check if a field is an Arrow JSON extension field (PyArrow pa.json() type)
pub fn is_arrow_json_field(field: &ArrowField) -> bool {
    // Arrow JSON extension type uses Utf8 or LargeUtf8 as storage type
    (field.data_type() == &DataType::Utf8 || field.data_type() == &DataType::LargeUtf8)
        && field
            .metadata()
            .get(ARROW_EXT_NAME_KEY)
            .map(|name| name == ARROW_JSON_EXT_NAME)
            .unwrap_or_default()
}

/// Check if a field or any of its descendants is a JSON field
pub fn has_json_fields(field: &ArrowField) -> bool {
    if is_json_field(field) {
        return true;
    }

    match field.data_type() {
        DataType::Struct(fields) => fields.iter().any(|f| has_json_fields(f)),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            has_json_fields(f)
        }
        DataType::Map(f, _) => has_json_fields(f),
        _ => false,
    }
}

/// Create a JSON field with the appropriate extension metadata
pub fn json_field(name: &str, nullable: bool) -> ArrowField {
    let mut field = ArrowField::new(name, DataType::LargeBinary, nullable);
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(ARROW_EXT_NAME_KEY.to_string(), JSON_EXT_NAME.to_string());
    field.set_metadata(metadata);
    field
}

/// A specialized array for JSON data stored as JSONB binary format
#[derive(Debug, Clone)]
pub struct JsonArray {
    inner: LargeBinaryArray,
}

impl JsonArray {
    /// Create a new JsonArray from an iterator of JSON strings
    pub fn try_from_iter<I, S>(iter: I) -> Result<Self, ArrowError>
    where
        I: IntoIterator<Item = Option<S>>,
        S: AsRef<str>,
    {
        let mut builder = LargeBinaryBuilder::new();

        for json_str in iter {
            match json_str {
                Some(s) => {
                    let encoded = encode_json(s.as_ref()).map_err(|e| {
                        ArrowError::InvalidArgumentError(format!("Failed to encode JSON: {}", e))
                    })?;
                    builder.append_value(&encoded);
                }
                None => builder.append_null(),
            }
        }

        Ok(Self {
            inner: builder.finish(),
        })
    }

    /// Get the underlying LargeBinaryArray
    pub fn into_inner(self) -> LargeBinaryArray {
        self.inner
    }

    /// Get a reference to the underlying LargeBinaryArray
    pub fn inner(&self) -> &LargeBinaryArray {
        &self.inner
    }

    /// Get the value at index i as decoded JSON string
    pub fn value(&self, i: usize) -> Result<String, ArrowError> {
        if self.inner.is_null(i) {
            return Err(ArrowError::InvalidArgumentError(
                "Value is null".to_string(),
            ));
        }

        let jsonb_bytes = self.inner.value(i);
        decode_json(jsonb_bytes)
            .map_err(|e| ArrowError::InvalidArgumentError(format!("Failed to decode JSON: {}", e)))
    }

    /// Get the value at index i as raw JSONB bytes
    pub fn value_bytes(&self, i: usize) -> &[u8] {
        self.inner.value(i)
    }

    /// Get JSONPath value from the JSON at index i
    pub fn json_path(&self, i: usize, path: &str) -> Result<Option<String>, ArrowError> {
        if self.inner.is_null(i) {
            return Ok(None);
        }

        let jsonb_bytes = self.inner.value(i);
        get_json_path(jsonb_bytes, path).map_err(|e| {
            ArrowError::InvalidArgumentError(format!("Failed to extract JSONPath: {}", e))
        })
    }

    /// Convert to Arrow string array (JSON as UTF-8)
    pub fn to_arrow_json(&self) -> Result<ArrayRef, ArrowError> {
        let mut builder = arrow_array::builder::StringBuilder::new();

        for i in 0..self.len() {
            if self.is_null(i) {
                builder.append_null();
            } else {
                let jsonb_bytes = self.inner.value(i);
                let json_str = decode_json(jsonb_bytes).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!("Failed to decode JSON: {}", e))
                })?;
                builder.append_value(&json_str);
            }
        }

        // Return as UTF-8 string array (Arrow represents JSON as strings)
        Ok(Arc::new(builder.finish()))
    }
}

impl Array for JsonArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.inner.to_data()
    }

    fn into_data(self) -> ArrayData {
        self.inner.into_data()
    }

    fn data_type(&self) -> &DataType {
        &DataType::LargeBinary
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self {
            inner: self.inner.slice(offset, length),
        })
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn offset(&self) -> usize {
        self.inner.offset()
    }

    fn nulls(&self) -> Option<&arrow_buffer::NullBuffer> {
        self.inner.nulls()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.inner.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.inner.get_array_memory_size()
    }
}

// TryFrom implementations for string arrays
impl TryFrom<StringArray> for JsonArray {
    type Error = ArrowError;

    fn try_from(array: StringArray) -> Result<Self, Self::Error> {
        Self::try_from(&array)
    }
}

impl TryFrom<&StringArray> for JsonArray {
    type Error = ArrowError;

    fn try_from(array: &StringArray) -> Result<Self, Self::Error> {
        let mut builder = LargeBinaryBuilder::with_capacity(array.len(), array.value_data().len());

        for i in 0..array.len() {
            if array.is_null(i) {
                builder.append_null();
            } else {
                let json_str = array.value(i);
                let encoded = encode_json(json_str).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!("Failed to encode JSON: {}", e))
                })?;
                builder.append_value(&encoded);
            }
        }

        Ok(Self {
            inner: builder.finish(),
        })
    }
}

impl TryFrom<LargeStringArray> for JsonArray {
    type Error = ArrowError;

    fn try_from(array: LargeStringArray) -> Result<Self, Self::Error> {
        Self::try_from(&array)
    }
}

impl TryFrom<&LargeStringArray> for JsonArray {
    type Error = ArrowError;

    fn try_from(array: &LargeStringArray) -> Result<Self, Self::Error> {
        let mut builder = LargeBinaryBuilder::with_capacity(array.len(), array.value_data().len());

        for i in 0..array.len() {
            if array.is_null(i) {
                builder.append_null();
            } else {
                let json_str = array.value(i);
                let encoded = encode_json(json_str).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!("Failed to encode JSON: {}", e))
                })?;
                builder.append_value(&encoded);
            }
        }

        Ok(Self {
            inner: builder.finish(),
        })
    }
}

impl TryFrom<ArrayRef> for JsonArray {
    type Error = ArrowError;

    fn try_from(array_ref: ArrayRef) -> Result<Self, Self::Error> {
        match array_ref.data_type() {
            DataType::Utf8 => {
                let string_array = array_ref
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        ArrowError::InvalidArgumentError("Failed to downcast to StringArray".into())
                    })?;
                Self::try_from(string_array)
            }
            DataType::LargeUtf8 => {
                let large_string_array = array_ref
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        ArrowError::InvalidArgumentError(
                            "Failed to downcast to LargeStringArray".into(),
                        )
                    })?;
                Self::try_from(large_string_array)
            }
            dt => Err(ArrowError::InvalidArgumentError(format!(
                "Unsupported array type for JSON: {:?}. Expected Utf8 or LargeUtf8",
                dt
            ))),
        }
    }
}

/// Encode JSON string to JSONB format
pub fn encode_json(json_str: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let value = jsonb::parse_value(json_str.as_bytes())?;
    Ok(value.to_vec())
}

/// Decode JSONB bytes to JSON string
pub fn decode_json(jsonb_bytes: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let raw_jsonb = jsonb::RawJsonb::new(jsonb_bytes);
    Ok(raw_jsonb.to_string())
}

/// Extract JSONPath value from JSONB
fn get_json_path(
    jsonb_bytes: &[u8],
    path: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let json_path = jsonb::jsonpath::parse_json_path(path.as_bytes())?;
    let raw_jsonb = jsonb::RawJsonb::new(jsonb_bytes);
    let mut selector = jsonb::jsonpath::Selector::new(raw_jsonb);

    match selector.select_values(&json_path) {
        Ok(values) => {
            if values.is_empty() {
                Ok(None)
            } else {
                Ok(Some(values[0].to_string()))
            }
        }
        Err(e) => Err(Box::new(e)),
    }
}

/// Convert an Arrow JSON field to Lance JSON field (with JSONB storage)
pub fn arrow_json_to_lance_json(field: &ArrowField) -> ArrowField {
    if is_arrow_json_field(field) {
        // Convert Arrow JSON (Utf8/LargeUtf8) to Lance JSON (LargeBinary)
        // Preserve all metadata from the original field
        let mut new_field =
            ArrowField::new(field.name(), DataType::LargeBinary, field.is_nullable());

        // Copy all metadata from the original field
        let mut metadata = field.metadata().clone();
        // Add/override the extension metadata for Lance JSON
        metadata.insert(ARROW_EXT_NAME_KEY.to_string(), JSON_EXT_NAME.to_string());

        new_field = new_field.with_metadata(metadata);
        new_field
    } else {
        field.clone()
    }
}

/// Convert a RecordBatch with Lance JSON columns (JSONB) back to Arrow JSON format (strings)
pub fn convert_lance_json_to_arrow(
    batch: &arrow_array::RecordBatch,
) -> Result<arrow_array::RecordBatch, ArrowError> {
    let schema = batch.schema();
    let mut needs_conversion = false;
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);

        if is_json_field(field) {
            needs_conversion = true;

            // Convert the field back to Arrow JSON (Utf8)
            let mut new_field = ArrowField::new(field.name(), DataType::Utf8, field.is_nullable());
            let mut metadata = field.metadata().clone();
            // Change from lance.json to arrow.json
            metadata.insert(
                ARROW_EXT_NAME_KEY.to_string(),
                ARROW_JSON_EXT_NAME.to_string(),
            );
            new_field.set_metadata(metadata);
            new_fields.push(new_field);

            // Convert the data from JSONB to JSON strings
            if batch.num_rows() == 0 {
                // For empty batches, create an empty String array
                let empty_strings = arrow_array::builder::StringBuilder::new().finish();
                new_columns.push(Arc::new(empty_strings) as ArrayRef);
            } else {
                // Convert JSONB back to JSON strings
                let binary_array = column
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        ArrowError::InvalidArgumentError(format!(
                            "Lance JSON field '{}' has unexpected type",
                            field.name()
                        ))
                    })?;

                let mut builder = arrow_array::builder::StringBuilder::new();
                for i in 0..binary_array.len() {
                    if binary_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let jsonb_bytes = binary_array.value(i);
                        let json_str = decode_json(jsonb_bytes).map_err(|e| {
                            ArrowError::InvalidArgumentError(format!(
                                "Failed to decode JSON: {}",
                                e
                            ))
                        })?;
                        builder.append_value(&json_str);
                    }
                }
                new_columns.push(Arc::new(builder.finish()) as ArrayRef);
            }
        } else {
            new_fields.push(field.as_ref().clone());
            new_columns.push(column.clone());
        }
    }

    if needs_conversion {
        let new_schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ));
        RecordBatch::try_new(new_schema, new_columns)
    } else {
        // No conversion needed, return original batch
        Ok(batch.clone())
    }
}

/// Convert a RecordBatch with Arrow JSON columns to Lance JSON format (JSONB)
pub fn convert_json_columns(
    batch: &arrow_array::RecordBatch,
) -> Result<arrow_array::RecordBatch, ArrowError> {
    let schema = batch.schema();
    let mut needs_conversion = false;
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);

        if is_arrow_json_field(field) {
            needs_conversion = true;

            // Convert the field metadata
            new_fields.push(arrow_json_to_lance_json(field));

            // Convert the data from JSON strings to JSONB
            if batch.num_rows() == 0 {
                // For empty batches, create an empty LargeBinary array
                let empty_binary = LargeBinaryBuilder::new().finish();
                new_columns.push(Arc::new(empty_binary) as ArrayRef);
            } else {
                // Convert non-empty data
                let json_array =
                    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
                        JsonArray::try_from(string_array)?
                    } else if let Some(large_string_array) =
                        column.as_any().downcast_ref::<LargeStringArray>()
                    {
                        JsonArray::try_from(large_string_array)?
                    } else {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Arrow JSON field '{}' has unexpected storage type: {:?}",
                            field.name(),
                            column.data_type()
                        )));
                    };

                let binary_array = json_array.into_inner();

                new_columns.push(Arc::new(binary_array) as ArrayRef);
            }
        } else {
            new_fields.push(field.as_ref().clone());
            new_columns.push(column.clone());
        }
    }

    if needs_conversion {
        let new_schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ));
        RecordBatch::try_new(new_schema, new_columns)
    } else {
        // No conversion needed, return original batch
        Ok(batch.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_field_creation() {
        let field = json_field("data", true);
        assert_eq!(field.name(), "data");
        assert_eq!(field.data_type(), &DataType::LargeBinary);
        assert!(field.is_nullable());
        assert!(is_json_field(&field));
    }

    #[test]
    fn test_json_array_from_strings() {
        let json_strings = vec![
            Some(r#"{"name": "Alice", "age": 30}"#),
            None,
            Some(r#"{"name": "Bob", "age": 25}"#),
        ];

        let array = JsonArray::try_from_iter(json_strings).unwrap();
        assert_eq!(array.len(), 3);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert!(!array.is_null(2));

        let decoded = array.value(0).unwrap();
        assert!(decoded.contains("Alice"));
    }

    #[test]
    fn test_json_array_from_string_array() {
        let string_array = StringArray::from(vec![
            Some(r#"{"name": "Alice"}"#),
            Some(r#"{"name": "Bob"}"#),
            None,
        ]);

        let json_array = JsonArray::try_from(string_array).unwrap();
        assert_eq!(json_array.len(), 3);
        assert!(!json_array.is_null(0));
        assert!(!json_array.is_null(1));
        assert!(json_array.is_null(2));
    }

    #[test]
    fn test_json_path_extraction() {
        let json_array = JsonArray::try_from_iter(vec![
            Some(r#"{"user": {"name": "Alice", "age": 30}}"#),
            Some(r#"{"user": {"name": "Bob"}}"#),
        ])
        .unwrap();

        let name = json_array.json_path(0, "$.user.name").unwrap();
        assert_eq!(name, Some("\"Alice\"".to_string()));

        let age = json_array.json_path(1, "$.user.age").unwrap();
        assert_eq!(age, None);
    }

    #[test]
    fn test_convert_json_columns() {
        // Create a batch with Arrow JSON column
        let json_strings = vec![Some(r#"{"name": "Alice"}"#), Some(r#"{"name": "Bob"}"#)];
        let json_arr = StringArray::from(json_strings);

        // Create field with arrow.json extension
        let mut field = ArrowField::new("data", DataType::Utf8, false);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        field.set_metadata(metadata);

        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(json_arr) as ArrayRef]).unwrap();

        // Convert the batch
        let converted = convert_json_columns(&batch).unwrap();

        // Check the converted schema
        assert_eq!(converted.num_columns(), 1);
        let converted_schema = converted.schema();
        let converted_field = converted_schema.field(0);
        assert_eq!(converted_field.data_type(), &DataType::LargeBinary);
        assert_eq!(
            converted_field.metadata().get(ARROW_EXT_NAME_KEY),
            Some(&JSON_EXT_NAME.to_string())
        );

        // Check the data was converted
        let converted_column = converted.column(0);
        assert_eq!(converted_column.data_type(), &DataType::LargeBinary);
        assert_eq!(converted_column.len(), 2);

        // Verify the data is valid JSONB
        let binary_array = converted_column
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        for i in 0..binary_array.len() {
            let jsonb_bytes = binary_array.value(i);
            let decoded = decode_json(jsonb_bytes).unwrap();
            assert!(decoded.contains("name"));
        }
    }
}
