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
use std::io::Cursor;

use arrow_json::reader::ValueIter;
use arrow_schema::ArrowError;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use serde_json::Value;

// NDJSON inference keeps its own ordered intermediate schema and materializes Databend table
// types directly instead of going through Arrow types. The rules are kept explicit here because
// they differ from Arrow's JSON inference and must be shared by infer_schema and schema evolution:
//
// - `json_max_depth` limits recursive inference for structured values (array/object). The root
//   NDJSON record object is not counted as a table column type; top-level field values start at
//   depth 1. When a structured value reaches the limit, inference stops and the field becomes
//   VARIANT.
// - Null samples only mark the final field nullable; they do not force a non-null scalar or
//   structured type to STRING/VARIANT.
// - Integer and floating point scalar conflicts merge to FLOAT64/DOUBLE. Other non-null scalar
//   conflicts are readable as text, so they merge to STRING.
// - Any conflict that involves a structured type or VARIANT merges to VARIANT. In particular,
//   scalar values mixed with arrays do not promote the column to ARRAY.
// - Empty arrays and empty objects stay refinable while more samples are merged. If they are still
//   empty when materialized, [] becomes ARRAY(VARIANT) and {} becomes VARIANT.
// - Object fields are stored in a HashMap plus a first-seen order vector, so repeated merges avoid
//   per-record linear field lookup while preserving stable output order.
#[derive(Clone, Debug, PartialEq, Eq)]
enum InferredJsonType {
    Any,
    Scalar(TableDataType),
    Array(Box<InferredJsonType>),
    Object(InferredJsonSchema),
    Variant,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct InferredJsonSchema {
    order: Vec<String>,
    fields: HashMap<String, InferredJsonType>,
}

impl InferredJsonSchema {
    pub(crate) fn merge(&mut self, mut other: InferredJsonSchema) {
        for name in other.order {
            let ty = other
                .fields
                .remove(&name)
                .expect("inferred JSON field order must reference an existing field");
            self.merge_field(name, ty);
        }
    }

    pub(crate) fn into_table_schema(mut self) -> TableSchema {
        TableSchema::new(
            self.order
                .into_iter()
                .map(|name| {
                    let ty = self
                        .fields
                        .remove(&name)
                        .expect("inferred JSON field order must reference an existing field");
                    TableField::new(&name, ty.into_table_type().wrap_nullable())
                })
                .collect(),
        )
    }

    fn into_tuple_type(mut self) -> TableDataType {
        if self.order.is_empty() {
            return TableDataType::Variant;
        }

        let (fields_name, fields_type) = self
            .order
            .into_iter()
            .map(|name| {
                let ty = self
                    .fields
                    .remove(&name)
                    .expect("inferred JSON field order must reference an existing field");
                (name, ty.into_table_type().wrap_nullable())
            })
            .unzip();
        TableDataType::Tuple {
            fields_name,
            fields_type,
        }
    }

    fn merge_field(&mut self, name: String, ty: InferredJsonType) {
        if let Some(existing) = self.fields.get_mut(&name) {
            existing.merge(ty);
        } else {
            self.order.push(name.clone());
            self.fields.insert(name, ty);
        }
    }

    fn merge_record(
        &mut self,
        value: Value,
        json_max_depth: Option<usize>,
    ) -> Result<(), ArrowError> {
        match value {
            Value::Object(map) => {
                for (name, value) in map {
                    self.merge_field(name, infer_value(&value, 1, json_max_depth));
                }
                Ok(())
            }
            value => Err(ArrowError::JsonError(format!(
                "Expected JSON record to be an object, found {value:?}"
            ))),
        }
    }
}

impl InferredJsonType {
    fn merge(&mut self, other: InferredJsonType) {
        match self {
            InferredJsonType::Any => {
                *self = other;
            }
            InferredJsonType::Variant => {}
            InferredJsonType::Scalar(left) => match other {
                InferredJsonType::Any => {}
                InferredJsonType::Variant => *self = InferredJsonType::Variant,
                InferredJsonType::Scalar(right) => {
                    if matches!(left, TableDataType::Null) {
                        *left = right;
                    } else if matches!(right, TableDataType::Null) {
                    } else if matches!(
                        (&*left, &right),
                        (
                            TableDataType::Number(NumberDataType::Int64),
                            TableDataType::Number(NumberDataType::Float64)
                        ) | (
                            TableDataType::Number(NumberDataType::Float64),
                            TableDataType::Number(NumberDataType::Int64)
                        )
                    ) {
                        *left = TableDataType::Number(NumberDataType::Float64);
                    } else if left != &right {
                        // Different scalar types cannot be represented precisely as one typed
                        // column, so Databend keeps the value readable as text instead of choosing
                        // an Arrow numeric promotion.
                        *left = TableDataType::String;
                    }
                }
                InferredJsonType::Array(_) | InferredJsonType::Object(_)
                    if matches!(left, TableDataType::Null) =>
                {
                    *self = other;
                }
                InferredJsonType::Array(_) | InferredJsonType::Object(_) => {
                    // Scalar plus array does not promote to array.
                    *self = InferredJsonType::Variant;
                }
            },
            InferredJsonType::Array(left) => match other {
                InferredJsonType::Any => {}
                InferredJsonType::Array(right) => left.merge(*right),
                InferredJsonType::Scalar(TableDataType::Null) => {}
                InferredJsonType::Scalar(_)
                | InferredJsonType::Object(_)
                | InferredJsonType::Variant => *self = InferredJsonType::Variant,
            },
            InferredJsonType::Object(left) => match other {
                InferredJsonType::Any => {}
                InferredJsonType::Object(right) => left.merge(right),
                InferredJsonType::Scalar(TableDataType::Null) => {}
                InferredJsonType::Scalar(_)
                | InferredJsonType::Array(_)
                | InferredJsonType::Variant => *self = InferredJsonType::Variant,
            },
        }
    }

    fn into_table_type(self) -> TableDataType {
        match self {
            InferredJsonType::Any => TableDataType::Null,
            InferredJsonType::Scalar(ty) => ty,
            InferredJsonType::Array(inner) => {
                let inner_type = match *inner {
                    // Empty arrays do not provide an element sample. Keep the in-memory state as
                    // `Any` so later non-empty arrays can still refine the element type, but
                    // materialize an array of VARIANT if the element type remains unknown.
                    InferredJsonType::Any => TableDataType::Variant,
                    inner => inner.into_table_type(),
                };
                TableDataType::Array(Box::new(inner_type.wrap_nullable()))
            }
            InferredJsonType::Object(schema) => schema.into_tuple_type(),
            InferredJsonType::Variant => TableDataType::Variant,
        }
    }
}

fn scalar_type(value: &Value) -> Option<TableDataType> {
    match value {
        Value::Null => Some(TableDataType::Null),
        Value::Bool(_) => Some(TableDataType::Boolean),
        Value::Number(number) if number.is_i64() => {
            Some(TableDataType::Number(NumberDataType::Int64))
        }
        Value::Number(_) => Some(TableDataType::Number(NumberDataType::Float64)),
        Value::String(_) => Some(TableDataType::String),
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn infer_value(value: &Value, depth: usize, json_max_depth: Option<usize>) -> InferredJsonType {
    if let Some(scalar) = scalar_type(value) {
        return InferredJsonType::Scalar(scalar);
    }

    match value {
        Value::Array(values) => {
            if json_max_depth.is_some_and(|max_depth| depth >= max_depth) {
                return InferredJsonType::Variant;
            }
            let mut element_type = InferredJsonType::Any;
            for value in values {
                element_type.merge(infer_value(value, depth + 1, json_max_depth));
            }
            InferredJsonType::Array(Box::new(element_type))
        }
        Value::Object(map) => {
            if json_max_depth.is_some_and(|max_depth| depth >= max_depth) {
                return InferredJsonType::Variant;
            }
            let mut schema = InferredJsonSchema::default();
            for (name, value) in map {
                schema.merge_field(name.clone(), infer_value(value, depth + 1, json_max_depth));
            }
            InferredJsonType::Object(schema)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => unreachable!(),
    }
}

pub(super) fn infer_ndjson_schema_state(
    file_bytes: &[u8],
    max_records: Option<usize>,
    json_max_depth: Option<usize>,
) -> std::result::Result<InferredJsonSchema, ArrowError> {
    let records = ValueIter::new(Cursor::new(file_bytes), max_records);
    let mut schema = InferredJsonSchema::default();

    for record in records {
        schema.merge_record(record?, json_max_depth)?;
    }

    Ok(schema)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::TableDataType;
    use databend_common_expression::types::NumberDataType;

    use super::infer_ndjson_schema_state;

    fn infer_ndjson_schema(
        ndjson: &[u8],
        max_records: Option<usize>,
        json_max_depth: Option<usize>,
    ) -> super::InferredJsonSchema {
        infer_ndjson_schema_state(ndjson, max_records, json_max_depth).unwrap()
    }

    fn field_type(ndjson: &str, field: &str) -> TableDataType {
        infer_ndjson_schema(ndjson.as_bytes(), None, None)
            .into_table_schema()
            .field_with_name(field)
            .unwrap()
            .data_type()
            .clone()
    }

    #[test]
    fn test_scalar_conflicts_become_string() {
        assert_eq!(
            field_type("{\"a\":true}\n{\"a\":\"yes\"}\n", "a"),
            TableDataType::String.wrap_nullable()
        );
    }

    #[test]
    fn test_integer_float_conflict_becomes_float() {
        assert_eq!(
            field_type("{\"a\":1}\n{\"a\":1.5}\n", "a"),
            TableDataType::Number(NumberDataType::Float64).wrap_nullable()
        );
        assert_eq!(
            field_type("{\"a\":1.5}\n{\"a\":1}\n", "a"),
            TableDataType::Number(NumberDataType::Float64).wrap_nullable()
        );
    }

    #[test]
    fn test_null_does_not_force_scalar_to_string() {
        assert_eq!(
            field_type("{\"a\":null}\n{\"a\":1}\n", "a"),
            TableDataType::Number(NumberDataType::Int64).wrap_nullable()
        );
        assert_eq!(
            field_type("{\"a\":true}\n{\"a\":null}\n", "a"),
            TableDataType::Boolean.wrap_nullable()
        );
    }

    #[test]
    fn test_null_does_not_force_structured_to_variant() {
        assert_eq!(
            field_type("{\"a\":null}\n{\"a\":[1]}\n", "a"),
            TableDataType::Array(Box::new(
                TableDataType::Number(NumberDataType::Int64).wrap_nullable()
            ))
            .wrap_nullable()
        );
        assert_eq!(
            field_type("{\"a\":{\"b\":1}}\n{\"a\":null}\n", "a"),
            TableDataType::Tuple {
                fields_name: vec!["b".to_string()],
                fields_type: vec![TableDataType::Number(NumberDataType::Int64).wrap_nullable()],
            }
            .wrap_nullable()
        );
    }

    #[test]
    fn test_structured_conflicts_become_variant() {
        assert_eq!(
            field_type("{\"a\":1}\n{\"a\":[1]}\n", "a"),
            TableDataType::Variant.wrap_nullable()
        );
        assert_eq!(
            field_type("{\"a\":{\"b\":1}}\n{\"a\":[1]}\n", "a"),
            TableDataType::Variant.wrap_nullable()
        );
    }

    #[test]
    fn test_object_merge_without_depth_limit() {
        let schema =
            infer_ndjson_schema(b"{\"a\":{\"b\":1}}\n{\"a\":{\"c\":\"text\"}}\n", None, None)
                .into_table_schema();

        assert_eq!(
            schema.field_with_name("a").unwrap().data_type(),
            &TableDataType::Tuple {
                fields_name: vec!["b".to_string(), "c".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int64).wrap_nullable(),
                    TableDataType::String.wrap_nullable(),
                ],
            }
            .wrap_nullable()
        );
    }

    #[test]
    fn test_state_merge_matches_single_file_object_merge() {
        let mut merged = infer_ndjson_schema_state(b"{\"a\":{\"x\":1}}\n", None, None).unwrap();
        merged.merge(infer_ndjson_schema_state(b"{\"a\":{\"y\":2}}\n", None, None).unwrap());

        let single = infer_ndjson_schema(b"{\"a\":{\"x\":1}}\n{\"a\":{\"y\":2}}\n", None, None)
            .into_table_schema();
        assert_eq!(merged.into_table_schema(), single);
    }

    #[test]
    fn test_state_merge_matches_single_file_array_merge() {
        let mut merged = infer_ndjson_schema_state(b"{\"a\":[1]}\n", None, None).unwrap();
        merged.merge(infer_ndjson_schema_state(b"{\"a\":[\"x\"]}\n", None, None).unwrap());

        let single =
            infer_ndjson_schema(b"{\"a\":[1]}\n{\"a\":[\"x\"]}\n", None, None).into_table_schema();
        assert_eq!(merged.into_table_schema(), single);
        assert_eq!(
            single.field_with_name("a").unwrap().data_type(),
            &TableDataType::Array(Box::new(TableDataType::String.wrap_nullable())).wrap_nullable()
        );
    }

    #[test]
    fn test_empty_array_materializes_as_array_variant() {
        assert_eq!(
            field_type("{\"a\":[]}\n", "a"),
            TableDataType::Array(Box::new(TableDataType::Variant.wrap_nullable())).wrap_nullable()
        );
    }

    #[test]
    fn test_empty_array_can_still_refine_from_later_samples() {
        assert_eq!(
            field_type("{\"a\":[]}\n{\"a\":[1]}\n", "a"),
            TableDataType::Array(Box::new(
                TableDataType::Number(NumberDataType::Int64).wrap_nullable()
            ))
            .wrap_nullable()
        );
    }

    #[test]
    fn test_empty_object_materializes_as_variant() {
        assert_eq!(
            field_type("{\"a\":{}}\n", "a"),
            TableDataType::Variant.wrap_nullable()
        );
    }

    #[test]
    fn test_empty_object_can_still_refine_from_later_samples() {
        assert_eq!(
            field_type("{\"a\":{}}\n{\"a\":{\"b\":1}}\n", "a"),
            TableDataType::Tuple {
                fields_name: vec!["b".to_string()],
                fields_type: vec![TableDataType::Number(NumberDataType::Int64).wrap_nullable()],
            }
            .wrap_nullable()
        );
    }

    #[test]
    fn test_state_merge_scalar_array_conflict_becomes_variant() {
        let mut merged = infer_ndjson_schema_state(b"{\"a\":1}\n", None, None).unwrap();
        merged.merge(infer_ndjson_schema_state(b"{\"a\":[1]}\n", None, None).unwrap());

        assert_eq!(
            merged
                .into_table_schema()
                .field_with_name("a")
                .unwrap()
                .data_type(),
            &TableDataType::Variant.wrap_nullable()
        );
    }

    #[test]
    fn test_depth_limit_uses_variant_for_structured_values() {
        let schema = infer_ndjson_schema(
            b"{\"a\":1,\"b\":{\"c\":2},\"c\":[{\"d\":3}]}\n",
            None,
            Some(1),
        )
        .into_table_schema();

        assert_eq!(
            schema.field_with_name("a").unwrap().data_type(),
            &TableDataType::Number(NumberDataType::Int64).wrap_nullable()
        );
        assert_eq!(
            schema.field_with_name("b").unwrap().data_type(),
            &TableDataType::Variant.wrap_nullable()
        );
        assert_eq!(
            schema.field_with_name("c").unwrap().data_type(),
            &TableDataType::Variant.wrap_nullable()
        );
    }

    #[test]
    fn test_max_records_limits_rows() {
        let schema = infer_ndjson_schema(b"{\"a\":1}\n{\"a\":\"text\"}\n", Some(1), None)
            .into_table_schema();
        assert_eq!(
            schema.field_with_name("a").unwrap().data_type(),
            &TableDataType::Number(NumberDataType::Int64).wrap_nullable()
        );
    }
}
