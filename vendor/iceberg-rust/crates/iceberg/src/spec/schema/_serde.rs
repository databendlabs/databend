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

/// This is a helper module that defines types to help with serialization/deserialization.
/// For deserialization the input first gets read into either the [SchemaV1] or [SchemaV2] struct
/// and then converted into the [Schema] struct. Serialization works the other way around.
/// [SchemaV1] and [SchemaV2] are internal struct that are only used for serialization and deserialization.
use serde::Deserialize;
/// This is a helper module that defines types to help with serialization/deserialization.
/// For deserialization the input first gets read into either the [SchemaV1] or [SchemaV2] struct
/// and then converted into the [Schema] struct. Serialization works the other way around.
/// [SchemaV1] and [SchemaV2] are internal struct that are only used for serialization and deserialization.
use serde::Serialize;

use super::{DEFAULT_SCHEMA_ID, Schema};
use crate::spec::StructType;
use crate::{Error, Result};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
/// Enum for Schema serialization/deserializaion
pub(super) enum SchemaEnum {
    V2(SchemaV2),
    V1(SchemaV1),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Defines the structure of a v2 schema for serialization/deserialization
pub(crate) struct SchemaV2 {
    pub schema_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
    #[serde(flatten)]
    pub fields: StructType,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Defines the structure of a v1 schema for serialization/deserialization
pub(crate) struct SchemaV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_field_ids: Option<Vec<i32>>,
    #[serde(flatten)]
    pub fields: StructType,
}

/// Helper to serialize/deserializa Schema
impl TryFrom<SchemaEnum> for Schema {
    type Error = Error;
    fn try_from(value: SchemaEnum) -> Result<Self> {
        match value {
            SchemaEnum::V2(value) => value.try_into(),
            SchemaEnum::V1(value) => value.try_into(),
        }
    }
}

impl From<Schema> for SchemaEnum {
    fn from(value: Schema) -> Self {
        SchemaEnum::V2(value.into())
    }
}

impl TryFrom<SchemaV2> for Schema {
    type Error = Error;
    fn try_from(value: SchemaV2) -> Result<Self> {
        Schema::builder()
            .with_schema_id(value.schema_id)
            .with_fields(value.fields.fields().iter().cloned())
            .with_identifier_field_ids(value.identifier_field_ids.unwrap_or_default())
            .build()
    }
}

impl TryFrom<SchemaV1> for Schema {
    type Error = Error;
    fn try_from(value: SchemaV1) -> Result<Self> {
        Schema::builder()
            .with_schema_id(value.schema_id.unwrap_or(DEFAULT_SCHEMA_ID))
            .with_fields(value.fields.fields().iter().cloned())
            .with_identifier_field_ids(value.identifier_field_ids.unwrap_or_default())
            .build()
    }
}

impl From<Schema> for SchemaV2 {
    fn from(value: Schema) -> Self {
        SchemaV2 {
            schema_id: value.schema_id,
            identifier_field_ids: if value.identifier_field_ids.is_empty() {
                None
            } else {
                Some(value.identifier_field_ids.into_iter().collect())
            },
            fields: value.r#struct,
        }
    }
}

impl From<Schema> for SchemaV1 {
    fn from(value: Schema) -> Self {
        SchemaV1 {
            schema_id: Some(value.schema_id),
            identifier_field_ids: if value.identifier_field_ids.is_empty() {
                None
            } else {
                Some(value.identifier_field_ids.into_iter().collect())
            },
            fields: value.r#struct,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::schema::tests::table_schema_simple;
    use crate::spec::{PrimitiveType, Type};

    fn check_schema_serde(json: &str, expected_type: Schema, _expected_enum: SchemaEnum) {
        let desered_type: Schema = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);
        assert!(matches!(desered_type.clone(), _expected_enum));

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<Schema>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, desered_type);
    }

    #[test]
    fn test_serde_with_schema_id() {
        let (schema, record) = table_schema_simple();

        let x: SchemaV2 = serde_json::from_str(record).unwrap();
        check_schema_serde(record, schema, SchemaEnum::V2(x));
    }

    #[test]
    fn test_serde_without_schema_id() {
        let (mut schema, record) = table_schema_simple();
        // we remove the ""schema-id": 1," string from example
        let new_record = record.replace("\"schema-id\":1,", "");
        // By default schema_id field is set to DEFAULT_SCHEMA_ID when no value is set in json
        schema.schema_id = DEFAULT_SCHEMA_ID;

        let x: SchemaV1 = serde_json::from_str(new_record.as_str()).unwrap();
        check_schema_serde(&new_record, schema, SchemaEnum::V1(x));
    }

    #[test]
    fn schema() {
        let record = r#"
        {
            "type": "struct",
            "schema-id": 1,
            "fields": [ {
            "id": 1,
            "name": "id",
            "required": true,
            "type": "uuid"
            }, {
            "id": 2,
            "name": "data",
            "required": false,
            "type": "int"
            } ]
            }
        "#;

        let result: SchemaV2 = serde_json::from_str(record).unwrap();
        assert_eq!(1, result.schema_id);
        assert_eq!(
            Box::new(Type::Primitive(PrimitiveType::Uuid)),
            result.fields[0].field_type
        );
        assert_eq!(1, result.fields[0].id);
        assert!(result.fields[0].required);

        assert_eq!(
            Box::new(Type::Primitive(PrimitiveType::Int)),
            result.fields[1].field_type
        );
        assert_eq!(2, result.fields[1].id);
        assert!(!result.fields[1].required);
    }
}
