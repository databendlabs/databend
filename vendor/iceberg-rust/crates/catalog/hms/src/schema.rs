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

use hive_metastore::FieldSchema;
use iceberg::spec::{PrimitiveType, Schema, SchemaVisitor, visit_schema};
use iceberg::{Error, ErrorKind, Result};

type HiveSchema = Vec<FieldSchema>;

#[derive(Debug, Default)]
pub(crate) struct HiveSchemaBuilder {
    schema: HiveSchema,
    depth: usize,
}

impl HiveSchemaBuilder {
    /// Creates a new `HiveSchemaBuilder` from iceberg `Schema`
    pub fn from_iceberg(schema: &Schema) -> Result<HiveSchemaBuilder> {
        let mut builder = Self::default();
        visit_schema(schema, &mut builder)?;
        Ok(builder)
    }

    /// Returns the newly converted `HiveSchema`
    pub fn build(self) -> HiveSchema {
        self.schema
    }

    /// Check if is in `StructType` while traversing schema
    fn is_inside_struct(&self) -> bool {
        self.depth > 0
    }
}

impl SchemaVisitor for HiveSchemaBuilder {
    type T = String;

    fn schema(
        &mut self,
        _schema: &iceberg::spec::Schema,
        value: String,
    ) -> iceberg::Result<String> {
        Ok(value)
    }

    fn before_struct_field(
        &mut self,
        _field: &iceberg::spec::NestedFieldRef,
    ) -> iceberg::Result<()> {
        self.depth += 1;
        Ok(())
    }

    fn r#struct(
        &mut self,
        r#_struct: &iceberg::spec::StructType,
        results: Vec<String>,
    ) -> iceberg::Result<String> {
        Ok(format!("struct<{}>", results.join(", ")))
    }

    fn after_struct_field(
        &mut self,
        _field: &iceberg::spec::NestedFieldRef,
    ) -> iceberg::Result<()> {
        self.depth -= 1;
        Ok(())
    }

    fn field(
        &mut self,
        field: &iceberg::spec::NestedFieldRef,
        value: String,
    ) -> iceberg::Result<String> {
        if self.is_inside_struct() {
            return Ok(format!("{}:{}", field.name, value));
        }

        self.schema.push(FieldSchema {
            name: Some(field.name.clone().into()),
            r#type: Some(value.clone().into()),
            comment: field.doc.clone().map(|doc| doc.into()),
        });

        Ok(value)
    }

    fn list(&mut self, _list: &iceberg::spec::ListType, value: String) -> iceberg::Result<String> {
        Ok(format!("array<{value}>"))
    }

    fn map(
        &mut self,
        _map: &iceberg::spec::MapType,
        key_value: String,
        value: String,
    ) -> iceberg::Result<String> {
        Ok(format!("map<{key_value},{value}>"))
    }

    fn primitive(&mut self, p: &iceberg::spec::PrimitiveType) -> iceberg::Result<String> {
        let hive_type = match p {
            PrimitiveType::Boolean => "boolean".to_string(),
            PrimitiveType::Int => "int".to_string(),
            PrimitiveType::Long => "bigint".to_string(),
            PrimitiveType::Float => "float".to_string(),
            PrimitiveType::Double => "double".to_string(),
            PrimitiveType::Date => "date".to_string(),
            PrimitiveType::Timestamp => "timestamp".to_string(),
            PrimitiveType::TimestampNs => "timestamp_ns".to_string(),
            PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Conversion from {p:?} is not supported"),
                ));
            }
            PrimitiveType::Time | PrimitiveType::String | PrimitiveType::Uuid => {
                "string".to_string()
            }
            PrimitiveType::Binary | PrimitiveType::Fixed(_) => "binary".to_string(),
            PrimitiveType::Decimal { precision, scale } => {
                format!("decimal({precision},{scale})")
            }
        };

        Ok(hive_type)
    }
}

#[cfg(test)]
mod tests {
    use iceberg::Result;
    use iceberg::spec::Schema;

    use super::*;

    #[test]
    fn test_schema_with_nested_maps() -> Result<()> {
        let record = r#"
            {
                "schema-id": 1,
                "type": "struct",
                "fields": [
                    {
                        "id": 1,
                        "name": "quux",
                        "required": true,
                        "type": {
                            "type": "map",
                            "key-id": 2,
                            "key": "string",
                            "value-id": 3,
                            "value-required": true,
                            "value": {
                                "type": "map",
                                "key-id": 4,
                                "key": "string",
                                "value-id": 5,
                                "value-required": true,
                                "value": "int"
                            }
                        }
                    }
                ]
            }
        "#;

        let schema = serde_json::from_str::<Schema>(record)?;

        let result = HiveSchemaBuilder::from_iceberg(&schema)?.build();

        let expected = vec![FieldSchema {
            name: Some("quux".into()),
            r#type: Some("map<string,map<string,int>>".into()),
            comment: None,
        }];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_schema_with_struct_inside_list() -> Result<()> {
        let record = r#"
        {
            "schema-id": 1,
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "location",
                    "required": true,
                    "type": {
                        "type": "list",
                        "element-id": 2,
                        "element-required": true,
                        "element": {
                            "type": "struct",
                            "fields": [
                                {
                                    "id": 3,
                                    "name": "latitude",
                                    "required": false,
                                    "type": "float"
                                },
                                {
                                    "id": 4,
                                    "name": "longitude",
                                    "required": false,
                                    "type": "float"
                                }
                            ]
                        }
                    }
                }
            ]
        }
        "#;

        let schema = serde_json::from_str::<Schema>(record)?;

        let result = HiveSchemaBuilder::from_iceberg(&schema)?.build();

        let expected = vec![FieldSchema {
            name: Some("location".into()),
            r#type: Some("array<struct<latitude:float, longitude:float>>".into()),
            comment: None,
        }];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_schema_with_structs() -> Result<()> {
        let record = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                {
                    "id": 1,
                    "name": "person",
                    "required": true,
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "id": 2,
                                "name": "name",
                                "required": true,
                                "type": "string"
                            },
                            {
                                "id": 3,
                                "name": "age",
                                "required": false,
                                "type": "int"
                            }
                        ]
                    }
                }
            ]
        }"#;

        let schema = serde_json::from_str::<Schema>(record)?;

        let result = HiveSchemaBuilder::from_iceberg(&schema)?.build();

        let expected = vec![FieldSchema {
            name: Some("person".into()),
            r#type: Some("struct<name:string, age:int>".into()),
            comment: None,
        }];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_schema_with_simple_fields() -> Result<()> {
        let record = r#"{
            "type": "struct",
            "schema-id": 1,
            "fields": [
                {
                    "id": 1,
                    "name": "c1",
                    "required": true,
                    "type": "boolean"
                },
                {
                    "id": 2,
                    "name": "c2",
                    "required": true,
                    "type": "int"
                },
                {
                    "id": 3,
                    "name": "c3",
                    "required": true,
                    "type": "long"
                },
                {
                    "id": 4,
                    "name": "c4",
                    "required": true,
                    "type": "float"
                },
                {
                    "id": 5,
                    "name": "c5",
                    "required": true,
                    "type": "double"
                },
                {
                    "id": 6,
                    "name": "c6",
                    "required": true,
                    "type": "decimal(2,2)"
                },
                {
                    "id": 7,
                    "name": "c7",
                    "required": true,
                    "type": "date"
                },
                {
                    "id": 8,
                    "name": "c8",
                    "required": true,
                    "type": "time"
                },
                {
                    "id": 9,
                    "name": "c9",
                    "required": true,
                    "type": "timestamp"
                },
                {
                    "id": 10,
                    "name": "c10",
                    "required": true,
                    "type": "string"
                },
                {
                    "id": 11,
                    "name": "c11",
                    "required": true,
                    "type": "uuid"
                },
                {
                    "id": 12,
                    "name": "c12",
                    "required": true,
                    "type": "fixed[4]"
                },
                {
                    "id": 13,
                    "name": "c13",
                    "required": true,
                    "type": "binary"
                }
            ]
        }"#;

        let schema = serde_json::from_str::<Schema>(record)?;

        let result = HiveSchemaBuilder::from_iceberg(&schema)?.build();

        let expected = vec![
            FieldSchema {
                name: Some("c1".into()),
                r#type: Some("boolean".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c2".into()),
                r#type: Some("int".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c3".into()),
                r#type: Some("bigint".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c4".into()),
                r#type: Some("float".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c5".into()),
                r#type: Some("double".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c6".into()),
                r#type: Some("decimal(2,2)".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c7".into()),
                r#type: Some("date".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c8".into()),
                r#type: Some("string".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c9".into()),
                r#type: Some("timestamp".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c10".into()),
                r#type: Some("string".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c11".into()),
                r#type: Some("string".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c12".into()),
                r#type: Some("binary".into()),
                comment: None,
            },
            FieldSchema {
                name: Some("c13".into()),
                r#type: Some("binary".into()),
                comment: None,
            },
        ];

        assert_eq!(result, expected);

        Ok(())
    }
}
