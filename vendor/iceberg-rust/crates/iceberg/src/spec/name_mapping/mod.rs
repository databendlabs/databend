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

//! Iceberg name mapping.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_with::{DefaultOnNull, serde_as};

/// Property name for name mapping.
pub const DEFAULT_SCHEMA_NAME_MAPPING: &str = "schema.name-mapping.default";

/// Iceberg fallback field name to ID mapping.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(transparent)]
pub struct NameMapping {
    root: Vec<MappedField>,
}

impl NameMapping {
    /// Create a new [`NameMapping`] given a collection of mapped fields.
    pub fn new(fields: Vec<MappedField>) -> Self {
        Self { root: fields }
    }

    /// Get a reference to fields which are to be mapped from name to field ID.
    pub fn fields(&self) -> &[MappedField] {
        &self.root
    }
}

/// Maps field names to IDs.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct MappedField {
    #[serde(skip_serializing_if = "Option::is_none")]
    field_id: Option<i32>,
    names: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde_as(deserialize_as = "DefaultOnNull")]
    fields: Vec<Arc<MappedField>>,
}

impl MappedField {
    /// Create a new [`MappedField`].
    pub fn new(field_id: Option<i32>, names: Vec<String>, fields: Vec<MappedField>) -> Self {
        Self {
            field_id,
            names,
            fields: fields.into_iter().map(Arc::new).collect(),
        }
    }

    /// Iceberg field ID when a field's name is present within `names`.
    pub fn field_id(&self) -> Option<i32> {
        self.field_id
    }

    /// Get a reference to names for a mapped field.
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Get a reference to the field mapping for any child fields.
    pub fn fields(&self) -> &[Arc<MappedField>] {
        &self.fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_mapped_field_deserialization() {
        let expected = MappedField {
            field_id: Some(1),
            names: vec!["id".to_string(), "record_id".to_string()],
            fields: vec![],
        };
        let mapped_field = r#"
        {
            "field-id": 1,
            "names": ["id", "record_id"]
        }
        "#;

        let mapped_field: MappedField = serde_json::from_str(mapped_field).unwrap();
        assert_eq!(mapped_field, expected);

        let mapped_field_with_null_fields = r#"
        {
            "field-id": 1,
            "names": ["id", "record_id"],
            "fields": null
        }
        "#;

        let mapped_field_with_null_fields: MappedField =
            serde_json::from_str(mapped_field_with_null_fields).unwrap();
        assert_eq!(mapped_field_with_null_fields, expected);
    }

    #[test]
    fn test_json_mapped_field_no_names_deserialization() {
        let expected = MappedField {
            field_id: Some(1),
            names: vec![],
            fields: vec![],
        };
        let mapped_field = r#"
        {
            "field-id": 1,
            "names": []
        }
        "#;

        let mapped_field: MappedField = serde_json::from_str(mapped_field).unwrap();
        assert_eq!(mapped_field, expected);

        let mapped_field_with_null_fields = r#"
        {
            "field-id": 1,
            "names": [],
            "fields": null
        }
        "#;

        let mapped_field_with_null_fields: MappedField =
            serde_json::from_str(mapped_field_with_null_fields).unwrap();
        assert_eq!(mapped_field_with_null_fields, expected);
    }

    #[test]
    fn test_json_mapped_field_no_field_id_deserialization() {
        let expected = MappedField {
            field_id: None,
            names: vec!["id".to_string(), "record_id".to_string()],
            fields: vec![],
        };
        let mapped_field = r#"
        {
            "names": ["id", "record_id"]
        }
        "#;

        let mapped_field: MappedField = serde_json::from_str(mapped_field).unwrap();
        assert_eq!(mapped_field, expected);

        let mapped_field_with_null_fields = r#"
        {
            "names": ["id", "record_id"],
            "fields": null
        }
        "#;

        let mapped_field_with_null_fields: MappedField =
            serde_json::from_str(mapped_field_with_null_fields).unwrap();
        assert_eq!(mapped_field_with_null_fields, expected);
    }

    #[test]
    fn test_json_name_mapping_deserialization() {
        let name_mapping = r#"
        [
            {
                "field-id": 1,
                "names": [
                    "id",
                    "record_id"
                ]
            },
            {
                "field-id": 2,
                "names": [
                    "data"
                ]
            },
            {
                "field-id": 3,
                "names": [
                    "location"
                ],
                "fields": [
                    {
                        "field-id": 4,
                        "names": [
                            "latitude",
                            "lat"
                        ]
                    },
                    {
                        "field-id": 5,
                        "names": [
                            "longitude",
                            "long"
                        ]
                    }
                ]
            }
        ]
        "#;

        let name_mapping: NameMapping = serde_json::from_str(name_mapping).unwrap();
        assert_eq!(name_mapping, NameMapping {
            root: vec![
                MappedField {
                    field_id: Some(1),
                    names: vec!["id".to_string(), "record_id".to_string()],
                    fields: vec![]
                },
                MappedField {
                    field_id: Some(2),
                    names: vec!["data".to_string()],
                    fields: vec![]
                },
                MappedField {
                    field_id: Some(3),
                    names: vec!["location".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(4),
                            names: vec!["latitude".to_string(), "lat".to_string()],
                            fields: vec![]
                        }
                        .into(),
                        MappedField {
                            field_id: Some(5),
                            names: vec!["longitude".to_string(), "long".to_string()],
                            fields: vec![]
                        }
                        .into(),
                    ]
                }
            ],
        });
    }

    #[test]
    fn test_json_name_mapping_serialization() {
        let name_mapping = NameMapping {
            root: vec![
                MappedField {
                    field_id: None,
                    names: vec!["foo".to_string()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(2),
                    names: vec!["bar".to_string()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(3),
                    names: vec!["baz".to_string()],
                    fields: vec![],
                },
                MappedField {
                    field_id: Some(4),
                    names: vec!["qux".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(5),
                            names: vec!["element".to_string()],
                            fields: vec![],
                        }
                        .into(),
                    ],
                },
                MappedField {
                    field_id: Some(6),
                    names: vec!["quux".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(7),
                            names: vec!["key".to_string()],
                            fields: vec![],
                        }
                        .into(),
                        MappedField {
                            field_id: Some(8),
                            names: vec!["value".to_string()],
                            fields: vec![
                                MappedField {
                                    field_id: Some(9),
                                    names: vec!["key".to_string()],
                                    fields: vec![],
                                }
                                .into(),
                                MappedField {
                                    field_id: Some(10),
                                    names: vec!["value".to_string()],
                                    fields: vec![],
                                }
                                .into(),
                            ],
                        }
                        .into(),
                    ],
                },
                MappedField {
                    field_id: Some(11),
                    names: vec!["location".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(12),
                            names: vec!["element".to_string()],
                            fields: vec![
                                MappedField {
                                    field_id: Some(13),
                                    names: vec!["latitude".to_string()],
                                    fields: vec![],
                                }
                                .into(),
                                MappedField {
                                    field_id: Some(14),
                                    names: vec!["longitude".to_string()],
                                    fields: vec![],
                                }
                                .into(),
                            ],
                        }
                        .into(),
                    ],
                },
                MappedField {
                    field_id: Some(15),
                    names: vec!["person".to_string()],
                    fields: vec![
                        MappedField {
                            field_id: Some(16),
                            names: vec!["name".to_string()],
                            fields: vec![],
                        }
                        .into(),
                        MappedField {
                            field_id: Some(17),
                            names: vec!["age".to_string()],
                            fields: vec![],
                        }
                        .into(),
                    ],
                },
            ],
        };
        let expected = r#"[{"names":["foo"]},{"field-id":2,"names":["bar"]},{"field-id":3,"names":["baz"]},{"field-id":4,"names":["qux"],"fields":[{"field-id":5,"names":["element"]}]},{"field-id":6,"names":["quux"],"fields":[{"field-id":7,"names":["key"]},{"field-id":8,"names":["value"],"fields":[{"field-id":9,"names":["key"]},{"field-id":10,"names":["value"]}]}]},{"field-id":11,"names":["location"],"fields":[{"field-id":12,"names":["element"],"fields":[{"field-id":13,"names":["latitude"]},{"field-id":14,"names":["longitude"]}]}]},{"field-id":15,"names":["person"],"fields":[{"field-id":16,"names":["name"]},{"field-id":17,"names":["age"]}]}]"#;
        assert_eq!(serde_json::to_string(&name_mapping).unwrap(), expected);
    }
}
