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

//! This module defines schema in iceberg.

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

mod utils;
mod visitor;
pub use self::visitor::*;
pub(super) mod _serde;
mod id_reassigner;
mod index;
mod prune_columns;
use bimap::BiHashMap;
use itertools::{Itertools, zip_eq};
use serde::{Deserialize, Serialize};

use self::_serde::SchemaEnum;
use self::id_reassigner::ReassignFieldIds;
use self::index::{IndexByName, index_by_id, index_parents};
pub use self::prune_columns::prune_columns;
use super::NestedField;
use crate::error::Result;
use crate::expr::accessor::StructAccessor;
use crate::spec::datatypes::{
    LIST_FIELD_NAME, ListType, MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME, MapType, NestedFieldRef,
    PrimitiveType, StructType, Type,
};
use crate::{Error, ErrorKind, ensure_data_valid};

/// Type alias for schema id.
pub type SchemaId = i32;
/// Reference to [`Schema`].
pub type SchemaRef = Arc<Schema>;
/// Default schema id.
pub const DEFAULT_SCHEMA_ID: SchemaId = 0;

/// Defines schema in iceberg.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(try_from = "SchemaEnum", into = "SchemaEnum")]
pub struct Schema {
    r#struct: StructType,
    schema_id: SchemaId,
    highest_field_id: i32,
    identifier_field_ids: HashSet<i32>,

    alias_to_id: BiHashMap<String, i32>,
    id_to_field: HashMap<i32, NestedFieldRef>,

    name_to_id: HashMap<String, i32>,
    lowercase_name_to_id: HashMap<String, i32>,
    id_to_name: HashMap<i32, String>,

    field_id_to_accessor: HashMap<i32, Arc<StructAccessor>>,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.r#struct == other.r#struct
            && self.schema_id == other.schema_id
            && self.identifier_field_ids == other.identifier_field_ids
    }
}

impl Eq for Schema {}

/// Schema builder.
#[derive(Debug)]
pub struct SchemaBuilder {
    schema_id: i32,
    fields: Vec<NestedFieldRef>,
    alias_to_id: BiHashMap<String, i32>,
    identifier_field_ids: HashSet<i32>,
    reassign_field_ids_from: Option<i32>,
}

impl SchemaBuilder {
    /// Add fields to schema builder.
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = NestedFieldRef>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// Reassign all field-ids (including nested) on build.
    /// Reassignment starts from the field-id specified in `start_from` (inclusive).
    ///
    /// All specified aliases and identifier fields will be updated to the new field-ids.
    pub(crate) fn with_reassigned_field_ids(mut self, start_from: i32) -> Self {
        self.reassign_field_ids_from = Some(start_from);
        self
    }

    /// Set schema id.
    pub fn with_schema_id(mut self, schema_id: i32) -> Self {
        self.schema_id = schema_id;
        self
    }

    /// Set identifier field ids.
    pub fn with_identifier_field_ids(mut self, ids: impl IntoIterator<Item = i32>) -> Self {
        self.identifier_field_ids.extend(ids);
        self
    }

    /// Set alias to filed id mapping.
    pub fn with_alias(mut self, alias_to_id: BiHashMap<String, i32>) -> Self {
        self.alias_to_id = alias_to_id;
        self
    }

    /// Builds the schema.
    pub fn build(self) -> Result<Schema> {
        let field_id_to_accessor = self.build_accessors();

        let r#struct = StructType::new(self.fields);
        let id_to_field = index_by_id(&r#struct)?;

        Self::validate_identifier_ids(
            &r#struct,
            &id_to_field,
            self.identifier_field_ids.iter().copied(),
        )?;

        let (name_to_id, id_to_name) = {
            let mut index = IndexByName::default();
            visit_struct(&r#struct, &mut index)?;
            index.indexes()
        };

        let lowercase_name_to_id = name_to_id
            .iter()
            .map(|(k, v)| (k.to_lowercase(), *v))
            .collect();

        let highest_field_id = id_to_field.keys().max().cloned().unwrap_or(0);

        let mut schema = Schema {
            r#struct,
            schema_id: self.schema_id,
            highest_field_id,
            identifier_field_ids: self.identifier_field_ids,
            alias_to_id: self.alias_to_id,
            id_to_field,

            name_to_id,
            lowercase_name_to_id,
            id_to_name,

            field_id_to_accessor,
        };

        if let Some(start_from) = self.reassign_field_ids_from {
            let mut id_reassigner = ReassignFieldIds::new(start_from);
            let new_fields = id_reassigner.reassign_field_ids(schema.r#struct.fields().to_vec())?;
            let new_identifier_field_ids =
                id_reassigner.apply_to_identifier_fields(schema.identifier_field_ids)?;
            let new_alias_to_id = id_reassigner.apply_to_aliases(schema.alias_to_id.clone())?;

            schema = Schema::builder()
                .with_schema_id(schema.schema_id)
                .with_fields(new_fields)
                .with_identifier_field_ids(new_identifier_field_ids)
                .with_alias(new_alias_to_id)
                .build()?;
        }

        Ok(schema)
    }

    fn build_accessors(&self) -> HashMap<i32, Arc<StructAccessor>> {
        let mut map = HashMap::new();

        for (pos, field) in self.fields.iter().enumerate() {
            match field.field_type.as_ref() {
                Type::Primitive(prim_type) => {
                    // add an accessor for this field
                    let accessor = Arc::new(StructAccessor::new(pos, prim_type.clone()));
                    map.insert(field.id, accessor.clone());
                }

                Type::Struct(nested) => {
                    // add accessors for nested fields
                    for (field_id, accessor) in Self::build_accessors_nested(nested.fields()) {
                        let new_accessor = Arc::new(StructAccessor::wrap(pos, accessor));
                        map.insert(field_id, new_accessor.clone());
                    }
                }
                _ => {
                    // Accessors don't get built for Map or List types
                }
            }
        }

        map
    }

    fn build_accessors_nested(fields: &[NestedFieldRef]) -> Vec<(i32, Box<StructAccessor>)> {
        let mut results = vec![];
        for (pos, field) in fields.iter().enumerate() {
            match field.field_type.as_ref() {
                Type::Primitive(prim_type) => {
                    let accessor = Box::new(StructAccessor::new(pos, prim_type.clone()));
                    results.push((field.id, accessor));
                }
                Type::Struct(nested) => {
                    let nested_accessors = Self::build_accessors_nested(nested.fields());

                    let wrapped_nested_accessors =
                        nested_accessors.into_iter().map(|(id, accessor)| {
                            let new_accessor = Box::new(StructAccessor::wrap(pos, accessor));
                            (id, new_accessor.clone())
                        });

                    results.extend(wrapped_nested_accessors);
                }
                _ => {
                    // Accessors don't get built for Map or List types
                }
            }
        }

        results
    }

    /// According to [the spec](https://iceberg.apache.org/spec/#identifier-fields), the identifier fields
    /// must meet the following requirements:
    /// - Float, double, and optional fields cannot be used as identifier fields.
    /// - Identifier fields may be nested in structs but cannot be nested within maps or lists.
    /// - A nested field cannot be used as an identifier field if it is nested in an optional struct, to avoid null values in identifiers.
    fn validate_identifier_ids(
        r#struct: &StructType,
        id_to_field: &HashMap<i32, NestedFieldRef>,
        identifier_field_ids: impl Iterator<Item = i32>,
    ) -> Result<()> {
        let id_to_parent = index_parents(r#struct)?;
        for identifier_field_id in identifier_field_ids {
            let field = id_to_field.get(&identifier_field_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add identifier field {identifier_field_id}: field does not exist"
                    ),
                )
            })?;
            ensure_data_valid!(
                field.required,
                "Cannot add identifier field: {} is an optional field",
                field.name
            );
            if let Type::Primitive(p) = field.field_type.as_ref() {
                ensure_data_valid!(
                    !matches!(p, PrimitiveType::Double | PrimitiveType::Float),
                    "Cannot add identifier field {}: cannot be a float or double type",
                    field.name
                );
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot add field {} as an identifier field: not a primitive type field",
                        field.name
                    ),
                ));
            }

            let mut cur_field_id = identifier_field_id;
            while let Some(parent) = id_to_parent.get(&cur_field_id) {
                let parent_field = id_to_field
                    .get(parent)
                    .expect("Field id should not disappear.");
                ensure_data_valid!(
                    parent_field.field_type.is_struct(),
                    "Cannot add field {} as an identifier field: must not be nested in {:?}",
                    field.name,
                    parent_field
                );
                ensure_data_valid!(
                    parent_field.required,
                    "Cannot add field {} as an identifier field: must not be nested in an optional field {}",
                    field.name,
                    parent_field
                );
                cur_field_id = *parent;
            }
        }

        Ok(())
    }
}

impl Schema {
    /// Create a schema builder.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder {
            schema_id: DEFAULT_SCHEMA_ID,
            fields: vec![],
            identifier_field_ids: HashSet::default(),
            alias_to_id: BiHashMap::default(),
            reassign_field_ids_from: None,
        }
    }

    /// Create a new schema builder from a schema.
    pub fn into_builder(self) -> SchemaBuilder {
        SchemaBuilder {
            schema_id: self.schema_id,
            fields: self.r#struct.fields().to_vec(),
            alias_to_id: self.alias_to_id,
            identifier_field_ids: self.identifier_field_ids,
            reassign_field_ids_from: None,
        }
    }

    /// Get field by field id.
    pub fn field_by_id(&self, field_id: i32) -> Option<&NestedFieldRef> {
        self.id_to_field.get(&field_id)
    }

    /// Get field by field name.
    ///
    /// Both full name and short name could work here.
    pub fn field_by_name(&self, field_name: &str) -> Option<&NestedFieldRef> {
        self.name_to_id
            .get(field_name)
            .and_then(|id| self.field_by_id(*id))
    }

    /// Get field by field name, but in case-insensitive way.
    ///
    /// Both full name and short name could work here.
    pub fn field_by_name_case_insensitive(&self, field_name: &str) -> Option<&NestedFieldRef> {
        self.lowercase_name_to_id
            .get(&field_name.to_lowercase())
            .and_then(|id| self.field_by_id(*id))
    }

    /// Get field by alias.
    pub fn field_by_alias(&self, alias: &str) -> Option<&NestedFieldRef> {
        self.alias_to_id
            .get_by_left(alias)
            .and_then(|id| self.field_by_id(*id))
    }

    /// Returns [`highest_field_id`].
    #[inline]
    pub fn highest_field_id(&self) -> i32 {
        self.highest_field_id
    }

    /// Returns [`schema_id`].
    #[inline]
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Returns [`r#struct`].
    #[inline]
    pub fn as_struct(&self) -> &StructType {
        &self.r#struct
    }

    /// Returns [`identifier_field_ids`].
    #[inline]
    pub fn identifier_field_ids(&self) -> impl ExactSizeIterator<Item = i32> + '_ {
        self.identifier_field_ids.iter().copied()
    }

    /// Get field id by full name.
    pub fn field_id_by_name(&self, name: &str) -> Option<i32> {
        self.name_to_id.get(name).copied()
    }

    /// Get full name by field id.
    pub fn name_by_field_id(&self, field_id: i32) -> Option<&str> {
        self.id_to_name.get(&field_id).map(String::as_str)
    }

    /// Get an accessor for retrieving data in a struct
    pub fn accessor_by_field_id(&self, field_id: i32) -> Option<Arc<StructAccessor>> {
        self.field_id_to_accessor.get(&field_id).cloned()
    }

    /// Check if this schema is identical to another schema semantically - excluding schema id.
    pub(crate) fn is_same_schema(&self, other: &SchemaRef) -> bool {
        self.as_struct().eq(other.as_struct())
            && self.identifier_field_ids().eq(other.identifier_field_ids())
    }

    /// Change the schema id of this schema.
    // This is redundant with the `with_schema_id` method on the builder, but useful
    // as it is infallible in contrast to the builder `build()` method.
    pub(crate) fn with_schema_id(self, schema_id: SchemaId) -> Self {
        Self { schema_id, ..self }
    }

    /// Return A HashMap matching field ids to field names.
    pub fn field_id_to_name_map(&self) -> &HashMap<i32, String> {
        &self.id_to_name
    }

    /// Return a hashmap matching field ids to nested fields.
    pub fn field_id_to_fields(&self) -> &HashMap<i32, NestedFieldRef> {
        &self.id_to_field
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "table {{")?;
        for field in self.as_struct().fields() {
            writeln!(f, "  {field}")?;
        }
        writeln!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bimap::BiHashMap;

    use crate::spec::datatypes::Type::{List, Map, Primitive, Struct};
    use crate::spec::datatypes::{
        ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, StructType, Type,
    };
    use crate::spec::schema::Schema;
    use crate::spec::values::Map as MapValue;
    use crate::spec::{Datum, Literal};

    #[test]
    fn test_construct_schema() {
        let field1: NestedFieldRef =
            NestedField::required(1, "f1", Type::Primitive(PrimitiveType::Boolean)).into();
        let field2: NestedFieldRef =
            NestedField::optional(2, "f2", Type::Primitive(PrimitiveType::Int)).into();

        let schema = Schema::builder()
            .with_fields(vec![field1.clone()])
            .with_fields(vec![field2.clone()])
            .with_schema_id(3)
            .build()
            .unwrap();

        assert_eq!(3, schema.schema_id());
        assert_eq!(2, schema.highest_field_id());
        assert_eq!(Some(&field1), schema.field_by_id(1));
        assert_eq!(Some(&field2), schema.field_by_id(2));
        assert_eq!(None, schema.field_by_id(3));
    }

    pub fn table_schema_simple<'a>() -> (Schema, &'a str) {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();
        let record = r#"{
            "type":"struct",
            "schema-id":1,
            "fields":[
                {
                    "id":1,
                    "name":"foo",
                    "required":false,
                    "type":"string"
                },
                {
                    "id":2,
                    "name":"bar",
                    "required":true,
                    "type":"int"
                },
                {
                    "id":3,
                    "name":"baz",
                    "required":false,
                    "type":"boolean"
                }
            ],
            "identifier-field-ids":[2]
        }"#;
        (schema, record)
    }

    pub fn table_schema_nested() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::required(
                    4,
                    "qux",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            5,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
                NestedField::required(
                    6,
                    "quux",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            7,
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            8,
                            Type::Map(MapType {
                                key_field: NestedField::map_key_element(
                                    9,
                                    Type::Primitive(PrimitiveType::String),
                                )
                                .into(),
                                value_field: NestedField::map_value_element(
                                    10,
                                    Type::Primitive(PrimitiveType::Int),
                                    true,
                                )
                                .into(),
                            }),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
                NestedField::required(
                    11,
                    "location",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            12,
                            Type::Struct(StructType::new(vec![
                                NestedField::optional(
                                    13,
                                    "latitude",
                                    Type::Primitive(PrimitiveType::Float),
                                )
                                .into(),
                                NestedField::optional(
                                    14,
                                    "longitude",
                                    Type::Primitive(PrimitiveType::Float),
                                )
                                .into(),
                            ])),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
                NestedField::optional(
                    15,
                    "person",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(16, "name", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(17, "age", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_schema_display() {
        let expected_str = "
table {
  1: foo: optional string\x20
  2: bar: required int\x20
  3: baz: optional boolean\x20
}
";

        assert_eq!(expected_str, format!("\n{}", table_schema_simple().0));
    }

    #[test]
    fn test_schema_build_failed_on_duplicate_names() {
        let ret = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "foo", Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Primitive(PrimitiveType::Boolean)).into(),
                NestedField::optional(4, "baz", Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build();

        assert!(
            ret.unwrap_err()
                .message()
                .contains("Invalid schema: multiple fields for name baz")
        );
    }

    #[test]
    fn test_schema_into_builder() {
        let original_schema = table_schema_nested();
        let builder = original_schema.clone().into_builder();
        let schema = builder.build().unwrap();

        assert_eq!(original_schema, schema);
    }

    #[test]
    fn test_schema_index_by_name() {
        let expected_name_to_id = HashMap::from(
            [
                ("foo", 1),
                ("bar", 2),
                ("baz", 3),
                ("qux", 4),
                ("qux.element", 5),
                ("quux", 6),
                ("quux.key", 7),
                ("quux.value", 8),
                ("quux.value.key", 9),
                ("quux.value.value", 10),
                ("location", 11),
                ("location.element", 12),
                ("location.element.latitude", 13),
                ("location.element.longitude", 14),
                ("location.latitude", 13),
                ("location.longitude", 14),
                ("person", 15),
                ("person.name", 16),
                ("person.age", 17),
            ]
            .map(|e| (e.0.to_string(), e.1)),
        );

        let schema = table_schema_nested();
        assert_eq!(&expected_name_to_id, &schema.name_to_id);
    }

    #[test]
    fn test_schema_index_by_name_case_insensitive() {
        let expected_name_to_id = HashMap::from(
            [
                ("fOo", 1),
                ("Bar", 2),
                ("BAz", 3),
                ("quX", 4),
                ("quX.ELEment", 5),
                ("qUUx", 6),
                ("QUUX.KEY", 7),
                ("QUUX.Value", 8),
                ("qUUX.VALUE.Key", 9),
                ("qUux.VaLue.Value", 10),
                ("lOCAtION", 11),
                ("LOCAtioN.ELeMENt", 12),
                ("LoCATion.element.LATitude", 13),
                ("locatION.ElemeNT.LONgitude", 14),
                ("LOCAtiON.LATITUDE", 13),
                ("LOCATION.LONGITUDE", 14),
                ("PERSon", 15),
                ("PERSON.Name", 16),
                ("peRSON.AGe", 17),
            ]
            .map(|e| (e.0.to_string(), e.1)),
        );

        let schema = table_schema_nested();
        for (name, id) in expected_name_to_id {
            assert_eq!(
                Some(id),
                schema.field_by_name_case_insensitive(&name).map(|f| f.id)
            );
        }
    }

    #[test]
    fn test_schema_find_column_name() {
        let expected_column_name = HashMap::from([
            (1, "foo"),
            (2, "bar"),
            (3, "baz"),
            (4, "qux"),
            (5, "qux.element"),
            (6, "quux"),
            (7, "quux.key"),
            (8, "quux.value"),
            (9, "quux.value.key"),
            (10, "quux.value.value"),
            (11, "location"),
            (12, "location.element"),
            (13, "location.element.latitude"),
            (14, "location.element.longitude"),
        ]);

        let schema = table_schema_nested();
        for (id, name) in expected_column_name {
            assert_eq!(
                Some(name),
                schema.name_by_field_id(id),
                "Column name for field id {id} not match."
            );
        }
    }

    #[test]
    fn test_schema_find_column_name_not_found() {
        let schema = table_schema_nested();

        assert!(schema.name_by_field_id(99).is_none());
    }

    #[test]
    fn test_schema_find_column_name_by_id_simple() {
        let expected_id_to_name = HashMap::from([(1, "foo"), (2, "bar"), (3, "baz")]);

        let schema = table_schema_simple().0;

        for (id, name) in expected_id_to_name {
            assert_eq!(
                Some(name),
                schema.name_by_field_id(id),
                "Column name for field id {id} not match."
            );
        }
    }

    #[test]
    fn test_schema_find_simple() {
        let schema = table_schema_simple().0;

        assert_eq!(
            Some(schema.r#struct.fields()[0].clone()),
            schema.field_by_id(1).cloned()
        );
        assert_eq!(
            Some(schema.r#struct.fields()[1].clone()),
            schema.field_by_id(2).cloned()
        );
        assert_eq!(
            Some(schema.r#struct.fields()[2].clone()),
            schema.field_by_id(3).cloned()
        );

        assert!(schema.field_by_id(4).is_none());
        assert!(schema.field_by_name("non exist").is_none());
    }

    #[test]
    fn test_schema_find_nested() {
        let expected_id_to_field: HashMap<i32, NestedField> = HashMap::from([
            (
                1,
                NestedField::optional(1, "foo", Primitive(PrimitiveType::String)),
            ),
            (
                2,
                NestedField::required(2, "bar", Primitive(PrimitiveType::Int)),
            ),
            (
                3,
                NestedField::optional(3, "baz", Primitive(PrimitiveType::Boolean)),
            ),
            (
                4,
                NestedField::required(
                    4,
                    "qux",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            5,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    }),
                ),
            ),
            (
                5,
                NestedField::required(5, "element", Primitive(PrimitiveType::String)),
            ),
            (
                6,
                NestedField::required(
                    6,
                    "quux",
                    Map(MapType {
                        key_field: NestedField::map_key_element(
                            7,
                            Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            8,
                            Map(MapType {
                                key_field: NestedField::map_key_element(
                                    9,
                                    Primitive(PrimitiveType::String),
                                )
                                .into(),
                                value_field: NestedField::map_value_element(
                                    10,
                                    Primitive(PrimitiveType::Int),
                                    true,
                                )
                                .into(),
                            }),
                            true,
                        )
                        .into(),
                    }),
                ),
            ),
            (
                7,
                NestedField::required(7, "key", Primitive(PrimitiveType::String)),
            ),
            (
                8,
                NestedField::required(
                    8,
                    "value",
                    Map(MapType {
                        key_field: NestedField::map_key_element(
                            9,
                            Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            10,
                            Primitive(PrimitiveType::Int),
                            true,
                        )
                        .into(),
                    }),
                ),
            ),
            (
                9,
                NestedField::required(9, "key", Primitive(PrimitiveType::String)),
            ),
            (
                10,
                NestedField::required(10, "value", Primitive(PrimitiveType::Int)),
            ),
            (
                11,
                NestedField::required(
                    11,
                    "location",
                    List(ListType {
                        element_field: NestedField::list_element(
                            12,
                            Struct(StructType::new(vec![
                                NestedField::optional(
                                    13,
                                    "latitude",
                                    Primitive(PrimitiveType::Float),
                                )
                                .into(),
                                NestedField::optional(
                                    14,
                                    "longitude",
                                    Primitive(PrimitiveType::Float),
                                )
                                .into(),
                            ])),
                            true,
                        )
                        .into(),
                    }),
                ),
            ),
            (
                12,
                NestedField::list_element(
                    12,
                    Struct(StructType::new(vec![
                        NestedField::optional(13, "latitude", Primitive(PrimitiveType::Float))
                            .into(),
                        NestedField::optional(14, "longitude", Primitive(PrimitiveType::Float))
                            .into(),
                    ])),
                    true,
                ),
            ),
            (
                13,
                NestedField::optional(13, "latitude", Primitive(PrimitiveType::Float)),
            ),
            (
                14,
                NestedField::optional(14, "longitude", Primitive(PrimitiveType::Float)),
            ),
            (
                15,
                NestedField::optional(
                    15,
                    "person",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(16, "name", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(17, "age", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                ),
            ),
            (
                16,
                NestedField::optional(16, "name", Type::Primitive(PrimitiveType::String)),
            ),
            (
                17,
                NestedField::required(17, "age", Type::Primitive(PrimitiveType::Int)),
            ),
        ]);

        let schema = table_schema_nested();
        for (id, field) in expected_id_to_field {
            assert_eq!(
                Some(&field),
                schema.field_by_id(id).map(|f| f.as_ref()),
                "Field for {id} not match."
            );
        }
    }

    #[test]
    fn test_build_accessors() {
        let schema = table_schema_nested();

        let test_struct = crate::spec::Struct::from_iter(vec![
            Some(Literal::string("foo value")),
            Some(Literal::int(1002)),
            Some(Literal::bool(true)),
            Some(Literal::List(vec![
                Some(Literal::string("qux item 1")),
                Some(Literal::string("qux item 2")),
            ])),
            Some(Literal::Map(MapValue::from([(
                Literal::string("quux key 1"),
                Some(Literal::Map(MapValue::from([(
                    Literal::string("quux nested key 1"),
                    Some(Literal::int(1000)),
                )]))),
            )]))),
            Some(Literal::List(vec![Some(Literal::Struct(
                crate::spec::Struct::from_iter(vec![
                    Some(Literal::float(52.509_09)),
                    Some(Literal::float(-1.885_249)),
                ]),
            ))])),
            Some(Literal::Struct(crate::spec::Struct::from_iter(vec![
                Some(Literal::string("Testy McTest")),
                Some(Literal::int(33)),
            ]))),
        ]);

        assert_eq!(
            schema
                .accessor_by_field_id(1)
                .unwrap()
                .get(&test_struct)
                .unwrap(),
            Some(Datum::string("foo value"))
        );
        assert_eq!(
            schema
                .accessor_by_field_id(2)
                .unwrap()
                .get(&test_struct)
                .unwrap(),
            Some(Datum::int(1002))
        );
        assert_eq!(
            schema
                .accessor_by_field_id(3)
                .unwrap()
                .get(&test_struct)
                .unwrap(),
            Some(Datum::bool(true))
        );
        assert_eq!(
            schema
                .accessor_by_field_id(16)
                .unwrap()
                .get(&test_struct)
                .unwrap(),
            Some(Datum::string("Testy McTest"))
        );
        assert_eq!(
            schema
                .accessor_by_field_id(17)
                .unwrap()
                .get(&test_struct)
                .unwrap(),
            Some(Datum::int(33))
        );
    }

    #[test]
    fn test_highest_field_id() {
        let schema = table_schema_nested();
        assert_eq!(17, schema.highest_field_id());

        let schema = table_schema_simple().0;
        assert_eq!(3, schema.highest_field_id());
    }

    #[test]
    fn test_highest_field_id_no_fields() {
        let schema = Schema::builder().with_schema_id(1).build().unwrap();
        assert_eq!(0, schema.highest_field_id());
    }

    #[test]
    fn test_field_ids_must_be_unique() {
        let reassigned_schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![5])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 3)]))
            .with_fields(vec![
                NestedField::required(5, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap_err();

        assert!(reassigned_schema.message().contains("'field.id' 3"));
    }

    #[test]
    fn test_reassign_ids_empty_schema() {
        let schema = Schema::builder().with_schema_id(1).build().unwrap();
        let reassigned_schema = schema
            .clone()
            .into_builder()
            .with_reassigned_field_ids(0)
            .build()
            .unwrap();

        assert_eq!(schema, reassigned_schema);
        assert_eq!(schema.highest_field_id(), 0);
    }

    #[test]
    fn test_identifier_field_ids() {
        // field in map
        assert!(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![2])
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "Map",
                        Type::Map(MapType::new(
                            NestedField::map_key_element(2, Type::Primitive(PrimitiveType::String))
                                .into(),
                            NestedField::map_value_element(
                                3,
                                Type::Primitive(PrimitiveType::Boolean),
                                true,
                            )
                            .into(),
                        )),
                    )
                    .into()
                ])
                .build()
                .is_err()
        );
        assert!(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![3])
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "Map",
                        Type::Map(MapType::new(
                            NestedField::map_key_element(2, Type::Primitive(PrimitiveType::String))
                                .into(),
                            NestedField::map_value_element(
                                3,
                                Type::Primitive(PrimitiveType::Boolean),
                                true,
                            )
                            .into(),
                        )),
                    )
                    .into()
                ])
                .build()
                .is_err()
        );

        // field in list
        assert!(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![2])
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "List",
                        Type::List(ListType::new(
                            NestedField::list_element(
                                2,
                                Type::Primitive(PrimitiveType::String),
                                true
                            )
                            .into(),
                        )),
                    )
                    .into()
                ])
                .build()
                .is_err()
        );

        // field in optional struct
        assert!(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![2])
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "Struct",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(
                                2,
                                "name",
                                Type::Primitive(PrimitiveType::String)
                            )
                            .into(),
                            NestedField::optional(3, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into()
                ])
                .build()
                .is_err()
        );

        // float and double
        assert!(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![1])
                .with_fields(vec![
                    NestedField::required(1, "Float", Type::Primitive(PrimitiveType::Float),)
                        .into()
                ])
                .build()
                .is_err()
        );
        assert!(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![1])
                .with_fields(vec![
                    NestedField::required(1, "Double", Type::Primitive(PrimitiveType::Double),)
                        .into()
                ])
                .build()
                .is_err()
        );

        // optional field
        assert!(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![1])
                .with_fields(vec![
                    NestedField::required(1, "Required", Type::Primitive(PrimitiveType::String),)
                        .into()
                ])
                .build()
                .is_ok()
        );
        assert!(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![1])
                .with_fields(vec![
                    NestedField::optional(1, "Optional", Type::Primitive(PrimitiveType::String),)
                        .into()
                ])
                .build()
                .is_err()
        );
    }
}
