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

use super::utils::try_insert_field;
use super::*;

pub struct ReassignFieldIds {
    next_field_id: i32,
    old_to_new_id: HashMap<i32, i32>,
}

// We are not using the visitor here, as post order traversal is not desired.
// Instead we want to re-assign all fields on one level first before diving deeper.
impl ReassignFieldIds {
    pub fn new(start_from: i32) -> Self {
        Self {
            next_field_id: start_from,
            old_to_new_id: HashMap::new(),
        }
    }

    pub fn reassign_field_ids(
        &mut self,
        fields: Vec<NestedFieldRef>,
    ) -> Result<Vec<NestedFieldRef>> {
        // Visit fields on the same level first
        let outer_fields = fields
            .into_iter()
            .map(|field| {
                try_insert_field(&mut self.old_to_new_id, field.id, self.next_field_id)?;
                let new_field = Arc::unwrap_or_clone(field).with_id(self.next_field_id);
                self.increase_next_field_id()?;
                Ok(Arc::new(new_field))
            })
            .collect::<Result<Vec<_>>>()?;

        // Now visit nested fields
        outer_fields
            .into_iter()
            .map(|field| {
                if field.field_type.is_primitive() {
                    Ok(field)
                } else {
                    let mut new_field = Arc::unwrap_or_clone(field);
                    *new_field.field_type = self.reassign_ids_visit_type(*new_field.field_type)?;
                    Ok(Arc::new(new_field))
                }
            })
            .collect()
    }

    fn reassign_ids_visit_type(&mut self, field_type: Type) -> Result<Type> {
        match field_type {
            Type::Primitive(s) => Ok(Type::Primitive(s)),
            Type::Struct(s) => {
                let new_fields = self.reassign_field_ids(s.fields().to_vec())?;
                Ok(Type::Struct(StructType::new(new_fields)))
            }
            Type::List(l) => {
                self.old_to_new_id
                    .insert(l.element_field.id, self.next_field_id);
                let mut element_field = Arc::unwrap_or_clone(l.element_field);
                element_field.id = self.next_field_id;
                self.increase_next_field_id()?;
                *element_field.field_type =
                    self.reassign_ids_visit_type(*element_field.field_type)?;
                Ok(Type::List(ListType {
                    element_field: Arc::new(element_field),
                }))
            }
            Type::Map(m) => {
                self.old_to_new_id
                    .insert(m.key_field.id, self.next_field_id);
                let mut key_field = Arc::unwrap_or_clone(m.key_field);
                key_field.id = self.next_field_id;
                self.increase_next_field_id()?;
                *key_field.field_type = self.reassign_ids_visit_type(*key_field.field_type)?;

                self.old_to_new_id
                    .insert(m.value_field.id, self.next_field_id);
                let mut value_field = Arc::unwrap_or_clone(m.value_field);
                value_field.id = self.next_field_id;
                self.increase_next_field_id()?;
                *value_field.field_type = self.reassign_ids_visit_type(*value_field.field_type)?;

                Ok(Type::Map(MapType {
                    key_field: Arc::new(key_field),
                    value_field: Arc::new(value_field),
                }))
            }
        }
    }

    fn increase_next_field_id(&mut self) -> Result<()> {
        self.next_field_id = self.next_field_id.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Field ID overflowed, cannot add more fields",
            )
        })?;
        Ok(())
    }

    pub fn apply_to_identifier_fields(&self, field_ids: HashSet<i32>) -> Result<HashSet<i32>> {
        field_ids
            .into_iter()
            .map(|id| {
                self.old_to_new_id.get(&id).copied().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Identifier Field ID {id} not found"),
                    )
                })
            })
            .collect()
    }

    pub fn apply_to_aliases(
        &self,
        alias: BiHashMap<String, i32>,
    ) -> Result<BiHashMap<String, i32>> {
        alias
            .into_iter()
            .map(|(name, id)| {
                self.old_to_new_id
                    .get(&id)
                    .copied()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Field with id {id} for alias {name} not found"),
                        )
                    })
                    .map(|new_id| (name, new_id))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::schema::tests::table_schema_nested;

    #[test]
    fn test_reassign_ids() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![3])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 3)]))
            .with_fields(vec![
                NestedField::optional(5, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(4, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        let reassigned_schema = schema
            .into_builder()
            .with_reassigned_field_ids(0)
            .build()
            .unwrap();

        let expected = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 1)]))
            .with_fields(vec![
                NestedField::optional(0, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(1, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();

        pretty_assertions::assert_eq!(expected, reassigned_schema);
        assert_eq!(reassigned_schema.highest_field_id(), 2);
    }

    #[test]
    fn test_reassigned_ids_nested() {
        let schema = table_schema_nested();
        let reassigned_schema = schema
            .into_builder()
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 2)]))
            .with_reassigned_field_ids(0)
            .build()
            .unwrap();

        let expected = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 1)]))
            .with_fields(vec![
                NestedField::optional(0, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(1, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::required(
                    3,
                    "qux",
                    Type::List(ListType {
                        element_field: NestedField::list_element(
                            7,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
                NestedField::required(
                    4,
                    "quux",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            8,
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            9,
                            Type::Map(MapType {
                                key_field: NestedField::map_key_element(
                                    10,
                                    Type::Primitive(PrimitiveType::String),
                                )
                                .into(),
                                value_field: NestedField::map_value_element(
                                    11,
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
                    5,
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
                    6,
                    "person",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(15, "name", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(16, "age", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();

        pretty_assertions::assert_eq!(expected, reassigned_schema);
        assert_eq!(reassigned_schema.highest_field_id(), 16);
        assert_eq!(reassigned_schema.field_by_id(6).unwrap().name, "person");
        assert_eq!(reassigned_schema.field_by_id(16).unwrap().name, "age");
    }

    #[test]
    fn test_reassign_ids_fails_with_duplicate_ids() {
        let reassigned_schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![5])
            .with_alias(BiHashMap::from_iter(vec![("bar_alias".to_string(), 3)]))
            .with_fields(vec![
                NestedField::required(5, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .with_reassigned_field_ids(0)
            .build()
            .unwrap_err();

        assert!(reassigned_schema.message().contains("'field.id' 3"));
    }
}
