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

use super::*;

struct PruneColumn {
    selected: HashSet<i32>,
    select_full_types: bool,
}

/// Visit a schema and returns only the fields selected by id set
pub fn prune_columns(
    schema: &Schema,
    selected: impl IntoIterator<Item = i32>,
    select_full_types: bool,
) -> Result<Type> {
    let mut visitor = PruneColumn::new(HashSet::from_iter(selected), select_full_types);
    let result = visit_schema(schema, &mut visitor);

    match result {
        Ok(s) => {
            if let Some(struct_type) = s {
                Ok(struct_type)
            } else {
                Ok(Type::Struct(StructType::default()))
            }
        }
        Err(e) => Err(e),
    }
}

impl PruneColumn {
    fn new(selected: HashSet<i32>, select_full_types: bool) -> Self {
        Self {
            selected,
            select_full_types,
        }
    }

    fn project_selected_struct(projected_field: Option<Type>) -> Result<StructType> {
        match projected_field {
            // If the field is a StructType, return it as such
            Some(Type::Struct(s)) => Ok(s),
            Some(_) => Err(Error::new(
                ErrorKind::Unexpected,
                "Projected field with struct type must be struct".to_string(),
            )),
            // If projected_field is None or not a StructType, return an empty StructType
            None => Ok(StructType::default()),
        }
    }
    fn project_list(list: &ListType, element_result: Type) -> Result<ListType> {
        if *list.element_field.field_type == element_result {
            return Ok(list.clone());
        }
        Ok(ListType {
            element_field: Arc::new(NestedField {
                id: list.element_field.id,
                name: list.element_field.name.clone(),
                required: list.element_field.required,
                field_type: Box::new(element_result),
                doc: list.element_field.doc.clone(),
                initial_default: list.element_field.initial_default.clone(),
                write_default: list.element_field.write_default.clone(),
            }),
        })
    }
    fn project_map(map: &MapType, value_result: Type) -> Result<MapType> {
        if *map.value_field.field_type == value_result {
            return Ok(map.clone());
        }
        Ok(MapType {
            key_field: map.key_field.clone(),
            value_field: Arc::new(NestedField {
                id: map.value_field.id,
                name: map.value_field.name.clone(),
                required: map.value_field.required,
                field_type: Box::new(value_result),
                doc: map.value_field.doc.clone(),
                initial_default: map.value_field.initial_default.clone(),
                write_default: map.value_field.write_default.clone(),
            }),
        })
    }
}

impl SchemaVisitor for PruneColumn {
    type T = Option<Type>;

    fn schema(&mut self, _schema: &Schema, value: Option<Type>) -> Result<Option<Type>> {
        Ok(Some(value.unwrap()))
    }

    fn field(&mut self, field: &NestedFieldRef, value: Option<Type>) -> Result<Option<Type>> {
        if self.selected.contains(&field.id) {
            if self.select_full_types {
                Ok(Some(*field.field_type.clone()))
            } else if field.field_type.is_struct() {
                Ok(Some(Type::Struct(PruneColumn::project_selected_struct(
                    value,
                )?)))
            } else if !field.field_type.is_nested() {
                Ok(Some(*field.field_type.clone()))
            } else {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Can't project list or map field directly when not selecting full type."
                        .to_string(),
                )
                .with_context("field_id", field.id.to_string())
                .with_context("field_type", field.field_type.to_string()))
            }
        } else {
            Ok(value)
        }
    }

    fn r#struct(
        &mut self,
        r#struct: &StructType,
        results: Vec<Option<Type>>,
    ) -> Result<Option<Type>> {
        let fields = r#struct.fields();
        let mut selected_field = Vec::with_capacity(fields.len());
        let mut same_type = true;

        for (field, projected_type) in zip_eq(fields.iter(), results.iter()) {
            if let Some(projected_type) = projected_type {
                if *field.field_type == *projected_type {
                    selected_field.push(field.clone());
                } else {
                    same_type = false;
                    let new_field = NestedField {
                        id: field.id,
                        name: field.name.clone(),
                        required: field.required,
                        field_type: Box::new(projected_type.clone()),
                        doc: field.doc.clone(),
                        initial_default: field.initial_default.clone(),
                        write_default: field.write_default.clone(),
                    };
                    selected_field.push(Arc::new(new_field));
                }
            }
        }

        if !selected_field.is_empty() {
            if selected_field.len() == fields.len() && same_type {
                return Ok(Some(Type::Struct(r#struct.clone())));
            } else {
                return Ok(Some(Type::Struct(StructType::new(selected_field))));
            }
        }
        Ok(None)
    }

    fn list(&mut self, list: &ListType, value: Option<Type>) -> Result<Option<Type>> {
        if self.selected.contains(&list.element_field.id) {
            if self.select_full_types {
                Ok(Some(Type::List(list.clone())))
            } else if list.element_field.field_type.is_struct() {
                let projected_struct = PruneColumn::project_selected_struct(value).unwrap();
                Ok(Some(Type::List(PruneColumn::project_list(
                    list,
                    Type::Struct(projected_struct),
                )?)))
            } else if list.element_field.field_type.is_primitive() {
                Ok(Some(Type::List(list.clone())))
            } else {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot explicitly project List or Map types, List element {} of type {} was selected",
                        list.element_field.id, list.element_field.field_type
                    ),
                ))
            }
        } else if let Some(result) = value {
            Ok(Some(Type::List(PruneColumn::project_list(list, result)?)))
        } else {
            Ok(None)
        }
    }

    fn map(
        &mut self,
        map: &MapType,
        _key_value: Option<Type>,
        value: Option<Type>,
    ) -> Result<Option<Type>> {
        if self.selected.contains(&map.value_field.id) {
            if self.select_full_types {
                Ok(Some(Type::Map(map.clone())))
            } else if map.value_field.field_type.is_struct() {
                let projected_struct =
                    PruneColumn::project_selected_struct(Some(value.unwrap())).unwrap();
                Ok(Some(Type::Map(PruneColumn::project_map(
                    map,
                    Type::Struct(projected_struct),
                )?)))
            } else if map.value_field.field_type.is_primitive() {
                Ok(Some(Type::Map(map.clone())))
            } else {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot explicitly project List or Map types, Map value {} of type {} was selected",
                        map.value_field.id, map.value_field.field_type
                    ),
                ))
            }
        } else if let Some(value_result) = value {
            Ok(Some(Type::Map(PruneColumn::project_map(
                map,
                value_result,
            )?)))
        } else if self.selected.contains(&map.key_field.id) {
            Ok(Some(Type::Map(map.clone())))
        } else {
            Ok(None)
        }
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Option<Type>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use Type::Primitive;

    use super::*;
    use crate::spec::schema::tests::table_schema_nested;

    #[test]
    fn test_schema_prune_columns_string() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([1]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_schema_prune_columns_string_full() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([1]);
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_schema_prune_columns_list() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
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
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([5]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_list_itself() {
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([4]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_prune_columns_list_full() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
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
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([5]);
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_map() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
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
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([9]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_map_itself() {
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([6]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_prune_columns_map_full() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
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
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([9]);
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_map_key() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
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
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([10]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_struct() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        15,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(
                                16,
                                "name",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([16]);
        let result = prune_columns(&schema, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_struct_full() {
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        15,
                        "person",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(
                                16,
                                "name",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let schema = table_schema_nested();
        let selected: HashSet<i32> = HashSet::from([16]);
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_empty_struct() {
        let schema_with_empty_struct_field = Schema::builder()
            .with_fields(vec![
                NestedField::optional(15, "person", Type::Struct(StructType::new(vec![]))).into(),
            ])
            .build()
            .unwrap();
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(15, "person", Type::Struct(StructType::new(vec![])))
                        .into(),
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let selected: HashSet<i32> = HashSet::from([15]);
        let result = prune_columns(&schema_with_empty_struct_field, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_empty_struct_full() {
        let schema_with_empty_struct_field = Schema::builder()
            .with_fields(vec![
                NestedField::optional(15, "person", Type::Struct(StructType::new(vec![]))).into(),
            ])
            .build()
            .unwrap();
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(15, "person", Type::Struct(StructType::new(vec![])))
                        .into(),
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let selected: HashSet<i32> = HashSet::from([15]);
        let result = prune_columns(&schema_with_empty_struct_field, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_struct_in_map() {
        let schema_with_struct_in_map_field = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(
                    6,
                    "id_to_person",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            7,
                            Type::Primitive(PrimitiveType::Int),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            8,
                            Type::Struct(StructType::new(vec![
                                NestedField::optional(10, "name", Primitive(PrimitiveType::String))
                                    .into(),
                                NestedField::required(11, "age", Primitive(PrimitiveType::Int))
                                    .into(),
                            ])),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        6,
                        "id_to_person",
                        Type::Map(MapType {
                            key_field: NestedField::map_key_element(
                                7,
                                Type::Primitive(PrimitiveType::Int),
                            )
                            .into(),
                            value_field: NestedField::map_value_element(
                                8,
                                Type::Struct(StructType::new(vec![
                                    NestedField::required(11, "age", Primitive(PrimitiveType::Int))
                                        .into(),
                                ])),
                                true,
                            )
                            .into(),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let selected: HashSet<i32> = HashSet::from([11]);
        let result = prune_columns(&schema_with_struct_in_map_field, selected, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }
    #[test]
    fn test_prune_columns_struct_in_map_full() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(
                    6,
                    "id_to_person",
                    Type::Map(MapType {
                        key_field: NestedField::map_key_element(
                            7,
                            Type::Primitive(PrimitiveType::Int),
                        )
                        .into(),
                        value_field: NestedField::map_value_element(
                            8,
                            Type::Struct(StructType::new(vec![
                                NestedField::optional(10, "name", Primitive(PrimitiveType::String))
                                    .into(),
                                NestedField::required(11, "age", Primitive(PrimitiveType::Int))
                                    .into(),
                            ])),
                            true,
                        )
                        .into(),
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let expected_type = Type::from(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        6,
                        "id_to_person",
                        Type::Map(MapType {
                            key_field: NestedField::map_key_element(
                                7,
                                Type::Primitive(PrimitiveType::Int),
                            )
                            .into(),
                            value_field: NestedField::map_value_element(
                                8,
                                Type::Struct(StructType::new(vec![
                                    NestedField::required(11, "age", Primitive(PrimitiveType::Int))
                                        .into(),
                                ])),
                                true,
                            )
                            .into(),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap()
                .as_struct()
                .clone(),
        );
        let selected: HashSet<i32> = HashSet::from([11]);
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_type);
    }

    #[test]
    fn test_prune_columns_select_original_schema() {
        let schema = table_schema_nested();
        let selected: HashSet<i32> = (0..schema.highest_field_id() + 1).collect();
        let result = prune_columns(&schema, selected, true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Type::Struct(schema.as_struct().clone()));
    }
}
