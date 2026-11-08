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

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_parquet::ParquetReadSchema;
use databend_common_storages_parquet::ParquetSchemaResolver;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::schema::types::SchemaDescriptor;
use parquet::schema::types::Type;
use parquet::schema::types::TypePtr;
use serde::Deserialize;

pub const NAME_MAPPING_PROPERTY: &str = "schema.name-mapping.default";

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MappedField {
    field_id: Option<i32>,
    names: Vec<String>,
    fields: Option<Vec<MappedField>>,
}

/// Interpret catalog identities once per scan. Only this table-engine layer
/// understands Iceberg name mapping, historical aliases and collection fields.
pub struct IcebergParquetSchemaResolver {
    mapping_json: Option<String>,
    mapping: OnceLock<Result<Vec<MappedField>>>,
    names: HashMap<i32, String>,
    read_schema: Arc<ParquetReadSchema>,
}

impl IcebergParquetSchemaResolver {
    pub fn new(schema: Schema, mapping: Option<&str>) -> Result<Self> {
        let mut names = HashMap::new();
        for field in schema.fields() {
            collect_names(field, &mut names)?;
        }
        Ok(Self {
            mapping_json: mapping.map(str::to_owned),
            mapping: OnceLock::new(),
            names,
            read_schema: Arc::new(ParquetReadSchema::new(schema)),
        })
    }
}

impl ParquetSchemaResolver for IcebergParquetSchemaResolver {
    fn read_schema(&self) -> Arc<ParquetReadSchema> {
        self.read_schema.clone()
    }

    fn resolve(&self, file: &SchemaDescriptor) -> Result<SchemaDescriptor> {
        // Explicit file IDs do not depend on name mapping, even if an unused
        // mapping property is invalid. Decode once, only for ID-less fields.
        let mapping = if file
            .root_schema()
            .get_fields()
            .iter()
            .any(needs_name_mapping)
        {
            self.mapping
                .get_or_init(|| {
                    self.mapping_json
                        .as_deref()
                        .map(|json| {
                            serde_json::from_str(json).map_err(|e| {
                                ErrorCode::TableSchemaMismatch(format!(
                                    "Invalid {NAME_MAPPING_PROPERTY}: {e}"
                                ))
                            })
                        })
                        .transpose()
                        .map(Option::unwrap_or_default)
                })
                .as_ref()
                .map_err(Clone::clone)?
                .as_slice()
        } else {
            &[]
        };
        let mut ids = HashSet::new();
        let fields = file
            .root_schema()
            .get_fields()
            .iter()
            .map(|field| resolve_field(field, mapping, &self.names, &mut ids, None, None))
            .collect::<Result<Vec<_>>>()?;
        let root = Type::group_type_builder(file.root_schema().name())
            .with_fields(fields)
            .build()?;
        Ok(SchemaDescriptor::new(Arc::new(root)))
    }
}

fn collect_names(field: &Field, names: &mut HashMap<i32, String>) -> Result<()> {
    if let Some(id) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
        let id = id.parse().map_err(|e| {
            ErrorCode::TableSchemaMismatch(format!("Invalid Iceberg field ID {id}: {e}"))
        })?;
        names.insert(id, field.name().clone());
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            for field in fields {
                collect_names(field, names)?;
            }
        }
        DataType::List(element) | DataType::LargeList(element) => collect_names(element, names)?,
        DataType::Map(entries, _) => collect_names(entries, names)?,
        _ => (),
    }
    Ok(())
}

fn is_list_wrapper(list: &Type, element: &Type) -> bool {
    let Type::GroupType { fields, .. } = element else {
        return false;
    };
    if fields.len() != 1 {
        return false;
    }
    // Match parquet-rs' LIST discrimination, including the exceptions for an
    // annotated repeated list and a group whose only child is repeated.
    let is_list = element.get_basic_info().converted_type() == parquet::basic::ConvertedType::LIST
        || matches!(
            element.get_basic_info().logical_type_ref(),
            Some(parquet::basic::LogicalType::List)
        );
    let has_repeated_child = fields[0].get_basic_info().has_repetition()
        && fields[0].get_basic_info().repetition() == parquet::basic::Repetition::REPEATED;
    is_list
        || has_repeated_child
        || (element.name() != "array" && element.name() != format!("{}_tuple", list.name()))
}

fn needs_name_mapping(field: &TypePtr) -> bool {
    if !field.get_basic_info().has_id() {
        return true;
    }
    let Type::GroupType { fields, .. } = field.as_ref() else {
        return false;
    };
    match field.get_basic_info().converted_type() {
        parquet::basic::ConvertedType::LIST => fields.iter().any(|element| {
            if is_list_wrapper(field, element) {
                element.get_fields().iter().any(needs_name_mapping)
            } else {
                needs_name_mapping(element)
            }
        }),
        parquet::basic::ConvertedType::MAP => fields.iter().any(|wrapper| {
            let Type::GroupType { fields, .. } = wrapper.as_ref() else {
                return false;
            };
            fields.iter().any(needs_name_mapping)
        }),
        _ => fields.iter().any(needs_name_mapping),
    }
}

/// Preserve all physical attributes. LIST/MAP wrappers are not Iceberg fields
/// and must not consume a mapping level. Never convert the schema through Arrow.
fn resolve_field(
    field: &Type,
    mapping: &[MappedField],
    names: &HashMap<i32, String>,
    ids: &mut HashSet<i32>,
    logical_name: Option<&str>,
    physical_name: Option<&str>,
) -> Result<TypePtr> {
    let info = field.get_basic_info();
    let mapped = if info.has_id() {
        mapping.iter().find(|m| m.field_id == Some(info.id()))
    } else {
        let mut matches = mapping.iter().filter(|m| {
            m.names
                .iter()
                .any(|n| n == logical_name.unwrap_or(field.name()))
        });
        let matched = matches.next();
        if matches.next().is_some() {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Ambiguous Iceberg name mapping for field {}",
                field.name()
            )));
        }
        matched
    };
    let id = if info.has_id() {
        Some(info.id())
    } else {
        mapped.and_then(|m| m.field_id)
    };
    if let Some(id) = id {
        if id <= 0 {
            let source = if info.has_id() {
                "file-supplied Parquet"
            } else {
                "name-mapped"
            };
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Invalid {source} field ID {id} for {}: Iceberg field IDs must be positive; an explicit ID is not treated as missing",
                field.name()
            )));
        }
        if !ids.insert(id) {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Duplicate Iceberg field ID {id} in Parquet schema"
            )));
        }
    }
    // Never let dropped/unmapped fields bind to a current field by name during
    // recursive struct alignment, even after a name is reused by a new column.
    let name = if let Some(name) = physical_name {
        Cow::Borrowed(name)
    } else if let Some(name) = id.and_then(|id| names.get(&id)) {
        Cow::Borrowed(name.as_str())
    } else {
        let mut name = format!("__databend_iceberg_unmapped_{}", field.name());
        while names.values().any(|target| target == &name) {
            name.push('_');
        }
        Cow::Owned(name)
    };
    let children = mapped.and_then(|m| m.fields.as_deref()).unwrap_or_default();
    let resolved = match field {
        Type::PrimitiveType {
            physical_type,
            type_length,
            scale,
            precision,
            ..
        } => Type::primitive_type_builder(&name, *physical_type)
            .with_repetition(info.repetition())
            .with_converted_type(info.converted_type())
            .with_logical_type(info.logical_type_ref().cloned())
            .with_length(*type_length)
            .with_scale(*scale)
            .with_precision(*precision)
            .with_id(id)
            .build()?,
        Type::GroupType { fields, .. } => {
            let fields = if info.converted_type() == parquet::basic::ConvertedType::LIST {
                fields
                    .iter()
                    .map(|element| {
                        if is_list_wrapper(field, element) {
                            let child = resolve_field(
                                &element.get_fields()[0],
                                children,
                                names,
                                ids,
                                Some("element"),
                                None,
                            )?;
                            // Wrapper status may depend on the old list name. A
                            // wrapper named `<new_list>_tuple` would turn into a
                            // legacy struct element once the outer list is renamed.
                            let wrapper_name = if element.name() == format!("{name}_tuple") {
                                "list"
                            } else {
                                element.name()
                            };
                            Ok(Arc::new(
                                Type::group_type_builder(wrapper_name)
                                    .with_repetition(element.get_basic_info().repetition())
                                    .with_converted_type(element.get_basic_info().converted_type())
                                    .with_logical_type(
                                        element.get_basic_info().logical_type_ref().cloned(),
                                    )
                                    .with_fields(vec![child])
                                    .build()?,
                            ))
                        } else {
                            // A legacy repeated group is itself the element.
                            // `array` preserves that encoding after the outer
                            // list is renamed; `<old_list>_tuple` would not.
                            let physical_name = matches!(element.as_ref(), Type::GroupType { .. })
                                .then_some("array");
                            resolve_field(
                                element,
                                children,
                                names,
                                ids,
                                Some("element"),
                                physical_name,
                            )
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
            } else if info.converted_type() == parquet::basic::ConvertedType::MAP {
                fields
                    .iter()
                    .map(|wrapper| {
                        let wrapper_info = wrapper.get_basic_info();
                        let Type::GroupType { fields, .. } = wrapper.as_ref() else {
                            return Err(ErrorCode::TableSchemaMismatch(
                                "Invalid Parquet MAP wrapper",
                            ));
                        };
                        let fields = fields
                            .iter()
                            .map(|child| resolve_field(child, children, names, ids, None, None))
                            .collect::<Result<Vec<_>>>()?;
                        Ok(Arc::new(
                            Type::group_type_builder(wrapper.name())
                                .with_repetition(wrapper_info.repetition())
                                .with_converted_type(wrapper_info.converted_type())
                                .with_logical_type(wrapper_info.logical_type_ref().cloned())
                                .with_fields(fields)
                                .build()?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                fields
                    .iter()
                    .map(|child| resolve_field(child, children, names, ids, None, None))
                    .collect::<Result<Vec<_>>>()?
            };
            Type::group_type_builder(&name)
                .with_repetition(info.repetition())
                .with_converted_type(info.converted_type())
                .with_logical_type(info.logical_type_ref().cloned())
                .with_fields(fields)
                .with_id(id)
                .build()?
        }
    };
    Ok(Arc::new(resolved))
}

#[cfg(test)]
mod tests {
    use databend_common_catalog::plan::Projection;
    use databend_common_expression::TableSchema;
    use parquet::schema::parser::parse_message_type;

    use super::*;

    fn field(name: &str, ty: DataType, id: i32) -> Field {
        Field::new(name, ty, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }
    fn file(schema: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(schema).unwrap()))
    }
    #[test]
    fn aliases_preserve_decimal_physical_encoding() {
        let resolver = IcebergParquetSchemaResolver::new(
            Schema::new(vec![field("amount", DataType::Decimal128(15, 2), 2)]),
            Some(r#"[{"field-id":2,"names":["d","old_amount","amount"]}]"#),
        )
        .unwrap();
        let original = file("message test { OPTIONAL FIXED_LEN_BYTE_ARRAY(7) d (DECIMAL(15,2)); }");
        let resolved = resolver.resolve(&original).unwrap();
        let column = resolved.column(0);
        assert_eq!(column.name(), "amount");
        assert_eq!(column.self_type().get_basic_info().id(), 2);
        assert_eq!(
            column.physical_type(),
            parquet::basic::Type::FIXED_LEN_BYTE_ARRAY
        );
        assert_eq!(column.type_length(), 7);
        assert_eq!(column.type_precision(), 15);
        assert_eq!(column.type_scale(), 2);
        assert_eq!(column.max_def_level(), original.column(0).max_def_level());
    }
    #[test]
    fn historical_aliases_resolve_to_the_same_reader_schema() {
        let resolver = IcebergParquetSchemaResolver::new(
            Schema::new(vec![field("amount", DataType::Decimal128(15, 2), 2)]),
            Some(r#"[{"field-id":2,"names":["old_d","legacy_amount","amount"]}]"#),
        )
        .unwrap();
        let first = resolver
            .resolve(&file(
                "message first { OPTIONAL FIXED_LEN_BYTE_ARRAY(7) old_d (DECIMAL(15,2)); }",
            ))
            .unwrap();
        let second = resolver.resolve(&file("message second { OPTIONAL FIXED_LEN_BYTE_ARRAY(7) legacy_amount (DECIMAL(15,2)); }")).unwrap();
        // Source reader reuse compares resolved fields, ignoring the message
        // name. Raw aliases therefore do not cause policies to be rebuilt.
        assert_eq!(
            first.root_schema().get_fields(),
            second.root_schema().get_fields()
        );
        assert_eq!(first.column(0).type_length(), 7);
        assert_eq!(second.column(0).type_length(), 7);
    }

    #[test]
    fn legacy_array_and_tuple_lists_keep_struct_elements_after_renaming() {
        use parquet::arrow::parquet_to_arrow_schema;
        let target = Schema::new(vec![field(
            "items",
            DataType::List(Arc::new(field(
                "element",
                DataType::Struct(vec![field("y", DataType::Int64, 3)].into()),
                2,
            ))),
            1,
        )]);
        let mapping = r#"[{"field-id":1,"names":["old_items","items"],"fields":[{"field-id":2,"names":["element"],"fields":[{"field-id":3,"names":["x","y"]}]}]}]"#;
        for element_name in ["array", "old_items_tuple"] {
            for explicit_ids in [false, true] {
                let (parent_id, element_id, child_id) = if explicit_ids {
                    (" = 1", " = 2", " = 3")
                } else {
                    ("", "", "")
                };
                let original = file(&format!(
                    "message test {{ OPTIONAL group old_items (LIST){parent_id} {{ REPEATED group {element_name}{element_id} {{ OPTIONAL INT64 x{child_id}; }} }} }}"
                ));
                let original_arrow = parquet_to_arrow_schema(&original, None).unwrap();
                let DataType::List(original_element) = original_arrow.field(0).data_type() else {
                    panic!()
                };
                assert!(matches!(original_element.data_type(), DataType::Struct(_)));
                let resolver =
                    IcebergParquetSchemaResolver::new(target.clone(), Some(mapping)).unwrap();
                let resolved = resolver.resolve(&original).unwrap();
                let resolved_arrow = parquet_to_arrow_schema(&resolved, None).unwrap();
                assert_eq!(resolved_arrow.field(0).name(), "items");
                let DataType::List(element) = resolved_arrow.field(0).data_type() else {
                    panic!()
                };
                let DataType::Struct(children) = element.data_type() else {
                    panic!("legacy {element_name} lost its struct element: {element:?}")
                };
                assert_eq!(children[0].name(), "y");
                assert_eq!(children[0].data_type(), &DataType::Int64);
                // parquet-rs does not attach the repeated group ID to the
                // Arrow element field in this legacy encoding. It must remain
                // in the physical descriptor, while leaf IDs reach Arrow.
                let repeated = &resolved.root_schema().get_fields()[0].get_fields()[0];
                assert_eq!(repeated.name(), "array");
                assert_eq!(repeated.get_basic_info().id(), 2);
                assert_eq!(children[0].metadata()[PARQUET_FIELD_ID_META_KEY], "3");
                assert_eq!(
                    resolved.column(0).max_def_level(),
                    original.column(0).max_def_level()
                );
                assert_eq!(
                    resolved.column(0).max_rep_level(),
                    original.column(0).max_rep_level()
                );
            }
        }
    }

    #[test]
    fn explicit_ids_do_not_parse_unused_invalid_mapping_in_collection_wrappers() {
        let target = Schema::new(vec![
            field(
                "items",
                DataType::List(Arc::new(field("element", DataType::Int64, 2))),
                1,
            ),
            field(
                "map",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                field("key", DataType::Int64, 4).with_nullable(false),
                                field("value", DataType::Int64, 5),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                3,
            ),
        ]);
        let resolver = IcebergParquetSchemaResolver::new(target, Some("not json")).unwrap();
        let explicit = file(
            "message test { OPTIONAL group items (LIST) = 1 { REPEATED group list { OPTIONAL INT64 element = 2; } } OPTIONAL group map (MAP) = 3 { REPEATED group key_value { REQUIRED INT64 key = 4; OPTIONAL INT64 value = 5; } } }",
        );
        assert!(resolver.resolve(&explicit).is_ok());
        assert!(resolver.mapping.get().is_none());
        let missing = file(
            "message test { OPTIONAL group items (LIST) = 1 { REPEATED group list { OPTIONAL INT64 element; } } }",
        );
        let error = resolver.resolve(&missing).unwrap_err();
        assert!(error.message().contains(NAME_MAPPING_PROPERTY));
        // A failed ID-less file must not poison later fully identified files.
        assert!(resolver.resolve(&explicit).is_ok());
    }

    #[test]
    fn wrapper_named_after_the_new_list_stays_a_wrapper() {
        use parquet::arrow::parquet_to_arrow_schema;
        // `items_tuple` is a three-level wrapper under `old_items`, but would
        // become a legacy struct element under the renamed `items` list.
        let target = Schema::new(vec![field(
            "items",
            DataType::List(Arc::new(field("element", DataType::Int64, 2))),
            1,
        )]);
        let resolver = IcebergParquetSchemaResolver::new(
            target,
            Some(r#"[{"field-id":1,"names":["old_items","items"],"fields":[{"field-id":2,"names":["element"]}]}]"#),
        )
        .unwrap();
        let original = file(
            "message test { OPTIONAL group old_items (LIST) { REPEATED group items_tuple { OPTIONAL INT64 element; } } }",
        );
        let original_arrow = parquet_to_arrow_schema(&original, None).unwrap();
        let resolved = resolver.resolve(&original).unwrap();
        let resolved_arrow = parquet_to_arrow_schema(&resolved, None).unwrap();
        let (DataType::List(before), DataType::List(after)) = (
            original_arrow.field(0).data_type(),
            resolved_arrow.field(0).data_type(),
        ) else {
            panic!()
        };
        assert_eq!(before.data_type(), &DataType::Int64);
        assert_eq!(after.data_type(), &DataType::Int64);
        assert_eq!(resolved_arrow.field(0).name(), "items");
        assert_eq!(after.metadata()[PARQUET_FIELD_ID_META_KEY], "2");
        assert_eq!(
            resolved.column(0).max_rep_level(),
            original.column(0).max_rep_level()
        );
    }

    #[test]
    fn file_supplied_zero_id_has_a_specific_error_and_never_uses_mapping() {
        let resolver = IcebergParquetSchemaResolver::new(
            Schema::new(vec![field("a", DataType::Int64, 1)]),
            Some(r#"[{"field-id":1,"names":["a"]}]"#),
        )
        .unwrap();
        let error = resolver
            .resolve(&file("message test { OPTIONAL INT64 a = 0; }"))
            .unwrap_err();
        assert!(error.message().contains("file-supplied Parquet field ID 0"));
        assert!(error.message().contains("not treated as missing"));
    }

    #[test]
    fn explicit_ids_override_names_and_positions() {
        let target = Schema::new(vec![
            field("a", DataType::Int64, 1),
            field("b", DataType::Int64, 2),
        ]);
        let table = TableSchema::try_from(&target).unwrap();
        let resolver = IcebergParquetSchemaResolver::new(
            target,
            Some(r#"[{"field-id":1,"names":["a"]},{"field-id":2,"names":["b"]}]"#),
        )
        .unwrap();
        let resolved = resolver
            .resolve(&file(
                "message test { OPTIONAL INT64 a = 2; OPTIONAL INT64 b = 1; }",
            ))
            .unwrap();
        assert_eq!(resolved.column(0).name(), "b");
        assert_eq!(
            resolver
                .read_schema()
                .project(&Projection::Columns(vec![0]), &table, &resolved)
                .unwrap()
                .1,
            vec![1]
        );
    }
    #[test]
    fn identical_nested_aliases_are_parent_scoped() {
        let resolver = IcebergParquetSchemaResolver::new(Schema::new(vec![
            field("left", DataType::Struct(vec![field("value", DataType::Int64, 30)].into()), 10),
            field("right", DataType::Struct(vec![field("value", DataType::Int64, 40)].into()), 20),
        ]), Some(r#"[
            {"field-id":10,"names":["old_left","left"],"fields":[{"field-id":30,"names":["old_value","value"]}]},
            {"field-id":20,"names":["old_right","right"],"fields":[{"field-id":40,"names":["old_value","value"]}]}
        ]"#)).unwrap();
        let resolved = resolver.resolve(&file("message test { OPTIONAL group old_right { OPTIONAL INT64 old_value; } OPTIONAL group old_left { OPTIONAL INT64 old_value; } }")).unwrap();
        assert_eq!(resolved.column(0).self_type().get_basic_info().id(), 40);
        assert_eq!(resolved.column(1).self_type().get_basic_info().id(), 30);
    }
    #[test]
    fn list_and_map_wrappers_do_not_consume_mapping_levels() {
        let resolver = IcebergParquetSchemaResolver::new(Schema::new(vec![
            field("items", DataType::List(Arc::new(field("element", DataType::Int64, 11))), 1),
            field("attributes", DataType::Map(Arc::new(Field::new("entries", DataType::Struct(vec![
                field("key", DataType::Int64, 21).with_nullable(false), field("value", DataType::Int64, 22),
            ].into()), false)), false), 2),
        ]), Some(r#"[
            {"field-id":1,"names":["old_items"],"fields":[{"field-id":11,"names":["element"]}]},
            {"field-id":2,"names":["old_attributes"],"fields":[{"field-id":21,"names":["key"]},{"field-id":22,"names":["value"]}]}
        ]"#)).unwrap();
        let original = file(
            "message test { OPTIONAL group old_items (LIST) { REPEATED group list { OPTIONAL INT64 item; } } OPTIONAL group old_attributes (MAP) { REPEATED group key_value { REQUIRED INT64 key; OPTIONAL INT64 value; } } }",
        );
        let resolved = resolver.resolve(&original).unwrap();
        for (i, id) in [11, 21, 22].into_iter().enumerate() {
            assert_eq!(resolved.column(i).self_type().get_basic_info().id(), id);
            assert_eq!(
                resolved.column(i).max_def_level(),
                original.column(i).max_def_level()
            );
            assert_eq!(
                resolved.column(i).max_rep_level(),
                original.column(i).max_rep_level()
            );
        }
    }
    #[test]
    fn absent_or_invalid_mapping_does_not_guess_names() {
        let schema = Schema::new(vec![field("a", DataType::Int64, 1)]);
        let table = TableSchema::try_from(&schema).unwrap();
        let resolver = IcebergParquetSchemaResolver::new(schema.clone(), None).unwrap();
        let resolved = resolver
            .resolve(&file("message test { OPTIONAL INT64 a; }"))
            .unwrap();
        assert!(
            resolver
                .read_schema()
                .project(&Projection::Columns(vec![0]), &table, &resolved)
                .is_err()
        );
        let invalid = IcebergParquetSchemaResolver::new(schema.clone(), Some("not json")).unwrap();
        assert!(
            invalid
                .resolve(&file("message test { OPTIONAL INT64 a; }"))
                .is_err()
        );
        assert!(
            invalid
                .resolve(&file("message test { OPTIONAL INT64 old = 1; }"))
                .is_ok()
        );
        let resolver = IcebergParquetSchemaResolver::new(
            schema,
            Some(r#"[{"field-id":1,"names":["a"]},{"field-id":2,"names":["a"]}]"#),
        )
        .unwrap();
        assert!(
            resolver
                .resolve(&file("message test { OPTIONAL INT64 a; }"))
                .is_err()
        );
    }
    #[test]
    fn unmapped_fields_do_not_block_explicit_ids_without_mapping() {
        let schema = Schema::new(vec![field("renamed", DataType::Int64, 1)]);
        let table = TableSchema::try_from(&schema).unwrap();
        let resolver = IcebergParquetSchemaResolver::new(schema, None).unwrap();
        let resolved = resolver
            .resolve(&file(
                "message test { OPTIONAL INT64 deleted; OPTIONAL INT64 old = 1; }",
            ))
            .unwrap();
        assert_eq!(
            resolver
                .read_schema()
                .project(&Projection::Columns(vec![0]), &table, &resolved)
                .unwrap()
                .1,
            vec![1]
        );
    }
}
