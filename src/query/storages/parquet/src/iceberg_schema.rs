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

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::LargeListArray;
use arrow_array::MapArray;
use arrow_array::StructArray;
use arrow_array::new_null_array;
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use databend_common_catalog::plan::Projection;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchema;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use parquet::schema::types::Type;
use parquet::schema::types::TypePtr;
use serde::Deserialize;

/// The complete Iceberg Arrow schema retains nested IDs, which TableDataType does not.
/// Kept in the scan schema's metadata so it survives projection and distribution.
pub const ICEBERG_ARROW_SCHEMA: &str = "databend.iceberg.arrow-schema";
pub const ICEBERG_NAME_MAPPING: &str = "schema.name-mapping.default";

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MappedField {
    field_id: Option<i32>,
    names: Vec<String>,
    fields: Option<Vec<MappedField>>,
}

pub fn iceberg_arrow_schema(schema: &TableSchema) -> Result<Option<Schema>> {
    schema
        .metadata
        .get(ICEBERG_ARROW_SCHEMA)
        .map(|json| {
            serde_json::from_str(json).map_err(|e| {
                ErrorCode::TableSchemaMismatch(format!("Invalid Iceberg Arrow schema: {e}"))
            })
        })
        .transpose()
}

/// Unlike the generic conversion, retain Iceberg identity metadata in batches.
pub(crate) fn scan_data_schema(table: &TableSchema) -> DataSchema {
    DataSchema::new_from(DataSchema::from(table).fields, table.metadata.clone())
}

fn field_id(field: &Field) -> Result<Option<i32>> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .map(|id| {
            id.parse().map_err(|e| {
                ErrorCode::TableSchemaMismatch(format!("Invalid Iceberg field ID {id}: {e}"))
            })
        })
        .transpose()
}

fn collect_names(field: &Field, names: &mut HashMap<i32, String>) -> Result<()> {
    if let Some(id) = field_id(field)? {
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

/// Apply name mapping to missing IDs, then resolve names using the current Iceberg
/// schema. Never round-trip through ArrowSchemaConverter: decimal widths and every
/// other physical detail must remain exactly as stored in the file. LIST/MAP
/// wrapper groups aren't Iceberg fields and must not consume a mapping level.
pub fn resolve_iceberg_schema(
    file: &SchemaDescriptor,
    table: &TableSchema,
) -> Result<SchemaDescriptor> {
    let target = iceberg_arrow_schema(table)?
        .ok_or_else(|| ErrorCode::Internal("Missing Iceberg Arrow schema in Parquet scan"))?;
    let mapping: Vec<MappedField> = table
        .metadata
        .get(ICEBERG_NAME_MAPPING)
        .map(|json| {
            serde_json::from_str(json).map_err(|e| {
                ErrorCode::TableSchemaMismatch(format!("Invalid {ICEBERG_NAME_MAPPING}: {e}"))
            })
        })
        .transpose()?
        .unwrap_or_default();
    let mut names = HashMap::new();
    for field in target.fields() {
        collect_names(field, &mut names)?;
    }
    let mut ids = HashSet::new();
    let fields = file
        .root_schema()
        .get_fields()
        .iter()
        .map(|field| resolve_field(field, &mapping, &names, &mut ids, None))
        .collect::<Result<Vec<_>>>()?;
    let root = Type::group_type_builder(file.root_schema().name())
        .with_fields(fields)
        .build()?;
    Ok(SchemaDescriptor::new(Arc::new(root)))
}

fn resolve_field(
    field: &Type,
    mapping: &[MappedField],
    names: &HashMap<i32, String>,
    ids: &mut HashSet<i32>,
    logical_name: Option<&str>,
) -> Result<TypePtr> {
    let info = field.get_basic_info();
    // An explicit ID is authoritative, even when the name mapping disagrees.
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
        if id <= 0 || !ids.insert(id) {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Invalid or duplicate Iceberg field ID {id} in Parquet schema"
            )));
        }
    }
    // A dropped or unmapped field may reuse a current field's name. Give it a
    // name outside the current schema so nested name-based casts cannot bind it.
    let name = if let Some(name) = id.and_then(|id| names.get(&id)) {
        Cow::Borrowed(name.as_str())
    } else {
        let mut name = format!("__databend_iceberg_unmapped_{}", field.name());
        while names.values().any(|target| target == &name) {
            name.push('_');
        }
        Cow::Owned(name)
    };
    let children = mapped.and_then(|m| m.fields.as_deref()).unwrap_or_default();
    let repetition = info.repetition();
    let resolved = match field {
        Type::PrimitiveType {
            physical_type,
            type_length,
            scale,
            precision,
            ..
        } => Type::primitive_type_builder(&name, *physical_type)
            .with_repetition(repetition)
            .with_converted_type(info.converted_type())
            .with_logical_type(info.logical_type())
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
                        // The repeated group in the standard three-level encoding
                        // isn't a field. Legacy two-level encodings have no wrapper.
                        let is_wrapper = matches!(element.as_ref(), Type::GroupType { fields, .. }
                        if fields.len() == 1 && element.name() != "array"
                            && element.name() != format!("{}_tuple", field.name()));
                        if is_wrapper {
                            let child = resolve_field(
                                &element.get_fields()[0],
                                children,
                                names,
                                ids,
                                Some("element"),
                            )?;
                            Ok(Arc::new(
                                Type::group_type_builder(element.name())
                                    .with_repetition(element.get_basic_info().repetition())
                                    .with_fields(vec![child])
                                    .build()?,
                            ))
                        } else {
                            resolve_field(element, children, names, ids, Some("element"))
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
                            .map(|child| resolve_field(child, children, names, ids, None))
                            .collect::<Result<Vec<_>>>()?;
                        Ok(Arc::new(
                            Type::group_type_builder(wrapper.name())
                                .with_repetition(wrapper_info.repetition())
                                .with_converted_type(wrapper_info.converted_type())
                                .with_logical_type(wrapper_info.logical_type())
                                .with_fields(fields)
                                .build()?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                fields
                    .iter()
                    .map(|child| resolve_field(child, children, names, ids, None))
                    .collect::<Result<Vec<_>>>()?
            };
            Type::group_type_builder(&name)
                .with_repetition(repetition)
                .with_converted_type(info.converted_type())
                .with_logical_type(info.logical_type())
                .with_fields(fields)
                .with_id(id)
                .build()?
        }
    };
    Ok(Arc::new(resolved))
}

pub fn target_field_id(schema: &Schema, path: &str) -> Result<i32> {
    field_id(target_field(schema, path)?)?.ok_or_else(|| {
        ErrorCode::TableSchemaMismatch(format!("Missing Iceberg field ID for {path}"))
    })
}

pub fn target_field<'a>(schema: &'a Schema, path: &str) -> Result<&'a Field> {
    let mut fields = schema.fields();
    let mut segments = path.split(':').peekable();
    while let Some(segment) = segments.next() {
        let field = fields
            .iter()
            .find(|field| field.name() == segment)
            .ok_or_else(|| {
                ErrorCode::TableSchemaMismatch(format!("Unknown Iceberg field path {path}"))
            })?;
        if segments.peek().is_none() {
            return Ok(field);
        }
        fields = match field.data_type() {
            DataType::Struct(fields) => fields,
            _ => {
                return Err(ErrorCode::TableSchemaMismatch(format!(
                    "Invalid Iceberg field path {path}"
                )));
            }
        };
    }
    Err(ErrorCode::TableSchemaMismatch("Empty Iceberg field path"))
}

/// Map the requested table fields to file positions by ID, not table ordinals.
/// For other Parquet users the existing ordinal projection is unchanged.
pub fn project_schema(
    projection: &Projection,
    table: &TableSchema,
    file: &SchemaDescriptor,
) -> Result<(ProjectionMask, Vec<usize>)> {
    let Some(target) = iceberg_arrow_schema(table)? else {
        return Ok(projection.to_arrow_projection(file));
    };
    let output = projection.project_schema(table);
    let mut leaves = Vec::new();
    for field in output.fields() {
        let id = target_field_id(&target, field.name())?;
        let node = find_field(file.root_schema(), id).ok_or_else(|| {
            ErrorCode::TableSchemaMismatch(format!(
                "Iceberg field {} (ID {id}) does not exist in the Parquet file",
                field.name()
            ))
        })?;
        let mut field_leaves = Vec::new();
        collect_leaf_ids(node, &mut field_leaves);
        leaves.extend((0..file.num_columns()).filter(|&idx| {
            let column = file.column(idx);
            let info = column.self_type().get_basic_info();
            info.has_id() && field_leaves.contains(&info.id())
        }));
    }
    leaves.sort_unstable();
    leaves.dedup();
    Ok((ProjectionMask::leaves(file, leaves.clone()), leaves))
}

fn find_field(field: &Type, id: i32) -> Option<&Type> {
    let info = field.get_basic_info();
    if info.has_id() && info.id() == id {
        return Some(field);
    }
    if let Type::GroupType { fields, .. } = field {
        return fields.iter().find_map(|field| find_field(field, id));
    }
    None
}

/// Nested names have already been resolved from Iceberg IDs. Align recursively
/// instead of relying on Arrow's positional fallback when a struct child is absent.
pub(crate) fn align_array(array: ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(array);
    }
    match target {
        DataType::Struct(fields) => {
            let source = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ErrorCode::TableSchemaMismatch("Expected Iceberg struct array"))?;
            let columns = fields
                .iter()
                .map(|field| {
                    if let Some(column) = source.column_by_name(field.name()) {
                        align_array(column.clone(), field.data_type())
                    } else if field.is_nullable() {
                        Ok(new_null_array(field.data_type(), array.len()))
                    } else {
                        Err(ErrorCode::TableSchemaMismatch(format!(
                            "Missing required Iceberg field {}",
                            field.name()
                        )))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                source.nulls().cloned(),
            )?))
        }
        DataType::LargeList(element) => {
            let source_type = match array.data_type() {
                DataType::List(field) | DataType::LargeList(field) => {
                    DataType::LargeList(field.clone())
                }
                _ => {
                    return Err(ErrorCode::TableSchemaMismatch(
                        "Expected Iceberg list array",
                    ));
                }
            };
            let array = cast(array.as_ref(), &source_type)?;
            let source = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let values = align_array(source.values().clone(), element.data_type())?;
            Ok(Arc::new(LargeListArray::try_new(
                element.clone(),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        DataType::Map(entries, ordered) => {
            let source = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| ErrorCode::TableSchemaMismatch("Expected Iceberg map array"))?;
            let values = align_array(Arc::new(source.entries().clone()), entries.data_type())?;
            let values = values
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .clone();
            Ok(Arc::new(MapArray::try_new(
                entries.clone(),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
                *ordered,
            )?))
        }
        _ => Ok(cast(array.as_ref(), target)?),
    }
}

fn collect_leaf_ids(field: &Type, ids: &mut Vec<i32>) {
    match field {
        Type::PrimitiveType { .. } => {
            let info = field.get_basic_info();
            if info.has_id() {
                ids.push(info.id());
            }
        }
        Type::GroupType { fields, .. } => {
            for field in fields {
                collect_leaf_ids(field, ids);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use arrow_array::RecordBatch;
    use arrow_array::StructArray;
    use arrow_buffer::NullBuffer;
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::schema::parser::parse_message_type;

    use super::*;
    use crate::parquet_reader::utils::compute_output_field_paths;
    use crate::parquet_reader::utils::transform_record_batch;

    fn field(name: &str, ty: DataType, id: i32) -> Field {
        Field::new(name, ty, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    fn table(target: Schema, mapping: &str) -> TableSchema {
        let mut table = TableSchema::try_from(&target).unwrap();
        table.metadata.insert(
            ICEBERG_ARROW_SCHEMA.to_string(),
            serde_json::to_string(&target).unwrap(),
        );
        table
            .metadata
            .insert(ICEBERG_NAME_MAPPING.to_string(), mapping.to_string());
        table
    }

    fn parquet(schema: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(schema).unwrap()))
    }

    #[test]
    fn name_mapping_preserves_decimal_physical_schema() {
        let table = table(
            Schema::new(vec![field("amount", DataType::Decimal128(15, 2), 2)]),
            r#"[{"field-id":2,"names":["d","old_amount","amount"]}]"#,
        );
        let file = parquet("message test { OPTIONAL FIXED_LEN_BYTE_ARRAY(7) d (DECIMAL(15,2)); }");
        let resolved = resolve_iceberg_schema(&file, &table).unwrap();
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
        assert_eq!(column.max_def_level(), file.column(0).max_def_level());
    }

    #[test]
    fn field_ids_override_names_and_projection_uses_file_positions() {
        let table = table(
            Schema::new(vec![
                field("a", DataType::Int64, 1),
                field("b", DataType::Int64, 2),
            ]),
            r#"[{"field-id":1,"names":["a"]},{"field-id":2,"names":["b"]}]"#,
        );
        // Same types, opposite identities. A name/position shortcut reads the wrong values.
        let file = parquet("message test { OPTIONAL INT64 a = 2; OPTIONAL INT64 b = 1; }");
        let resolved = resolve_iceberg_schema(&file, &table).unwrap();
        assert_eq!(resolved.column(0).name(), "b");
        assert_eq!(resolved.column(1).name(), "a");
        let (_, leaves) = project_schema(&Projection::Columns(vec![0]), &table, &resolved).unwrap();
        assert_eq!(leaves, vec![1]);
        let batch = RecordBatch::try_new(
            Arc::new(parquet_to_arrow_schema(&resolved, None).unwrap()),
            vec![
                Arc::new(Int64Array::from(vec![20])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .unwrap();
        let block = transform_record_batch(&scan_data_schema(&table), &batch, &None).unwrap();
        assert_eq!(
            block.get_by_offset(0).value().index(0).unwrap().to_owned(),
            databend_common_expression::Scalar::Number(
                databend_common_expression::types::number::NumberScalar::Int64(10)
            )
        );
        assert_eq!(
            block.get_by_offset(1).value().index(0).unwrap().to_owned(),
            databend_common_expression::Scalar::Number(
                databend_common_expression::types::number::NumberScalar::Int64(20)
            )
        );
    }

    #[test]
    fn nested_aliases_are_scoped_to_their_parent_and_keep_parent_nulls() {
        let target = Schema::new(vec![
            field(
                "left",
                DataType::Struct(vec![field("value", DataType::Int64, 30)].into()),
                10,
            ),
            field(
                "right",
                DataType::Struct(vec![field("value", DataType::Int64, 40)].into()),
                20,
            ),
        ]);
        let table = table(
            target,
            r#"[
            {"field-id":10,"names":["old_left","left"],"fields":[{"field-id":30,"names":["old_value","value"]}]},
            {"field-id":20,"names":["old_right","right"],"fields":[{"field-id":40,"names":["old_value","value"]}]}
        ]"#,
        );
        let file = parquet(
            "message test {
            OPTIONAL group old_right { OPTIONAL INT64 old_value; }
            OPTIONAL group old_left { OPTIONAL INT64 old_value; }
        }",
        );
        let resolved = resolve_iceberg_schema(&file, &table).unwrap();
        assert_eq!(resolved.column(0).self_type().get_basic_info().id(), 40);
        assert_eq!(resolved.column(1).self_type().get_basic_info().id(), 30);
        let projection =
            Projection::InnerColumns(std::collections::BTreeMap::from([(0, vec![0, 0])]));
        let (mask, leaves) = project_schema(&projection, &table, &resolved).unwrap();
        assert_eq!(leaves, vec![1]);
        let projected = projection.project_schema(&table);
        let paths = compute_output_field_paths(&resolved, &mask, &projected, true).unwrap();
        let arrow = parquet_to_arrow_schema(&resolved, None).unwrap();
        let DataType::Struct(children) = arrow.field(1).data_type() else {
            panic!()
        };
        let array = StructArray::new(
            children.clone(),
            vec![Arc::new(Int64Array::from(vec![5, 99]))],
            Some(NullBuffer::from(vec![true, false])),
        );
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![arrow.field(1).clone()])), vec![
                Arc::new(array),
            ])
            .unwrap();
        let block = transform_record_batch(&scan_data_schema(&projected), &batch, &paths).unwrap();
        assert_eq!(block.num_rows(), 2);
        assert_eq!(
            block.get_by_offset(0).value().index(1).unwrap().to_owned(),
            databend_common_expression::Scalar::Null
        );
        // A full struct's child names must come from Iceberg, not DataType::Tuple
        // (whose conversion back to Arrow would invent positional child names).
        let whole = table.project(&[0]);
        let block = transform_record_batch(&scan_data_schema(&whole), &batch, &None).unwrap();
        assert_eq!(
            block.get_by_offset(0).value().index(0).unwrap().to_owned(),
            databend_common_expression::Scalar::Tuple(vec![
                databend_common_expression::Scalar::Number(
                    databend_common_expression::types::number::NumberScalar::Int64(5),
                )
            ]),
        );
        assert_eq!(
            block.get_by_offset(0).value().index(1).unwrap().to_owned(),
            databend_common_expression::Scalar::Null
        );
    }

    #[test]
    fn list_and_map_wrappers_do_not_consume_mapping_levels() {
        let target = Schema::new(vec![
            field(
                "items",
                DataType::List(Arc::new(field("element", DataType::Int64, 11))),
                1,
            ),
            field(
                "attributes",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                field("key", DataType::Int64, 21).with_nullable(false),
                                field("value", DataType::Int64, 22),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                2,
            ),
        ]);
        let table = table(
            target,
            r#"[
            {"field-id":1,"names":["old_items"],"fields":[{"field-id":11,"names":["element"]}]},
            {"field-id":2,"names":["old_attributes"],"fields":[{"field-id":21,"names":["key"]},{"field-id":22,"names":["value"]}]}
        ]"#,
        );
        let file = parquet("message test {
            OPTIONAL group old_items (LIST) { REPEATED group list { OPTIONAL INT64 item; } }
            OPTIONAL group old_attributes (MAP) { REPEATED group key_value { REQUIRED INT64 key; OPTIONAL INT64 value; } }
        }");
        let resolved = resolve_iceberg_schema(&file, &table).unwrap();
        assert_eq!(
            (0..3)
                .map(|i| resolved.column(i).self_type().get_basic_info().id())
                .collect::<Vec<_>>(),
            vec![11, 21, 22]
        );
        for i in 0..3 {
            assert_eq!(
                resolved.column(i).max_def_level(),
                file.column(i).max_def_level()
            );
            assert_eq!(
                resolved.column(i).max_rep_level(),
                file.column(i).max_rep_level()
            );
        }
        let (_, leaves) = project_schema(&Projection::Columns(vec![1]), &table, &resolved).unwrap();
        assert_eq!(leaves, vec![1, 2]);
    }

    #[test]
    fn whole_struct_alignment_reorders_children_and_never_uses_positional_fallback() {
        let source_fields = vec![
            Field::new("b", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
        ];
        let source = StructArray::new(
            source_fields.into(),
            vec![
                Arc::new(Int64Array::from(vec![20])),
                Arc::new(Int64Array::from(vec![10])),
            ],
            None,
        );
        let target = DataType::Struct(
            vec![
                Field::new("a", DataType::Int64, true),
                Field::new("new_child", DataType::Int64, true),
                Field::new("b", DataType::Int64, true),
            ]
            .into(),
        );
        let array = align_array(Arc::new(source.clone()), &target).unwrap();
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            array
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(array.column(1).null_count(), 1);
        assert_eq!(
            array
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
        let required =
            DataType::Struct(vec![Field::new("new_child", DataType::Int64, false)].into());
        assert!(align_array(Arc::new(source), &required).is_err());
    }

    #[test]
    fn missing_and_invalid_mapping_never_guess_current_names() {
        let mut table = table(Schema::new(vec![field("a", DataType::Int64, 1)]), "[]");
        let file = parquet("message test { OPTIONAL INT64 a; }");
        let resolved = resolve_iceberg_schema(&file, &table).unwrap();
        assert!(project_schema(&Projection::Columns(vec![0]), &table, &resolved).is_err());
        table
            .metadata
            .insert(ICEBERG_NAME_MAPPING.to_string(), "not json".to_string());
        assert!(resolve_iceberg_schema(&file, &table).is_err());
        table.metadata.insert(
            ICEBERG_NAME_MAPPING.to_string(),
            r#"[{"field-id":1,"names":["a"]},{"field-id":2,"names":["a"]}]"#.to_string(),
        );
        assert!(resolve_iceberg_schema(&file, &table).is_err());
    }

    #[test]
    fn unmapped_fields_and_no_mapping_with_explicit_ids_are_supported() {
        let mut table = table(
            Schema::new(vec![field("renamed", DataType::Int64, 1)]),
            "[]",
        );
        table.metadata.remove(ICEBERG_NAME_MAPPING);
        let file = parquet("message test { OPTIONAL INT64 deleted; OPTIONAL INT64 old = 1; }");
        let resolved = resolve_iceberg_schema(&file, &table).unwrap();
        let (_, leaves) = project_schema(&Projection::Columns(vec![0]), &table, &resolved).unwrap();
        assert_eq!(leaves, vec![1]);
    }
}
