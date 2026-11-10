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

use std::sync::Arc;

use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use databend_common_catalog::plan::Projection;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use parquet::schema::types::Type;

/// Format-specific identity resolution is supplied by the table engine. The
/// Parquet source does not interpret table properties or manufacture field IDs.
pub trait ParquetSchemaResolver: Send + Sync {
    fn resolve(&self, file: &SchemaDescriptor) -> Result<SchemaDescriptor>;
    fn read_schema(&self) -> Arc<ParquetReadSchema>;
}

/// An explicit logical schema for readers that match columns by stable file IDs.
/// Ordinary Parquet reads retain their existing projection/name matching rules.
/// IDs here belong to the file format, not TableSchema's internal column IDs.
pub struct ParquetReadSchema {
    schema: Schema,
}

impl ParquetReadSchema {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }

    pub fn field_id(&self, path: &str) -> Result<i32> {
        parquet_field_id(self.field(path)?)?.ok_or_else(|| {
            ErrorCode::TableSchemaMismatch(format!("Missing file field ID for {path}"))
        })
    }

    /// Keep the requested runtime types and tuple names while restoring only
    /// file-format IDs from the explicit logical schema, including nested IDs.
    pub(crate) fn field_for_read(&self, field: &TableField) -> Result<Field> {
        with_field_ids(&Field::from(field), self.field(field.name())?)
    }

    fn field(&self, path: &str) -> Result<&Field> {
        fn visit<'a>(
            field: &'a Field,
            prefix: &str,
            path: &str,
            found: &mut Option<&'a Field>,
        ) -> Result<()> {
            if prefix == path {
                if found.is_some() {
                    return Err(ErrorCode::TableSchemaMismatch(format!(
                        "Ambiguous field path {path}"
                    )));
                }
                *found = Some(field);
                return Ok(());
            }
            if !path
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with(':'))
            {
                return Ok(());
            }
            if let DataType::Struct(fields) = field.data_type() {
                for child in fields {
                    visit(child, &format!("{prefix}:{}", child.name()), path, found)?;
                }
            }
            Ok(())
        }
        let mut found = None;
        for field in self.schema.fields() {
            visit(field, field.name(), path, &mut found)?;
        }
        found.ok_or_else(|| ErrorCode::TableSchemaMismatch(format!("Unknown field path {path}")))
    }

    pub fn project(
        &self,
        projection: &Projection,
        table: &TableSchema,
        file: &SchemaDescriptor,
    ) -> Result<(ProjectionMask, Vec<usize>)> {
        let output = projection.project_schema(table);
        let mut leaves = Vec::new();
        for field in output.fields() {
            let id = self.field_id(field.name())?;
            let node = find_field(file.root_schema(), id).ok_or_else(|| {
                ErrorCode::TableSchemaMismatch(format!(
                    "Field {} (file ID {id}) does not exist in the Parquet file",
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
}

fn with_field_ids(target: &Field, logical: &Field) -> Result<Field> {
    let data_type = match (target.data_type(), logical.data_type()) {
        (DataType::Struct(target), DataType::Struct(logical)) => {
            let fields = target
                .iter()
                .map(|target| {
                    let logical = logical
                        .iter()
                        .find(|field| field.name() == target.name())
                        .ok_or_else(|| {
                            ErrorCode::TableSchemaMismatch(format!(
                                "Unknown logical child {}",
                                target.name()
                            ))
                        })?;
                    Ok(Arc::new(with_field_ids(target, logical)?))
                })
                .collect::<Result<Vec<FieldRef>>>()?;
            DataType::Struct(fields.into())
        }
        (DataType::LargeList(target), DataType::List(logical) | DataType::LargeList(logical)) => {
            DataType::LargeList(Arc::new(with_field_ids(target, logical)?))
        }
        (DataType::Map(target, ordered), DataType::Map(logical, _)) => {
            DataType::Map(Arc::new(with_field_ids(target, logical)?), *ordered)
        }
        _ => target.data_type().clone(),
    };
    let mut metadata = target.metadata().clone();
    metadata.remove(PARQUET_FIELD_ID_META_KEY);
    if let Some(id) = logical.metadata().get(PARQUET_FIELD_ID_META_KEY) {
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.clone());
    }
    Ok(target
        .clone()
        .with_data_type(data_type)
        .with_metadata(metadata))
}

pub(crate) fn project_schema(
    projection: &Projection,
    table: &TableSchema,
    file: &SchemaDescriptor,
    read_schema: Option<&ParquetReadSchema>,
) -> Result<(ProjectionMask, Vec<usize>)> {
    match read_schema {
        Some(schema) => schema.project(projection, table, file),
        None => Ok(projection.to_arrow_projection(file)),
    }
}

/// Message/root names are not column identity and do not affect decoding. All
/// field attributes (IDs, order, physical encodings and nesting) must still match.
pub(crate) fn same_read_schema(left: &SchemaDescriptor, right: &SchemaDescriptor) -> bool {
    left.root_schema().get_fields() == right.root_schema().get_fields()
}

pub(crate) fn parquet_field_id(field: &Field) -> Result<Option<i32>> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .map(|id| {
            id.parse().map_err(|e| {
                ErrorCode::TableSchemaMismatch(format!("Invalid Parquet field ID {id}: {e}"))
            })
        })
        .transpose()
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
    use std::collections::HashMap;

    use super::*;

    fn field(name: &str, ty: DataType, id: i32) -> Field {
        Field::new(name, ty, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    #[test]
    fn identity_paths_respect_literal_colons_at_each_schema_boundary() {
        let schema = ParquetReadSchema::new(Schema::new(vec![
            field("a:b", DataType::Int64, 1),
            field(
                "parent:x",
                DataType::Struct(vec![field("child:y", DataType::Int64, 3)].into()),
                2,
            ),
        ]));
        assert_eq!(schema.field_id("a:b").unwrap(), 1);
        assert_eq!(schema.field_id("parent:x:child:y").unwrap(), 3);
        let ambiguous = ParquetReadSchema::new(Schema::new(vec![
            field("a:b", DataType::Int64, 1),
            field(
                "a",
                DataType::Struct(vec![field("b", DataType::Int64, 3)].into()),
                2,
            ),
        ]));
        assert!(
            ambiguous
                .field_id("a:b")
                .unwrap_err()
                .message()
                .contains("Ambiguous")
        );
    }

    #[test]
    fn reader_reuse_ignores_only_message_names_not_file_encodings_or_ids() {
        use parquet::schema::parser::parse_message_type;
        let descriptor =
            |message: &str| SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()));
        let expected = descriptor(
            "message expected { OPTIONAL INT64 k = 1; OPTIONAL FIXED_LEN_BYTE_ARRAY(7) d (DECIMAL(15,2)) = 2; }",
        );
        let same = descriptor(
            "message file { OPTIONAL INT64 k = 1; OPTIONAL FIXED_LEN_BYTE_ARRAY(7) d (DECIMAL(15,2)) = 2; }",
        );
        assert!(same_read_schema(&expected, &same));
        let other_encoding = descriptor(
            "message file { OPTIONAL INT64 k = 1; OPTIONAL INT64 d (DECIMAL(15,2)) = 2; }",
        );
        assert!(!same_read_schema(&expected, &other_encoding));
        let other_id = descriptor(
            "message file { OPTIONAL INT64 k = 3; OPTIONAL FIXED_LEN_BYTE_ARRAY(7) d (DECIMAL(15,2)) = 2; }",
        );
        assert!(!same_read_schema(&expected, &other_id));
        let other_order = descriptor(
            "message file { OPTIONAL FIXED_LEN_BYTE_ARRAY(7) d (DECIMAL(15,2)) = 2; OPTIONAL INT64 k = 1; }",
        );
        assert!(!same_read_schema(&expected, &other_order));
    }

    #[test]
    fn target_projection_restores_nested_file_ids_without_using_column_ordinals() {
        let logical = Schema::new(vec![
            field(
                "item",
                DataType::Struct(vec![field("value", DataType::Int64, 30)].into()),
                10,
            ),
            field(
                "list",
                DataType::List(Arc::new(field("element", DataType::Int64, 50))),
                40,
            ),
        ]);
        let table = TableSchema::try_from(&logical).unwrap();
        let schema = ParquetReadSchema::new(logical);
        let item = schema.field_for_read(&table.fields()[0]).unwrap();
        assert_eq!(parquet_field_id(&item).unwrap(), Some(10));
        let DataType::Struct(children) = item.data_type() else {
            panic!()
        };
        assert_eq!(parquet_field_id(&children[0]).unwrap(), Some(30));
        let list = schema.field_for_read(&table.fields()[1]).unwrap();
        let DataType::LargeList(element) = list.data_type() else {
            panic!()
        };
        assert_eq!(parquet_field_id(element).unwrap(), Some(50));
        assert_eq!(element.data_type(), &DataType::Int64);
        let nested = table.inner_project(&std::collections::BTreeMap::from([(0, vec![0, 0])]));
        assert_eq!(
            parquet_field_id(&schema.field_for_read(&nested.fields()[0]).unwrap()).unwrap(),
            Some(30)
        );
    }
}
