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
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::LargeListArray;
use arrow_array::MapArray;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_buffer::NullBuffer;
use arrow_buffer::NullBufferBuilder;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::Fields;
use arrow_schema::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::VariantDataType;
use databend_common_expression::converts::arrow::ARROW_EXT_TYPE_VARIANT;
use databend_common_expression::converts::arrow::EXTENSION_KEY;
use databend_common_expression::types::variant_parquet::jsonb_value_to_parquet_variant_parts_with_field_names;
use jsonb::Value as JsonbValue;
use jsonb::keypath::KeyPath as JsonbKeyPath;
use jsonb::keypath::KeyPaths as JsonbKeyPaths;

const VARIANT_VALUE_COLUMN_ID_MASK: ColumnId = 1 << 31;

#[derive(Debug, Clone)]
pub struct VariantShreddedColumn {
    pub source_column_id: ColumnId,
    pub column_id: ColumnId,
    pub key_paths: OwnedKeyPaths,
    pub data_type: ArrowDataType,
}

/// Represents a set of key path chains.
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct OwnedKeyPaths {
    pub paths: Vec<OwnedKeyPath>,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum OwnedKeyPath {
    /// represents the index of an Array.
    Index(i32),
    /// represents the field name of an Object.
    Name(String),
}

impl OwnedKeyPath {
    pub(crate) fn to_borrowed_key_path(&self) -> JsonbKeyPath<'_> {
        match self {
            OwnedKeyPath::Index(idx) => JsonbKeyPath::Index(*idx),
            OwnedKeyPath::Name(name) => JsonbKeyPath::Name(Cow::Borrowed(name.as_str())),
        }
    }
}

impl OwnedKeyPaths {
    pub(crate) fn to_borrowed_key_paths(&self) -> JsonbKeyPaths<'_> {
        let paths = self
            .paths
            .iter()
            .map(|path| path.to_borrowed_key_path())
            .collect::<Vec<_>>();
        JsonbKeyPaths { paths }
    }

    pub(crate) fn is_prefix_path(&self, other: &OwnedKeyPaths) -> bool {
        if self.paths.len() >= other.paths.len() {
            return false;
        }
        for (self_path, other_path) in self.paths.iter().zip(other.paths.iter()) {
            if !self_path.eq(other_path) {
                return false;
            }
        }
        true
    }

    fn object_segments(&self) -> Option<Vec<String>> {
        if self.paths.is_empty() {
            return None;
        }
        let mut segments = Vec::with_capacity(self.paths.len());
        for path in &self.paths {
            match path {
                OwnedKeyPath::Name(name) => segments.push(name.clone()),
                OwnedKeyPath::Index(idx) => segments.push(idx.to_string()),
            }
        }
        Some(segments)
    }
}

pub fn variant_value_column_id(column_id: ColumnId) -> ColumnId {
    column_id | VARIANT_VALUE_COLUMN_ID_MASK
}

pub fn is_variant_value_column_id(column_id: ColumnId) -> bool {
    (column_id & VARIANT_VALUE_COLUMN_ID_MASK) != 0
}

pub fn parquet_variant_leaf_column_ids(schema: &TableSchema) -> Vec<ColumnId> {
    parquet_variant_leaf_column_ids_with_shredding(schema, None)
}

pub fn parquet_variant_leaf_column_ids_with_shredding(
    schema: &TableSchema,
    shredded_columns: Option<&[VariantShreddedColumn]>,
) -> Vec<ColumnId> {
    let typed_value_trees = shredded_columns
        .map(build_typed_value_trees)
        .unwrap_or_default();
    let mut ids = Vec::new();
    for field in schema.fields() {
        if field.data_type().remove_nullable() == TableDataType::Variant {
            ids.push(field.column_id());
            ids.push(variant_value_column_id(field.column_id()));
            if let Some(tree) = typed_value_trees.get(&field.column_id()) {
                append_typed_value_leaf_column_ids(tree, &mut ids);
            }
        } else {
            ids.extend(field.leaf_column_ids());
        }
    }
    ids
}

pub fn arrow_schema_with_parquet_variant(schema: &TableSchema) -> Schema {
    arrow_schema_with_parquet_variant_and_shredding(schema, None)
}

pub fn arrow_schema_with_parquet_variant_and_shredding(
    schema: &TableSchema,
    shredded_columns: Option<&[VariantShreddedColumn]>,
) -> Schema {
    let typed_value_trees = shredded_columns
        .map(build_typed_value_trees)
        .unwrap_or_default();
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if field.data_type().remove_nullable() == TableDataType::Variant {
                let arrow_field = Field::from(field);
                let mut metadata = arrow_field.metadata().clone();
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_VARIANT.to_string(),
                );
                let mut struct_fields = vec![
                    Field::new("metadata", ArrowDataType::LargeBinary, false),
                    Field::new("value", ArrowDataType::LargeBinary, true),
                ];
                if let Some(tree) = typed_value_trees.get(&field.column_id()) {
                    if !tree.children.is_empty() {
                        let typed_value_fields = build_typed_value_fields(tree);
                        if !typed_value_fields.is_empty() {
                            struct_fields.push(Field::new(
                                "typed_value",
                                ArrowDataType::Struct(typed_value_fields),
                                true,
                            ));
                        }
                    }
                }
                let struct_fields = Fields::from(struct_fields);
                Field::new(
                    field.name(),
                    ArrowDataType::Struct(struct_fields),
                    field.is_nullable_or_null(),
                )
                .with_metadata(metadata)
            } else {
                Field::from(field)
            }
        })
        .collect::<Vec<_>>();

    let metadata = schema
        .metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    Schema::new(fields).with_metadata(metadata)
}

pub fn build_parquet_variant_record_batch(
    schema: &TableSchema,
    block: DataBlock,
) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(arrow_schema_with_parquet_variant(schema));
    build_parquet_variant_record_batch_with_arrow_schema(schema, &arrow_schema, block)
}

pub fn build_parquet_variant_record_batch_with_arrow_schema(
    schema: &TableSchema,
    arrow_schema: &Arc<Schema>,
    block: DataBlock,
) -> Result<RecordBatch> {
    if block.columns().len() != schema.num_fields() {
        return Err(ErrorCode::Internal(format!(
            "The number of columns in the data block does not match the number of fields in the table schema, block_columns: {}, table_schema_fields: {}",
            block.columns().len(),
            schema.num_fields()
        )));
    }

    if schema.num_fields() == 0 {
        return Ok(RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow_array::RecordBatchOptions::default().with_row_count(Some(block.num_rows())),
        )?);
    }

    let mut arrays = Vec::with_capacity(block.columns().len());
    let mut entries = block.take_columns();
    for ((entry, field), arrow_field) in entries
        .drain(..)
        .zip(schema.fields())
        .zip(arrow_schema.fields())
    {
        let array = if field.data_type().remove_nullable() == TableDataType::Variant
            && matches!(arrow_field.data_type(), ArrowDataType::Struct(_))
        {
            let column = entry.to_column().maybe_gc();
            let struct_array = build_parquet_variant_struct_array(column)?;
            Arc::new(struct_array) as ArrayRef
        } else {
            let array = entry.to_column().maybe_gc().into_arrow_rs();
            adjust_nested_array(array, arrow_field.as_ref())
        };
        arrays.push(array);
    }

    Ok(RecordBatch::try_new(arrow_schema.clone(), arrays)?)
}

pub fn build_parquet_variant_record_batch_with_inline_shredding(
    schema: &TableSchema,
    arrow_schema: &Arc<Schema>,
    block: DataBlock,
    shredded_columns: &[VariantShreddedColumn],
    inline_virtual_block: &DataBlock,
) -> Result<(RecordBatch, HashSet<ColumnId>)> {
    if block.columns().len() != schema.num_fields() {
        return Err(ErrorCode::Internal(format!(
            "The number of columns in the data block does not match the number of fields in the table schema, block_columns: {}, table_schema_fields: {}",
            block.columns().len(),
            schema.num_fields()
        )));
    }

    if schema.num_fields() == 0 {
        return Ok((
            RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &arrow_array::RecordBatchOptions::default().with_row_count(Some(block.num_rows())),
            )?,
            HashSet::new(),
        ));
    }

    let mut virtual_arrays = HashMap::with_capacity(inline_virtual_block.num_columns());
    for (idx, entry) in inline_virtual_block.columns().iter().enumerate() {
        let array = entry.to_column().maybe_gc().into_arrow_rs();
        let column_id = databend_common_expression::VIRTUAL_COLUMN_ID_START + idx as u32;
        virtual_arrays.insert(column_id, array);
    }

    let typed_value_arrays =
        build_typed_value_arrays_by_source(shredded_columns, &virtual_arrays, block.num_rows())?;

    let mut arrays = Vec::with_capacity(block.columns().len());
    let mut value_all_null_sources = HashSet::new();
    let mut entries = block.take_columns();
    for ((entry, field), arrow_field) in entries
        .drain(..)
        .zip(schema.fields())
        .zip(arrow_schema.fields())
    {
        let array = if field.data_type().remove_nullable() == TableDataType::Variant
            && matches!(arrow_field.data_type(), ArrowDataType::Struct(_))
        {
            let column = entry.to_column().maybe_gc();
            let typed_value = typed_value_arrays.get(&field.column_id()).cloned();
            let typed_value_present = typed_value.is_some();
            let (struct_array, value_all_null) =
                build_parquet_variant_struct_array_with_typed_value_stats(column, typed_value)?;
            if value_all_null && typed_value_present {
                value_all_null_sources.insert(field.column_id());
            }
            Arc::new(struct_array) as ArrayRef
        } else {
            let array = entry.to_column().maybe_gc().into_arrow_rs();
            adjust_nested_array(array, arrow_field.as_ref())
        };
        arrays.push(array);
    }

    Ok((
        RecordBatch::try_new(arrow_schema.clone(), arrays)?,
        value_all_null_sources,
    ))
}

fn build_parquet_variant_struct_array(
    column: databend_common_expression::Column,
) -> Result<StructArray> {
    build_parquet_variant_struct_array_with_typed_value(column, None)
}

fn build_parquet_variant_struct_array_with_typed_value(
    column: databend_common_expression::Column,
    typed_value: Option<ArrayRef>,
) -> Result<StructArray> {
    let (struct_array, _) =
        build_parquet_variant_struct_array_with_typed_value_stats(column, typed_value)?;
    Ok(struct_array)
}

fn build_parquet_variant_struct_array_with_typed_value_stats(
    column: databend_common_expression::Column,
    typed_value: Option<ArrayRef>,
) -> Result<(StructArray, bool)> {
    let len = column.len();
    let mut metadata_builder = LargeBinaryBuilder::with_capacity(len, 0);
    let mut value_builder = LargeBinaryBuilder::with_capacity(len, 0);
    let mut nulls = NullBufferBuilder::new(len);
    let typed_value_is_scalar = typed_value
        .as_ref()
        .map(|array| is_scalar_typed_value(array.data_type()))
        .unwrap_or(false);
    let mut value_all_null = true;

    match column {
        databend_common_expression::Column::Variant(col) => {
            let mut field_names = BTreeSet::new();
            let mut parsed_values = Vec::with_capacity(len);
            for value in col.iter() {
                let parsed = jsonb::from_slice(value).map_err(|e| {
                    ErrorCode::Internal(format!(
                        "Failed to decode JSONB value for variant encoding: {e}"
                    ))
                })?;
                collect_jsonb_field_names(&parsed, &mut field_names);
                parsed_values.push(parsed);
            }
            let field_names = field_names.into_iter().collect::<Vec<_>>();
            for (idx, parsed) in parsed_values.into_iter().enumerate() {
                let (metadata, variant_value) =
                    jsonb_value_to_parquet_variant_parts_with_field_names(&parsed, &field_names)?;
                metadata_builder.append_value(metadata);
                if should_omit_variant_value(
                    typed_value.as_ref(),
                    idx,
                    &parsed,
                    typed_value_is_scalar,
                ) {
                    value_builder.append_null();
                } else {
                    value_builder.append_value(variant_value);
                    value_all_null = false;
                }
                nulls.append_non_null();
            }
        }
        databend_common_expression::Column::Nullable(col) => {
            let (inner, validity) = col.destructure();
            let databend_common_expression::Column::Variant(col) = inner else {
                return Err(ErrorCode::Internal(
                    "Unexpected non-variant column for variant struct encoding".to_string(),
                ));
            };
            let mut field_names = BTreeSet::new();
            let mut parsed_values = Vec::with_capacity(len);
            for (idx, value) in col.iter().enumerate() {
                if !validity.get(idx).unwrap_or(false) {
                    parsed_values.push(None);
                    continue;
                }
                let parsed = jsonb::from_slice(value).map_err(|e| {
                    ErrorCode::Internal(format!(
                        "Failed to decode JSONB value for variant encoding: {e}"
                    ))
                })?;
                collect_jsonb_field_names(&parsed, &mut field_names);
                parsed_values.push(Some(parsed));
            }
            let field_names = field_names.into_iter().collect::<Vec<_>>();
            for (idx, parsed) in parsed_values.into_iter().enumerate() {
                match parsed {
                    Some(parsed) => {
                        let (metadata, variant_value) =
                            jsonb_value_to_parquet_variant_parts_with_field_names(
                                &parsed,
                                &field_names,
                            )?;
                        metadata_builder.append_value(metadata);
                        if should_omit_variant_value(
                            typed_value.as_ref(),
                            idx,
                            &parsed,
                            typed_value_is_scalar,
                        ) {
                            value_builder.append_null();
                        } else {
                            value_builder.append_value(variant_value);
                            value_all_null = false;
                        }
                        nulls.append_non_null();
                    }
                    None => {
                        metadata_builder.append_null();
                        value_builder.append_null();
                        nulls.append_null();
                    }
                }
            }
        }
        databend_common_expression::Column::Null { len } => {
            for _ in 0..len {
                metadata_builder.append_null();
                value_builder.append_null();
                nulls.append_null();
            }
        }
        other => {
            return Err(ErrorCode::Internal(format!(
                "Unexpected column type for variant struct encoding: {:?}",
                other.data_type()
            )));
        }
    }

    let mut struct_fields = vec![
        Field::new("metadata", ArrowDataType::LargeBinary, false),
        Field::new("value", ArrowDataType::LargeBinary, true),
    ];
    let metadata_array = Arc::new(metadata_builder.finish()) as ArrayRef;
    let value_array = Arc::new(value_builder.finish()) as ArrayRef;
    let mut struct_arrays = vec![metadata_array, value_array];
    if let Some(typed_value) = typed_value {
        struct_fields.push(Field::new(
            "typed_value",
            typed_value.data_type().clone(),
            true,
        ));
        struct_arrays.push(typed_value);
    }
    let struct_fields = Fields::from(struct_fields);
    Ok((
        StructArray::new(struct_fields, struct_arrays, nulls.finish()),
        value_all_null,
    ))
}

fn should_omit_variant_value(
    typed_value: Option<&ArrayRef>,
    row: usize,
    parsed: &JsonbValue<'_>,
    typed_value_is_scalar: bool,
) -> bool {
    // NOTE: We intentionally keep `value` even when typed_value is present.
    // Inline shredding stores typed_value columns in the same Parquet file, but the
    // base block reader currently reconstructs VARIANT from the `metadata` + `value`
    // fields only (typed_value is not loaded there). If we omit `value` for scalar
    // rows, full-variant reads and get_by_keypath fallbacks would return NULL.
    // TODO: Load typed_value into the base variant reader so we can safely omit `value`
    //       for fully shredded scalar rows (per Parquet VARIANT guidance) without
    //       breaking correctness.
    let _ = (typed_value, row, parsed, typed_value_is_scalar);
    false
}

fn is_scalar_typed_value(data_type: &ArrowDataType) -> bool {
    matches!(
        data_type,
        ArrowDataType::Boolean
            | ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64
            | ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64
            | ArrowDataType::Float16
            | ArrowDataType::Float32
            | ArrowDataType::Float64
            | ArrowDataType::Utf8
            | ArrowDataType::Utf8View
            | ArrowDataType::LargeUtf8
            | ArrowDataType::Binary
            | ArrowDataType::BinaryView
            | ArrowDataType::LargeBinary
    )
}

fn collect_jsonb_field_names(value: &JsonbValue<'_>, field_names: &mut BTreeSet<String>) {
    match value {
        JsonbValue::Array(values) => {
            for entry in values {
                collect_jsonb_field_names(entry, field_names);
            }
        }
        JsonbValue::Object(map) => {
            for (key, value) in map {
                field_names.insert(key.clone());
                collect_jsonb_field_names(value, field_names);
            }
        }
        _ => {}
    }
}

fn adjust_nested_array(array: ArrayRef, arrow_field: &Field) -> ArrayRef {
    match arrow_field.data_type() {
        ArrowDataType::Struct(fields) => {
            let array = array.as_struct();
            let inner_arrays = array
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(array, field)| adjust_nested_array(array.clone(), field.as_ref()))
                .collect();

            let array = StructArray::new(fields.clone(), inner_arrays, array.nulls().cloned());
            Arc::new(array) as _
        }
        ArrowDataType::LargeList(field) => {
            let array = array.as_list::<i64>();
            let values = adjust_nested_array(array.values().clone(), field.as_ref());
            let array = LargeListArray::new(
                field.clone(),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            );
            Arc::new(array) as _
        }
        ArrowDataType::Map(field, ordered) => {
            let array = array.as_map();
            let entry = Arc::new(array.entries().clone()) as Arc<dyn Array>;
            let entry = adjust_nested_array(entry, field.as_ref());
            let array = MapArray::new(
                field.clone(),
                array.offsets().clone(),
                entry.as_struct().clone(),
                array.nulls().cloned(),
                *ordered,
            );
            Arc::new(array) as _
        }
        _ => array,
    }
}

fn build_typed_value_arrays_by_source(
    shredded_columns: &[VariantShreddedColumn],
    virtual_arrays: &HashMap<ColumnId, ArrayRef>,
    num_rows: usize,
) -> Result<HashMap<ColumnId, ArrayRef>> {
    let typed_value_trees = build_typed_value_trees(shredded_columns);
    let mut typed_value_arrays = HashMap::with_capacity(typed_value_trees.len());
    for (source_column_id, tree) in typed_value_trees {
        if tree.children.is_empty() {
            continue;
        }
        let (typed_value_struct, _) =
            build_typed_value_struct_array(&tree, virtual_arrays, num_rows)?;
        typed_value_arrays.insert(source_column_id, Arc::new(typed_value_struct) as ArrayRef);
    }
    Ok(typed_value_arrays)
}

#[derive(Debug, Default, Clone)]
struct TypedValueNode {
    children: BTreeMap<String, TypedValueNode>,
    column_id: Option<ColumnId>,
    data_type: Option<ArrowDataType>,
}

fn build_typed_value_trees(
    shredded_columns: &[VariantShreddedColumn],
) -> BTreeMap<ColumnId, TypedValueNode> {
    let mut roots: BTreeMap<ColumnId, TypedValueNode> = BTreeMap::new();
    for column in shredded_columns {
        let Some(segments) = column.key_paths.object_segments() else {
            continue;
        };
        if segments.is_empty() {
            continue;
        }
        let root = roots.entry(column.source_column_id).or_default();
        insert_typed_value_path(root, &segments, column.column_id, &column.data_type);
    }
    roots
}

fn insert_typed_value_path(
    node: &mut TypedValueNode,
    segments: &[String],
    column_id: ColumnId,
    data_type: &ArrowDataType,
) {
    if segments.is_empty() {
        node.column_id = Some(column_id);
        node.data_type = Some(data_type.clone());
        return;
    }
    let key = segments[0].clone();
    let child = node.children.entry(key).or_default();
    insert_typed_value_path(child, &segments[1..], column_id, data_type);
}

fn build_typed_value_fields(node: &TypedValueNode) -> Fields {
    let mut fields = Vec::with_capacity(node.children.len());
    for (name, child) in node.children.iter() {
        let field_type = build_shredded_field_type(child);
        fields.push(Field::new(name, field_type, true));
    }
    Fields::from(fields)
}

fn build_shredded_field_type(node: &TypedValueNode) -> ArrowDataType {
    let typed_value_type = if node.children.is_empty() {
        node.data_type.clone().unwrap_or(ArrowDataType::Null)
    } else {
        ArrowDataType::Struct(build_typed_value_fields(node))
    };
    ArrowDataType::Struct(Fields::from(vec![Field::new(
        "typed_value",
        typed_value_type,
        true,
    )]))
}

fn append_typed_value_leaf_column_ids(node: &TypedValueNode, ids: &mut Vec<ColumnId>) {
    if node.children.is_empty() {
        if let Some(column_id) = node.column_id {
            ids.push(column_id);
        }
        return;
    }
    for child in node.children.values() {
        append_typed_value_leaf_column_ids(child, ids);
    }
}

fn build_typed_value_struct_array(
    node: &TypedValueNode,
    virtual_arrays: &HashMap<ColumnId, ArrayRef>,
    num_rows: usize,
) -> Result<(StructArray, Option<NullBuffer>)> {
    let mut fields = Vec::with_capacity(node.children.len());
    let mut arrays = Vec::with_capacity(node.children.len());
    let mut child_nulls = Vec::with_capacity(node.children.len());

    for (name, child) in node.children.iter() {
        let (child_array, child_typed_nulls) =
            build_shredded_field_array(child, virtual_arrays, num_rows)?;
        fields.push(Field::new(name, child_array.data_type().clone(), true));
        arrays.push(child_array);
        child_nulls.push(child_typed_nulls);
    }

    let struct_nulls = compute_struct_nulls(num_rows, &child_nulls);
    let struct_array = StructArray::new(Fields::from(fields), arrays, struct_nulls.clone());
    Ok((struct_array, struct_nulls))
}

fn build_shredded_field_array(
    node: &TypedValueNode,
    virtual_arrays: &HashMap<ColumnId, ArrayRef>,
    num_rows: usize,
) -> Result<(ArrayRef, Option<NullBuffer>)> {
    let (typed_value_array, typed_value_nulls) = if node.children.is_empty() {
        let column_id = node.column_id.ok_or_else(|| {
            ErrorCode::Internal("Missing column id for typed_value leaf".to_string())
        })?;
        let array = virtual_arrays.get(&column_id).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Missing virtual column array for column_id {}",
                column_id
            ))
        })?;
        (array.clone(), array.nulls().cloned())
    } else {
        let (typed_value_struct, typed_value_nulls) =
            build_typed_value_struct_array(node, virtual_arrays, num_rows)?;
        (Arc::new(typed_value_struct) as ArrayRef, typed_value_nulls)
    };

    let field = Field::new("typed_value", typed_value_array.data_type().clone(), true);
    let struct_array = StructArray::new(Fields::from(vec![field]), vec![typed_value_array], None);
    Ok((Arc::new(struct_array) as ArrayRef, typed_value_nulls))
}

fn compute_struct_nulls(num_rows: usize, child_nulls: &[Option<NullBuffer>]) -> Option<NullBuffer> {
    if child_nulls.is_empty() {
        return Some(NullBuffer::from(vec![false; num_rows]));
    }

    let mut any_valid = vec![false; num_rows];
    for nulls in child_nulls {
        match nulls {
            None => {
                for value in any_valid.iter_mut() {
                    *value = true;
                }
                break;
            }
            Some(nulls) => {
                for (idx, value) in any_valid.iter_mut().enumerate().take(num_rows) {
                    if nulls.is_valid(idx) {
                        *value = true;
                    }
                }
            }
        }
    }

    if any_valid.iter().all(|value| *value) {
        None
    } else {
        Some(NullBuffer::from(any_valid))
    }
}

pub fn variant_data_type_to_arrow(data_type: &VariantDataType) -> Option<ArrowDataType> {
    match data_type {
        VariantDataType::Boolean => Some(ArrowDataType::Boolean),
        VariantDataType::UInt64 => Some(ArrowDataType::UInt64),
        VariantDataType::Int64 => Some(ArrowDataType::Int64),
        VariantDataType::Float64 => Some(ArrowDataType::Float64),
        VariantDataType::String => Some(ArrowDataType::Utf8View),
        VariantDataType::Jsonb => None,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow_array::Int64Array;
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::VIRTUAL_COLUMN_ID_START;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::VariantType;

    use super::*;

    #[test]
    fn test_inline_shredding_keeps_scalar_value_column() -> Result<()> {
        let value1 = jsonb::parse_value(b"11")
            .map_err(|e| ErrorCode::Internal(e.to_string()))?
            .to_vec();
        let value2 = jsonb::parse_value(b"22")
            .map_err(|e| ErrorCode::Internal(e.to_string()))?
            .to_vec();

        let column = Column::Variant(
            VariantType::from_data(vec![value1.clone(), value2.clone()])
                .into_variant()
                .expect("variant column"),
        );
        let typed_value = Arc::new(Int64Array::from(vec![Some(11_i64), Some(22_i64)])) as ArrayRef;

        let struct_array =
            build_parquet_variant_struct_array_with_typed_value(column, Some(typed_value))?;

        let value_array = struct_array
            .column_by_name("value")
            .ok_or_else(|| ErrorCode::Internal("missing value column".to_string()))?
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .ok_or_else(|| ErrorCode::Internal("value column is not LargeBinary".to_string()))?;

        assert!(!value_array.is_null(0));
        assert!(!value_array.is_null(1));

        let decoded = Column::from_arrow_rs(Arc::new(struct_array), &DataType::Variant)?;
        let Column::Variant(decoded) = decoded else {
            return Err(ErrorCode::Internal(
                "expected decoded variant column".to_string(),
            ));
        };

        let decoded1 =
            jsonb::from_slice(decoded.value(0)).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let decoded2 =
            jsonb::from_slice(decoded.value(1)).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let expected1 =
            jsonb::from_slice(value1.as_slice()).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let expected2 =
            jsonb::from_slice(value2.as_slice()).map_err(|e| ErrorCode::Internal(e.to_string()))?;

        assert_eq!(decoded1, expected1);
        assert_eq!(decoded2, expected2);
        Ok(())
    }

    #[test]
    fn test_inline_shredding_keeps_value_for_object_with_scalar_typed_value() -> Result<()> {
        let value = jsonb::parse_value(br#"{"a":1}"#)
            .map_err(|e| ErrorCode::Internal(e.to_string()))?
            .to_vec();

        let column = Column::Variant(
            VariantType::from_data(vec![value.clone()])
                .into_variant()
                .expect("variant column"),
        );
        let typed_value = Arc::new(Int64Array::from(vec![Some(1_i64)])) as ArrayRef;

        let (struct_array, value_all_null) =
            build_parquet_variant_struct_array_with_typed_value_stats(column, Some(typed_value))?;

        assert!(!value_all_null);
        let value_array = struct_array
            .column_by_name("value")
            .ok_or_else(|| ErrorCode::Internal("missing value column".to_string()))?
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .ok_or_else(|| ErrorCode::Internal("value column is not LargeBinary".to_string()))?;
        assert!(!value_array.is_null(0));
        Ok(())
    }

    #[test]
    fn test_inline_shredding_keeps_value_for_non_scalar_typed_value() -> Result<()> {
        let value = jsonb::parse_value(br#"{"a":1}"#)
            .map_err(|e| ErrorCode::Internal(e.to_string()))?
            .to_vec();

        let column = Column::Variant(
            VariantType::from_data(vec![value.clone()])
                .into_variant()
                .expect("variant column"),
        );
        let inner = Int64Array::from(vec![Some(1_i64)]);
        let typed_value_struct = StructArray::new(
            Fields::from(vec![Field::new("typed_value", ArrowDataType::Int64, true)]),
            vec![Arc::new(inner) as ArrayRef],
            None,
        );
        let typed_value = Arc::new(typed_value_struct) as ArrayRef;

        let (struct_array, value_all_null) =
            build_parquet_variant_struct_array_with_typed_value_stats(column, Some(typed_value))?;

        assert!(!value_all_null);
        let value_array = struct_array
            .column_by_name("value")
            .ok_or_else(|| ErrorCode::Internal("missing value column".to_string()))?
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .ok_or_else(|| ErrorCode::Internal("value column is not LargeBinary".to_string()))?;
        assert!(!value_array.is_null(0));
        Ok(())
    }

    #[test]
    fn test_parquet_variant_leaf_ids_with_shredding() -> Result<()> {
        let field = TableField::new_from_column_id("v", TableDataType::Variant, 1);
        let schema = TableSchema::new_from_column_ids(vec![field], BTreeMap::new(), 2);

        let shredded = VariantShreddedColumn {
            source_column_id: 1,
            column_id: VIRTUAL_COLUMN_ID_START,
            key_paths: OwnedKeyPaths {
                paths: vec![OwnedKeyPath::Name("a".to_string())],
            },
            data_type: ArrowDataType::Int64,
        };

        let ids = parquet_variant_leaf_column_ids_with_shredding(&schema, Some(&[shredded]));

        assert!(ids.contains(&1));
        assert!(ids.contains(&variant_value_column_id(1)));
        assert!(ids.contains(&VIRTUAL_COLUMN_ID_START));
        Ok(())
    }
}
