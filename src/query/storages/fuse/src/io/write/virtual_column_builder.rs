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
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

use databend_common_catalog::plan::VirtualColumnLayout;
use databend_common_catalog::plan::VirtualColumnPath;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::conversion::number_common_type;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::i256;
use databend_common_expression::utils::FromData;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_hashtable::StackHashMap;
use databend_storages_common_blocks::SerializedParquet;
use databend_storages_common_blocks::blocks_to_parquet_with_stats;
use databend_storages_common_index::VirtualColumnNameIndex;
use databend_storages_common_index::VirtualColumnNode;
use databend_storages_common_index::VirtualColumnSharedColumnIdMap;
use databend_storages_common_index::VirtualColumnSharedDataType;
use databend_storages_common_table_meta::meta::DraftVirtualBlockMeta;
use databend_storages_common_table_meta::meta::DraftVirtualColumnBlockMeta;
use databend_storages_common_table_meta::meta::DraftVirtualColumnMeta;
use databend_storages_common_table_meta::meta::DraftVirtualColumnPathStatistics;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use databend_storages_common_table_meta::meta::VirtualColumnPhysicalType;
use databend_storages_common_table_meta::meta::VirtualOverflowBucketMeta;
use databend_storages_common_table_meta::meta::VirtualPathKind;
use databend_storages_common_table_meta::meta::VirtualSourceBlockMeta;
use databend_storages_common_table_meta::meta::VirtualValueState;
use databend_storages_common_table_meta::meta::virtual_overflow_bucket;
use jsonb::Date as JsonbDate;
use jsonb::Decimal64 as JsonbDecimal64;
use jsonb::Decimal128 as JsonbDecimal128;
use jsonb::Decimal256 as JsonbDecimal256;
use jsonb::Error as JsonbError;
use jsonb::Interval as JsonbInterval;
use jsonb::JsonbPathSegment;
use jsonb::JsonbStructureVisitor;
use jsonb::Number as JsonbNumber;
use jsonb::RawJsonb;
use jsonb::Timestamp as JsonbTimestamp;
use jsonb::TimestampTz as JsonbTimestampTz;
use jsonb::Value as JsonbValue;
use jsonb::VisitControl;
use jsonb::keypath::KeyPath;
use jsonb::keypath::OwnedKeyPath;
use jsonb::keypath::OwnedKeyPaths;
use log::info;
use parquet::file::metadata::KeyValue;
use parquet::file::metadata::ParquetMetaData;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use crate::MAX_VIRTUAL_COLUMN_DIRECT_COLUMNS;
use crate::MAX_VIRTUAL_COLUMN_PATH_STATISTICS;
use crate::VIRTUAL_COLUMN_MAX_OVERFLOW_BUCKETS;
use crate::VIRTUAL_COLUMN_OVERFLOW_ENTRIES_PER_BUCKET;
use crate::index::VIRTUAL_COLUMN_NODES_KEY;
use crate::index::VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY;
use crate::index::encode_compact_virtual_column_nodes;
use crate::index::encode_compact_virtual_column_shared_ids;
use crate::index::encode_compact_virtual_column_string_table;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnLayoutPolicy;
use crate::io::write::WriteSettings;
use crate::statistics::gen_columns_statistics;

const DEFAULT_VIRTUAL_COLUMN_NUMBER: usize = 32;
// Bound per-group expansion; high-cardinality element objects stay whole Jsonb.
const MAX_ARRAY_GROUP_COLUMNS: usize = 64;

#[derive(Debug, Clone)]
pub struct VirtualColumnState {
    pub data: opendal::Buffer,
    pub draft_virtual_block_meta: DraftVirtualBlockMeta,
}

#[derive(Clone, Default)]
pub enum VirtualColumnBuildMode {
    /// Classify paths from the data collected for each output block.
    #[default]
    Auto,
    /// Reproduce a prescribed layout, for example during repair/refresh.
    Exact(Arc<VirtualColumnLayout>),
    /// Use a layout selected from multiple input blocks during compaction/recluster.
    Adaptive(Arc<VirtualColumnLayout>),
}

impl VirtualColumnBuildMode {
    fn layout(&self) -> Option<&VirtualColumnLayout> {
        match self {
            Self::Auto => None,
            Self::Exact(layout) | Self::Adaptive(layout) => Some(layout),
        }
    }
}

#[derive(Clone)]
pub struct VirtualColumnBuilder {
    // Variant fields
    variant_fields: Vec<TableField>,
    // Variant field offsets
    variant_offsets: Vec<usize>,
    // Store virtual paths, the value is the index in `virtual_values`
    virtual_paths: Vec<HashMap<OwnedKeyPaths, usize>>,
    // Store virtual values across multiple blocks
    virtual_values: Vec<Vec<JsonbScalarValue>>,
    // Rows whose source value is SQL NULL, per variant field, across blocks.
    sql_null_rows: Vec<Vec<usize>>,
    // Total number of rows processed
    total_rows: usize,
    // Explicit Auto/Exact/Adaptive build semantics.
    mode: VirtualColumnBuildMode,
    max_path_statistics: usize,
    // User-configured per-source direct path budget, capped by the hard limit.
    max_direct_columns: usize,
}

impl VirtualColumnBuilder {
    pub fn try_create(
        schema: TableSchemaRef,
        policy: VirtualColumnLayoutPolicy,
    ) -> Result<VirtualColumnBuilder> {
        let mut variant_fields = Vec::new();
        let mut variant_offsets = Vec::new();
        for (i, field) in schema.fields.iter().enumerate() {
            if field.data_type().remove_nullable() == TableDataType::Variant {
                variant_fields.push(field.clone());
                variant_offsets.push(i);
            }
        }
        if variant_fields.is_empty() {
            return Err(ErrorCode::VirtualColumnError(
                "Virtual column only support variant type, but this table don't have variant type fields",
            ));
        }
        let mut virtual_paths = Vec::with_capacity(variant_fields.len());
        for _ in 0..variant_fields.len() {
            virtual_paths.push(HashMap::with_capacity(DEFAULT_VIRTUAL_COLUMN_NUMBER));
        }
        let virtual_values = Vec::with_capacity(DEFAULT_VIRTUAL_COLUMN_NUMBER);
        let sql_null_rows = vec![Vec::new(); variant_fields.len()];
        Ok(VirtualColumnBuilder {
            variant_offsets,
            variant_fields,
            virtual_paths,
            virtual_values,
            sql_null_rows,
            total_rows: 0,
            mode: VirtualColumnBuildMode::Auto,
            max_path_statistics: if policy.max_path_statistics == 0 {
                MAX_VIRTUAL_COLUMN_PATH_STATISTICS
            } else {
                policy
                    .max_path_statistics
                    .min(MAX_VIRTUAL_COLUMN_PATH_STATISTICS)
            },
            max_direct_columns: if policy.max_direct_columns == 0 {
                MAX_VIRTUAL_COLUMN_DIRECT_COLUMNS
            } else {
                policy
                    .max_direct_columns
                    .min(MAX_VIRTUAL_COLUMN_DIRECT_COLUMNS)
            },
        })
    }

    pub fn with_exact_layout(mut self, layout: Arc<VirtualColumnLayout>) -> Self {
        self.mode = VirtualColumnBuildMode::Exact(layout);
        self
    }

    pub fn with_adaptive_layout(mut self, layout: Arc<VirtualColumnLayout>) -> Self {
        self.mode = VirtualColumnBuildMode::Adaptive(layout);
        self
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        let num_rows = block.num_rows();

        // Generate hash values for existing key paths and use hash value to loop up
        // instead of generating `OwnedKeyPaths` repeatedly
        let mut hash_to_index = Vec::with_capacity(self.variant_fields.len());
        for virtual_paths in &self.virtual_paths {
            let mut field_hash_to_index: StackHashMap<u128, usize, 16> =
                StackHashMap::with_capacity(virtual_paths.len());
            for (virtual_path, index) in virtual_paths.iter() {
                let borrowed_key_paths = virtual_path.as_key_paths();

                let mut hasher = SipHasher24::new();
                borrowed_key_paths.hash(&mut hasher);
                let hash128 = hasher.finish128();
                let hash_value = hash128.into();
                unsafe {
                    match field_hash_to_index.insert_and_entry(hash_value) {
                        Ok(e) => {
                            let v = e.get_mut();
                            *v = *index;
                        }
                        Err(e) => {
                            let v = e.get_mut();
                            *v = *index;
                        }
                    }
                }
            }
            hash_to_index.push(field_hash_to_index);
        }

        self.extract_virtual_values(block, 0, num_rows, &mut hash_to_index)?;

        self.total_rows += num_rows;

        Ok(())
    }

    fn extract_virtual_values(
        &mut self,
        block: &DataBlock,
        start_row: usize,
        end_row: usize,
        hash_to_index: &mut [StackHashMap<u128, usize, 16>],
    ) -> Result<()> {
        for (i, offset) in self.variant_offsets.iter().enumerate() {
            let column = block.get_by_offset(*offset);
            for row in start_row..end_row {
                let val = unsafe { column.index_unchecked(row) };
                // Blocks are added repeatedly, so the actual rows need to add the rows of the previous blocks
                let block_row = self.total_rows + row;
                let ScalarRef::Variant(jsonb_bytes) = val else {
                    // SQL NULL has no jsonb value; it is recorded in the root validity column.
                    self.sql_null_rows[i].push(block_row);
                    continue;
                };
                let raw_jsonb = RawJsonb::new(jsonb_bytes);
                let mut collector = VirtualValueCollector {
                    row: block_row,
                    hash_to_index: &mut hash_to_index[i],
                    virtual_paths: &mut self.virtual_paths[i],
                    virtual_values: &mut self.virtual_values,
                    key_paths: Vec::with_capacity(3),
                };
                raw_jsonb.visit_structure(&mut collector).map_err(|error| {
                    ErrorCode::VirtualColumnError(format!(
                        "failed to extract virtual column values: {error}"
                    ))
                })?;
            }
        }
        Ok(())
    }

    fn jsonb_value_to_scalar(value: JsonbValue<'_>) -> Scalar {
        match value {
            JsonbValue::Null => Scalar::Null,
            JsonbValue::Bool(v) => Scalar::Boolean(v),
            JsonbValue::String(s) => Scalar::String(s.to_string()),
            JsonbValue::Number(n) => match n {
                JsonbNumber::Int64(v) => Scalar::Number(NumberScalar::Int64(v)),
                JsonbNumber::UInt64(v) => Scalar::Number(NumberScalar::UInt64(v)),
                JsonbNumber::Float64(v) => Scalar::Number(NumberScalar::Float64(v.into())),
                JsonbNumber::Decimal64(v) => Scalar::Decimal(DecimalScalar::Decimal64(
                    v.value,
                    DecimalSize::new_unchecked(i64::MAX_PRECISION, v.scale),
                )),
                JsonbNumber::Decimal128(v) => Scalar::Decimal(DecimalScalar::Decimal128(
                    v.value,
                    DecimalSize::new_unchecked(i128::MAX_PRECISION, v.scale),
                )),
                JsonbNumber::Decimal256(v) => Scalar::Decimal(DecimalScalar::Decimal256(
                    i256(v.value),
                    DecimalSize::new_unchecked(i256::MAX_PRECISION, v.scale),
                )),
            },
            JsonbValue::Binary(v) => Scalar::Binary(v.to_vec()),
            JsonbValue::Date(v) => Scalar::Date(v.value),
            JsonbValue::Timestamp(v) => Scalar::Timestamp(v.value),
            JsonbValue::TimestampTz(v) => Scalar::TimestampTz(timestamp_tz::new(v.value, v.offset)),
            JsonbValue::Interval(v) => {
                Scalar::Interval(months_days_micros::new(v.months, v.days, v.micros))
            }
            _ => Scalar::Variant(value.to_vec()),
        }
    }

    fn scalar_to_jsonb_value(scalar: ScalarRef<'_>) -> JsonbValue<'_> {
        match scalar {
            ScalarRef::Null => JsonbValue::Null,
            ScalarRef::Boolean(v) => JsonbValue::Bool(v),
            ScalarRef::String(s) => JsonbValue::String(Cow::Borrowed(s)),
            ScalarRef::Number(NumberScalar::Int64(n)) => JsonbValue::Number(JsonbNumber::Int64(n)),
            ScalarRef::Number(NumberScalar::UInt64(n)) => {
                JsonbValue::Number(JsonbNumber::UInt64(n))
            }
            ScalarRef::Number(NumberScalar::Float64(n)) => {
                JsonbValue::Number(JsonbNumber::Float64(n.0))
            }
            ScalarRef::Decimal(DecimalScalar::Decimal64(v, size)) => {
                JsonbValue::Number(JsonbNumber::Decimal64(JsonbDecimal64 {
                    value: v,
                    scale: size.scale(),
                }))
            }
            ScalarRef::Decimal(DecimalScalar::Decimal128(v, size)) => {
                JsonbValue::Number(JsonbNumber::Decimal128(JsonbDecimal128 {
                    value: v,
                    scale: size.scale(),
                }))
            }
            ScalarRef::Decimal(DecimalScalar::Decimal256(v, size)) => {
                JsonbValue::Number(JsonbNumber::Decimal256(JsonbDecimal256 {
                    value: v.0,
                    scale: size.scale(),
                }))
            }
            ScalarRef::Binary(v) => JsonbValue::Binary(v),
            ScalarRef::Date(v) => JsonbValue::Date(JsonbDate { value: v }),
            ScalarRef::Timestamp(v) => JsonbValue::Timestamp(JsonbTimestamp { value: v }),
            ScalarRef::TimestampTz(v) => JsonbValue::TimestampTz(JsonbTimestampTz {
                value: v.timestamp(),
                offset: v.seconds_offset(),
            }),
            ScalarRef::Interval(v) => JsonbValue::Interval(JsonbInterval {
                months: v.months(),
                days: v.days(),
                micros: v.microseconds(),
            }),
            ScalarRef::Variant(v) => RawJsonb::new(v).to_value().unwrap(),
            _ => unreachable!(),
        }
    }

    fn scalar_to_variant_bytes(scalar: ScalarRef<'_>) -> Vec<u8> {
        let jsonb_value = Self::scalar_to_jsonb_value(scalar);
        let mut buf = Vec::new();
        jsonb_value.write_to_vec(&mut buf);
        buf
    }

    fn key_path_segment(path: &OwnedKeyPath) -> String {
        match path {
            OwnedKeyPath::Index(idx) => idx.to_string(),
            OwnedKeyPath::Name(name) => name.to_string(),
        }
    }

    fn build_path_statistics(
        variant_fields: &[TableField],
        virtual_paths: &[HashMap<OwnedKeyPaths, usize>],
        virtual_values: &[Vec<JsonbScalarValue>],
        direct_paths: &HashSet<(ColumnId, String)>,
        max_path_statistics: usize,
    ) -> HashMap<ColumnId, DraftVirtualColumnPathStatistics> {
        let mut statistics = HashMap::new();
        for (source_field, field_virtual_paths) in variant_fields.iter().zip(virtual_paths.iter()) {
            let mut path_counts = Vec::new();
            for (path, index) in field_virtual_paths {
                // The root is a structure column, not a path to select or count.
                if path.paths.is_empty() {
                    continue;
                }
                let canonical_path = path.to_canonical_path();
                if direct_paths.contains(&(source_field.column_id, canonical_path.clone())) {
                    continue;
                }
                let value_count = virtual_values
                    .get(*index)
                    .map(|values| values.len().min(u32::MAX as usize) as u32)
                    .unwrap_or(0);
                if value_count == 0 {
                    continue;
                }
                path_counts.push((canonical_path, value_count));
            }
            if path_counts.is_empty() {
                continue;
            }
            let complete = path_counts.len() <= max_path_statistics;
            path_counts
                .sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
            path_counts.truncate(max_path_statistics);
            path_counts.sort_by(|left, right| left.0.cmp(&right.0));
            statistics.insert(source_field.column_id, DraftVirtualColumnPathStatistics {
                path_statistics_complete: complete,
                path_counts,
            });
        }
        statistics
    }

    fn get_string_table_id(
        name: &str,
        string_table: &mut Vec<String>,
        string_table_index: &mut HashMap<String, u32>,
    ) -> u32 {
        if let Some(id) = string_table_index.get(name) {
            return *id;
        }
        let id = string_table.len() as u32;
        string_table.push(name.to_string());
        string_table_index.insert(name.to_string(), id);
        id
    }

    fn virtual_column_node_mut<'n>(
        root: &'n mut VirtualColumnNode,
        path: &OwnedKeyPaths,
        string_table: &mut Vec<String>,
        string_table_index: &mut HashMap<String, u32>,
    ) -> &'n mut VirtualColumnNode {
        // Build a trie from key path segments. Each segment is stored once in the
        // string table and referenced by id to keep parquet metadata compact.
        // The root path is the source node itself.
        let mut current = root;
        for segment in &path.paths {
            let segment_name = Self::key_path_segment(segment);
            let segment_id =
                Self::get_string_table_id(&segment_name, string_table, string_table_index);
            current = current.children.entry(segment_id).or_default();
        }
        current
    }

    fn insert_virtual_column_node(
        root: &mut VirtualColumnNode,
        path: &OwnedKeyPaths,
        leaf: VirtualColumnNameIndex,
        string_table: &mut Vec<String>,
        string_table_index: &mut HashMap<String, u32>,
    ) {
        Self::virtual_column_node_mut(root, path, string_table, string_table_index).leaf =
            Some(leaf);
    }

    fn build_block_local_layout(
        variant_fields: &[TableField],
        virtual_paths: &[HashMap<OwnedKeyPaths, usize>],
        virtual_values: &[Vec<JsonbScalarValue>],
        max_direct_columns: usize,
    ) -> VirtualColumnLayout {
        let mut direct_paths = Vec::new();
        for (source_field, paths) in variant_fields.iter().zip(virtual_paths) {
            let mut candidates = paths
                .iter()
                // The root is always direct and does not use the path budget.
                .filter(|(path, _)| !path.paths.is_empty())
                .map(|(path, index)| {
                    (
                        virtual_values.get(*index).map(Vec::len).unwrap_or(0),
                        path.to_canonical_path(),
                    )
                })
                .filter(|(count, _)| *count > 0)
                .collect::<Vec<_>>();
            candidates
                .sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
            direct_paths.extend(candidates.into_iter().take(max_direct_columns).map(
                |(_, path)| VirtualColumnPath {
                    source_column_id: source_field.column_id,
                    path,
                },
            ));
        }
        direct_paths.sort();
        VirtualColumnLayout { direct_paths }
    }

    fn build_variant_column<'v>(
        total_rows: usize,
        values: impl Iterator<Item = &'v JsonbScalarValue>,
    ) -> Column {
        let mut bitmap = MutableBitmap::from_len_zeroed(total_rows);
        let mut builder =
            BinaryColumnBuilder::with_capacity(total_rows, values.size_hint().0.saturating_mul(10));
        let mut last_row = 0usize;
        for val in values {
            while last_row < val.row {
                builder.commit_row();
                last_row += 1;
            }
            bitmap.set(val.row, true);
            let bytes = Self::scalar_to_variant_bytes(val.scalar.as_ref());
            builder.put_slice(&bytes);
            builder.commit_row();
            last_row += 1;
        }
        while last_row < total_rows {
            builder.commit_row();
            last_row += 1;
        }
        let nullable_column = NullableColumn {
            column: Column::Variant(builder.build()),
            validity: bitmap.into(),
        };
        Column::Nullable(Box::new(nullable_column))
    }

    fn build_direct_column<'v>(
        total_rows: usize,
        values: impl Iterator<Item = &'v JsonbScalarValue>,
        physical_type: &VirtualColumnPhysicalType,
    ) -> Result<(Column, TableDataType)> {
        if matches!(physical_type, VirtualColumnPhysicalType::Jsonb) {
            let column = Self::build_variant_column(total_rows, values);
            let table_type = infer_schema_type(&DataType::Nullable(Box::new(DataType::Variant)))?;
            Ok((column, table_type))
        } else {
            Self::build_typed_column(total_rows, values, physical_type)
        }
    }

    fn build_typed_column<'v>(
        total_rows: usize,
        values: impl Iterator<Item = &'v JsonbScalarValue>,
        value_type: &VirtualColumnPhysicalType,
    ) -> Result<(Column, TableDataType)> {
        let data_type = DataType::from(&value_type.table_data_type()).wrap_nullable();

        let mut builder = ColumnBuilder::with_capacity(&data_type, total_rows);
        let mut last_row = 0usize;
        let null_scalar = ScalarRef::Null;
        for val in values {
            if val.row > last_row {
                let default_len = val.row - last_row;
                builder.push_repeat(&null_scalar, default_len);
                last_row = val.row;
            }
            if val.scalar.is_null() {
                builder.push(ScalarRef::Null);
            } else {
                let scalar = match (value_type, &val.scalar) {
                    (VirtualColumnPhysicalType::Array(inner), Scalar::Variant(bytes)) => {
                        Self::build_typed_array_scalar(bytes, inner)?
                    }
                    _ => databend_common_expression::cast_scalar(
                        None,
                        val.scalar.clone(),
                        &data_type.remove_nullable(),
                        &BUILTIN_FUNCTIONS,
                    )?,
                };
                builder.push(scalar.as_ref());
            }
            last_row += 1;
        }
        if last_row < total_rows {
            builder.push_repeat(&null_scalar, total_rows - last_row);
        }
        let column = builder.build();
        let table_type = infer_schema_type(&data_type)?;
        Ok((column, table_type))
    }

    /// Materialize a complete scalar array leaf, preserving JSON null elements
    /// as nullable array entries. Inference has already rejected containers and
    /// selected a lossless common scalar type across the leaf's arrays.
    fn build_typed_array_scalar(bytes: &[u8], inner: &VirtualColumnPhysicalType) -> Result<Scalar> {
        let value = RawJsonb::new(bytes).to_value().map_err(|error| {
            ErrorCode::VirtualColumnError(format!("failed to decode typed array leaf: {error}"))
        })?;
        let JsonbValue::Array(elements) = value else {
            return Err(ErrorCode::Internal(
                "typed virtual array leaf is not an array",
            ));
        };
        let inner_type = DataType::from(&inner.table_data_type());
        let mut builder = ColumnBuilder::with_capacity(&inner_type.wrap_nullable(), elements.len());
        for element in elements {
            let scalar = Self::jsonb_value_to_scalar(element);
            if scalar.is_null() {
                builder.push(ScalarRef::Null);
            } else {
                let scalar = databend_common_expression::cast_scalar(
                    None,
                    scalar,
                    &inner_type,
                    &BUILTIN_FUNCTIONS,
                )?;
                builder.push(scalar.as_ref());
            }
        }
        Ok(Scalar::Array(builder.build()))
    }

    /// Builds the nullable `Boolean` validity column of a source: false for
    /// rows whose source value is SQL NULL.
    fn build_validity_column(total_rows: usize, sql_null_rows: &[usize]) -> Column {
        let mut values = vec![true; total_rows];
        for row in sql_null_rows {
            values[*row] = false;
        }
        Column::Nullable(Box::new(NullableColumn {
            column: BooleanType::from_data(values),
            validity: MutableBitmap::from_len_set(total_rows).into(),
        }))
    }

    /// Builds one overflow bucket, a `Map(String, Variant)` column. `entries`
    /// must be sorted by `(row, path)`.
    fn build_overflow_bucket_column(entries: &[OverflowEntry], total_rows: usize) -> Column {
        let mut key_builder = ColumnBuilder::with_capacity(&DataType::String, entries.len());
        let mut value_builder = ColumnBuilder::with_capacity(&DataType::Variant, entries.len());
        let mut offsets = Vec::with_capacity(total_rows + 1);
        offsets.push(0u64);
        let mut entries = entries.iter().peekable();
        let mut current = 0u64;
        for row in 0..total_rows {
            while let Some(entry) = entries.next_if(|entry| entry.row == row) {
                key_builder.push(ScalarRef::String(entry.path.as_str()));
                value_builder.push(ScalarRef::Variant(entry.value.as_slice()));
                current += 1;
            }
            offsets.push(current);
        }
        debug_assert!(entries.next().is_none());

        let tuple = Column::Tuple(vec![key_builder.build(), value_builder.build()]);
        Column::Map(Box::new(ArrayColumn::new(tuple, Buffer::from(offsets))))
    }

    fn overflow_map_type() -> TableDataType {
        TableDataType::Map(Box::new(TableDataType::Tuple {
            fields_name: vec!["key".to_string(), "value".to_string()],
            fields_type: vec![TableDataType::String, TableDataType::Variant],
        }))
    }

    /// The overflow bucket count of a block: one bucket per
    /// `VIRTUAL_COLUMN_OVERFLOW_ENTRIES_PER_BUCKET` entries, capped.
    fn overflow_bucket_count(entry_count: usize) -> usize {
        entry_count
            .div_ceil(VIRTUAL_COLUMN_OVERFLOW_ENTRIES_PER_BUCKET)
            .clamp(1, VIRTUAL_COLUMN_MAX_OVERFLOW_BUCKETS)
    }

    fn build_shared_map_column(
        data_type: VirtualColumnSharedDataType,
        shared_value_indexes: Vec<usize>,
        virtual_values: &[Vec<JsonbScalarValue>],
        total_rows: usize,
    ) -> Column {
        let mut shared_values: HashMap<usize, Vec<(u32, Scalar)>> = HashMap::new();
        for (index, value_index) in shared_value_indexes.into_iter().enumerate() {
            let key_name_index = index as u32;
            let values = &virtual_values[value_index];
            for val in values {
                if let Some(shared_rows) = shared_values.get_mut(&val.row) {
                    shared_rows.push((key_name_index, val.scalar.clone()));
                } else {
                    let shared_rows = vec![(key_name_index, val.scalar.clone())];
                    shared_values.insert(val.row, shared_rows);
                }
            }
        }

        let mut key_builder =
            ColumnBuilder::with_capacity(&DataType::Number(NumberDataType::UInt32), total_rows);
        let value_data_type = Self::shared_value_data_type(data_type);
        let mut value_builder = ColumnBuilder::with_capacity(&value_data_type, total_rows);
        let mut offsets = Vec::with_capacity(total_rows + 1);
        offsets.push(0);
        let mut current = 0u64;
        for row in 0..total_rows {
            if let Some(shared_rows) = shared_values.remove(&row) {
                for (key_name_index, scalar) in shared_rows {
                    key_builder.push(ScalarRef::Number(NumberScalar::UInt32(key_name_index)));
                    if matches!(data_type, VirtualColumnSharedDataType::Jsonb) {
                        let jsonb_bytes = Self::scalar_to_variant_bytes(scalar.as_ref());
                        value_builder.push(ScalarRef::Variant(jsonb_bytes.as_slice()));
                    } else {
                        value_builder.push(scalar.as_ref());
                    }
                    current += 1;
                }
            }
            offsets.push(current);
        }

        let keys_column = key_builder.build();
        let values_column = value_builder.build();
        let tuple = Column::Tuple(vec![keys_column, values_column]);
        let array_col = ArrayColumn::new(tuple, Buffer::from(offsets));
        Column::Map(Box::new(array_col))
    }

    fn shared_data_type_from_variant_type(
        value_type: &VirtualColumnPhysicalType,
    ) -> VirtualColumnSharedDataType {
        match value_type {
            VirtualColumnPhysicalType::Boolean => VirtualColumnSharedDataType::Boolean,
            VirtualColumnPhysicalType::Number(NumberDataType::UInt64) => {
                VirtualColumnSharedDataType::UInt64
            }
            VirtualColumnPhysicalType::Number(NumberDataType::Int64) => {
                VirtualColumnSharedDataType::Int64
            }
            VirtualColumnPhysicalType::Number(NumberDataType::Float64) => {
                VirtualColumnSharedDataType::Float64
            }
            VirtualColumnPhysicalType::String => VirtualColumnSharedDataType::String,
            _ => VirtualColumnSharedDataType::Jsonb,
        }
    }

    fn shared_value_data_type(data_type: VirtualColumnSharedDataType) -> DataType {
        match data_type {
            VirtualColumnSharedDataType::Boolean => DataType::Boolean,
            VirtualColumnSharedDataType::UInt64 => DataType::Number(NumberDataType::UInt64),
            VirtualColumnSharedDataType::Int64 => DataType::Number(NumberDataType::Int64),
            VirtualColumnSharedDataType::Float64 => DataType::Number(NumberDataType::Float64),
            VirtualColumnSharedDataType::String => DataType::String,
            VirtualColumnSharedDataType::Jsonb => DataType::Variant,
        }
    }

    fn physical_column_name(
        source_column_id: ColumnId,
        source_name: &str,
        canonical_path: &str,
    ) -> String {
        // The root path is the empty string; `$` cannot clash with a key,
        // because such a key is quoted in canonical paths.
        let path = if canonical_path.is_empty() {
            "$"
        } else {
            canonical_path
        };
        format!("{source_column_id}_{source_name}.{path}")
    }

    /// The Jsonb bytes of a value stored in the overflow map.
    fn overflow_value_bytes(value: &JsonbScalarValue, state: VirtualValueState) -> Vec<u8> {
        match state {
            // JSON null is extracted as `Scalar::Null`.
            VirtualValueState::JsonNull => JsonbValue::Null.to_vec(),
            _ => Self::scalar_to_variant_bytes(value.scalar.as_ref()),
        }
    }

    // Shared names omit the direct path separator before their tagged first-leaf
    // ordinal. Readers still use trie/shared leaf ordinals as authoritative identity.
    fn shared_column_name(
        source_column_id: ColumnId,
        source_name: &str,
        data_type: VirtualColumnSharedDataType,
        parquet_column_id: u32,
    ) -> String {
        let data_type = match data_type {
            VirtualColumnSharedDataType::Boolean => "bool",
            VirtualColumnSharedDataType::UInt64 => "uint64",
            VirtualColumnSharedDataType::Int64 => "int64",
            VirtualColumnSharedDataType::Float64 => "float64",
            VirtualColumnSharedDataType::String => "string",
            VirtualColumnSharedDataType::Jsonb => "jsonb",
        };
        format!("{source_column_id}_{source_name}__shared_{data_type}_{parquet_column_id}__")
    }

    /// Split a source-root array<object> into aligned list columns. Non-object
    /// elements (except JSON null) keep the whole array as a leaf. Arrays in
    /// object fields are never expanded. Empty arrays participate only when
    /// another root array establishes an object element schema.
    fn build_array_group(
        total_rows: usize,
        values: &[JsonbScalarValue],
        max_element_paths: usize,
    ) -> Result<Option<Vec<ArrayElementColumn>>> {
        let mut has_array = false;
        for value in values.iter().filter(|value| value.state.is_none()) {
            let Scalar::Variant(bytes) = &value.scalar else {
                return Ok(None);
            };
            match RawJsonb::new(bytes).array_length() {
                Ok(Some(_)) => has_array = true,
                Ok(None) => return Ok(None),
                Err(error) => {
                    return Err(ErrorCode::VirtualColumnError(format!(
                        "failed to inspect array group: {error}"
                    )));
                }
            }
        }
        if !has_array {
            return Ok(None);
        }
        let mut paths = HashMap::new();
        let mut element_values = Vec::new();
        let mut hash_to_index = StackHashMap::with_capacity(DEFAULT_VIRTUAL_COLUMN_NUMBER);
        let mut offsets = Vec::with_capacity(total_rows + 1);
        let mut validity = MutableBitmap::from_len_zeroed(total_rows);
        let mut element_count = 0;
        let mut has_object = false;
        let mut last_row = 0;
        offsets.push(0u64);
        for value in values {
            while last_row < value.row {
                offsets.push(element_count as u64);
                last_row += 1;
            }
            if value.state == Some(VirtualValueState::EmptyArray) {
                validity.set(value.row, true);
            } else if value.state.is_none() {
                let Scalar::Variant(bytes) = &value.scalar else {
                    return Ok(None);
                };
                let Some(elements) = RawJsonb::new(bytes).array_values().map_err(|error| {
                    ErrorCode::VirtualColumnError(format!("failed to read array group: {error}"))
                })?
                else {
                    return Ok(None);
                };
                validity.set(value.row, true);
                for element in elements {
                    let raw = element.as_raw();
                    if raw.is_object().map_err(|error| {
                        ErrorCode::VirtualColumnError(format!(
                            "failed to inspect root array element: {error}"
                        ))
                    })? {
                        has_object = true;
                    } else if !raw.is_null().map_err(|error| {
                        ErrorCode::VirtualColumnError(format!(
                            "failed to inspect root array element: {error}"
                        ))
                    })? {
                        return Ok(None);
                    }
                    let mut collector = VirtualValueCollector {
                        row: element_count,
                        hash_to_index: &mut hash_to_index,
                        virtual_paths: &mut paths,
                        virtual_values: &mut element_values,
                        key_paths: Vec::new(),
                    };
                    element
                        .as_raw()
                        .visit_structure(&mut collector)
                        .map_err(|error| {
                            ErrorCode::VirtualColumnError(format!(
                                "failed to extract array group elements: {error}"
                            ))
                        })?;
                    element_count += 1;
                    if paths.len() > max_element_paths {
                        return Ok(None);
                    }
                }
            }
            offsets.push(element_count as u64);
            last_row += 1;
        }
        while last_row < total_rows {
            offsets.push(element_count as u64);
            last_row += 1;
        }
        if !has_object || paths.is_empty() {
            return Ok(None);
        }
        let offsets = Buffer::from(offsets);
        let validity: databend_common_expression::types::Bitmap = validity.into();
        let mut columns = Vec::with_capacity(paths.len());
        for (path, index) in paths.into_iter().collect::<BTreeMap<_, _>>() {
            let values = &element_values[index];
            // Explicit JSON null must remain different from a missing field.
            let physical_type = if values.iter().any(|value| value.scalar.is_null()) {
                VirtualColumnPhysicalType::Jsonb
            } else {
                Self::inference_data_type(values)
            };
            let (elements, element_type) =
                Self::build_direct_column(element_count, values.iter(), &physical_type)?;
            let column = Column::Nullable(Box::new(NullableColumn {
                column: Column::Array(Box::new(ArrayColumn::new(elements, offsets.clone()))),
                validity: validity.clone(),
            }));
            columns.push(ArrayElementColumn {
                path,
                column,
                table_type: TableDataType::Nullable(Box::new(TableDataType::Array(Box::new(
                    element_type,
                )))),
                physical_type: VirtualColumnPhysicalType::Array(Box::new(physical_type)),
            });
        }
        Ok(Some(columns))
    }

    #[async_backtrace::framed]
    pub fn finalize(
        &mut self,
        write_settings: &WriteSettings,
        location: &Location,
    ) -> Result<VirtualColumnState> {
        let mut virtual_paths = Vec::with_capacity(self.variant_fields.len());
        for _ in 0..self.variant_fields.len() {
            virtual_paths.push(HashMap::with_capacity(DEFAULT_VIRTUAL_COLUMN_NUMBER));
        }
        std::mem::swap(&mut self.virtual_paths, &mut virtual_paths);

        let mut virtual_values = Vec::with_capacity(self.virtual_values.len());
        std::mem::swap(&mut self.virtual_values, &mut virtual_values);

        let mut sql_null_rows = vec![Vec::new(); self.variant_fields.len()];
        std::mem::swap(&mut self.sql_null_rows, &mut sql_null_rows);

        let total_rows = self.total_rows;
        self.total_rows = 0;
        let extracted_path_count = virtual_paths.iter().map(HashMap::len).sum::<usize>();
        let extracted_value_count = virtual_values.iter().map(Vec::len).sum::<usize>();
        if total_rows == 0 || extracted_value_count == 0 {
            info!(
                "No virtual column data generated for block {}: rows={}, variant_fields={}, extracted_paths={}",
                location.0,
                total_rows,
                self.variant_fields.len(),
                extracted_path_count
            );
            let path_statistics = Self::build_path_statistics(
                &self.variant_fields,
                &virtual_paths,
                &virtual_values,
                &HashSet::new(),
                self.max_path_statistics,
            );
            let draft_virtual_block_meta = DraftVirtualBlockMeta {
                virtual_columns: None,
                path_statistics: (!path_statistics.is_empty()).then_some(path_statistics),
            };

            return Ok(VirtualColumnState {
                data: opendal::Buffer::new(),
                draft_virtual_block_meta,
            });
        }

        let effective_layout = match self.mode.layout() {
            Some(layout) => Cow::Borrowed(layout),
            None => Cow::Owned(Self::build_block_local_layout(
                &self.variant_fields,
                &virtual_paths,
                &virtual_values,
                self.max_direct_columns,
            )),
        };

        let direct_paths = effective_layout
            .direct_paths
            .iter()
            .map(|path| (path.source_column_id, path.path.clone()))
            .collect::<HashSet<_>>();
        let mut path_statistics = Self::build_path_statistics(
            &self.variant_fields,
            &virtual_paths,
            &virtual_values,
            &direct_paths,
            self.max_path_statistics,
        );

        // One entry per Parquet leaf ordinal. Direct leaves retain their logical
        // source/path/type identity; shared map key/value leaves are described by
        // `typed_shared_column_ids` instead.
        let mut virtual_column_mappings: Vec<LeafMapping> = Vec::new();
        let mut virtual_fields = Vec::new();
        let mut virtual_columns = Vec::new();
        let mut string_table = Vec::new();
        let mut string_table_index = HashMap::new();
        let mut virtual_column_nodes = HashMap::new();
        let mut shared_column_names = HashMap::new();
        let mut needs_footer = false;
        // leaf_index tracks the parquet column id for virtual columns and shared maps.
        // It stays aligned with the VirtualColumnNameIndex used by the trie.
        let mut leaf_index: u32 = 0;
        for ((source_field, field_virtual_paths), source_sql_null_rows) in self
            .variant_fields
            .iter()
            .zip(virtual_paths.into_iter())
            .zip(sql_null_rows.iter())
        {
            // JSON null, `{}` and `[]` of direct paths, stored in the overflow map
            // so they do not force the value column to Jsonb.
            let mut overflow_entries = Vec::new();
            let mut array_path_counts = Vec::new();
            let sorted_virtual_paths: BTreeMap<_, _> = field_virtual_paths.into_iter().collect();
            let mut shared_values_by_type: BTreeMap<
                VirtualColumnSharedDataType,
                Vec<(OwnedKeyPaths, usize)>,
            > = BTreeMap::new();
            let node = virtual_column_nodes
                .entry(source_field.column_id)
                .or_insert_with(VirtualColumnNode::default);
            for (path, value_index) in sorted_virtual_paths {
                let values = &virtual_values[value_index];
                if values.is_empty() {
                    continue;
                }
                let canonical_path = path.to_canonical_path();
                // The root is always direct.
                let is_direct = path.paths.is_empty()
                    || effective_layout.contains(source_field.column_id, &canonical_path);
                if is_direct {
                    if path.paths.is_empty()
                        && let Some(columns) = Self::build_array_group(
                            total_rows,
                            values,
                            self.max_direct_columns.min(MAX_ARRAY_GROUP_COLUMNS),
                        )?
                    {
                        // Scalar-only BlockMeta planning cannot reconstruct groups.
                        needs_footer = true;
                        let group_node = Self::virtual_column_node_mut(
                            node,
                            &path,
                            &mut string_table,
                            &mut string_table_index,
                        );
                        let element_node = group_node
                            .element
                            .get_or_insert_with(|| Box::new(VirtualColumnNode::default()));
                        for element in columns {
                            let element_path = jsonb::keypath::encode_canonical_structure_path(
                                path.paths
                                    .iter()
                                    .map(|key| match key {
                                        OwnedKeyPath::Name(key) => JsonbPathSegment::Key(key),
                                        OwnedKeyPath::Index(_) => unreachable!(),
                                    })
                                    .chain(std::iter::once(JsonbPathSegment::Element))
                                    .chain(element.path.paths.iter().map(|key| match key {
                                        OwnedKeyPath::Name(key) => JsonbPathSegment::Key(key),
                                        OwnedKeyPath::Index(_) => unreachable!(),
                                    })),
                            );
                            let name = Self::physical_column_name(
                                source_field.column_id,
                                &source_field.name,
                                &element_path,
                            );
                            virtual_fields.push(TableField::new_from_column_id(
                                &name,
                                element.table_type,
                                leaf_index,
                            ));
                            virtual_columns.push(BlockEntry::Column(element.column));
                            virtual_column_mappings.push(LeafMapping::ArrayElement(
                                DirectColumnMapping {
                                    source_column_id: source_field.column_id,
                                    path: element_path,
                                    physical_type: element.physical_type,
                                    value_state_mask: 0,
                                },
                            ));
                            Self::insert_virtual_column_node(
                                element_node,
                                &element.path,
                                VirtualColumnNameIndex::Column(leaf_index),
                                &mut string_table,
                                &mut string_table_index,
                            );
                            leaf_index += 1;
                        }
                        for value in values {
                            if let Some(state) = value.state
                                && state != VirtualValueState::EmptyArray
                            {
                                overflow_entries.push(OverflowEntry {
                                    row: value.row,
                                    path: canonical_path.clone(),
                                    value: Self::overflow_value_bytes(value, state),
                                    state,
                                });
                            }
                        }
                        continue;
                    }
                    let mut value_state_mask = 0;
                    for value in values {
                        if let Some(state) = value.state {
                            value_state_mask |= state.mask_bit();
                            overflow_entries.push(OverflowEntry {
                                row: value.row,
                                path: canonical_path.clone(),
                                value: Self::overflow_value_bytes(value, state),
                                state,
                            });
                        }
                    }
                    let value_rows = || values.iter().filter(|value| value.state.is_none());
                    if value_rows().next().is_some() {
                        let physical_type = Self::inference_data_type(value_rows());
                        if !path.paths.is_empty()
                            && matches!(&physical_type, VirtualColumnPhysicalType::Array(_))
                        {
                            // Parquet num_values counts elements, not source rows.
                            array_path_counts.push((
                                canonical_path.clone(),
                                values.len().min(u32::MAX as usize) as u32,
                            ));
                        }
                        let (column, table_type) =
                            Self::build_direct_column(total_rows, value_rows(), &physical_type)?;
                        let physical_name = Self::physical_column_name(
                            source_field.column_id,
                            &source_field.name,
                            &canonical_path,
                        );
                        virtual_fields.push(TableField::new_from_column_id(
                            &physical_name,
                            table_type,
                            leaf_index,
                        ));
                        virtual_columns.push(BlockEntry::Column(column));
                        debug_assert_eq!(virtual_column_mappings.len(), leaf_index as usize);
                        virtual_column_mappings.push(LeafMapping::Direct(DirectColumnMapping {
                            source_column_id: source_field.column_id,
                            path: canonical_path,
                            physical_type,
                            value_state_mask,
                        }));
                        Self::insert_virtual_column_node(
                            node,
                            &path,
                            VirtualColumnNameIndex::Column(leaf_index),
                            &mut string_table,
                            &mut string_table_index,
                        );
                        leaf_index += 1;
                    } else {
                        // Every value is in the overflow map. Keep a trie node without
                        // a leaf, so object reconstruction still visits the path.
                        Self::virtual_column_node_mut(
                            node,
                            &path,
                            &mut string_table,
                            &mut string_table_index,
                        );
                    }
                    continue;
                }

                // Shared paths keep JSON null, `{}` and `[]` in their values, so
                // the value type falls back to Jsonb to preserve them.
                let physical_type = if values.iter().any(|value| value.scalar.is_null()) {
                    // Nullable typed columns cannot distinguish an absent path
                    // from an explicit JSON null. Jsonb preserves all three
                    // states: missing, JSON null, and a present non-null value.
                    VirtualColumnPhysicalType::Jsonb
                } else {
                    Self::inference_data_type(values)
                };
                let shared_type = Self::shared_data_type_from_variant_type(&physical_type);
                shared_values_by_type
                    .entry(shared_type)
                    .or_default()
                    .push((path, value_index));
            }

            // Typed array leaves count Parquet elements, not source rows.
            // Retain their source-row frequencies for compaction layout selection.
            if !array_path_counts.is_empty() {
                let statistics = path_statistics
                    .entry(source_field.column_id)
                    .or_insert_with(|| DraftVirtualColumnPathStatistics {
                        path_statistics_complete: true,
                        path_counts: Vec::new(),
                    });
                statistics.path_counts.extend(array_path_counts);
                if statistics.path_counts.len() > self.max_path_statistics {
                    statistics.path_statistics_complete = false;
                    statistics.path_counts.sort_by(|left, right| {
                        right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
                    });
                    statistics.path_counts.truncate(self.max_path_statistics);
                }
                statistics
                    .path_counts
                    .sort_by(|left, right| left.0.cmp(&right.0));
            }

            for (shared_data_type, shared_values) in shared_values_by_type {
                if shared_values.is_empty() {
                    continue;
                }
                needs_footer = true;

                let shared_value_indexes = shared_values
                    .iter()
                    .map(|(_, index)| *index)
                    .collect::<Vec<_>>();
                let column = Self::build_shared_map_column(
                    shared_data_type,
                    shared_value_indexes,
                    &virtual_values,
                    total_rows,
                );

                let map_type = TableDataType::Map(Box::new(TableDataType::Tuple {
                    fields_name: vec!["key".to_string(), "value".to_string()],
                    fields_type: vec![
                        TableDataType::Number(NumberDataType::UInt32),
                        infer_schema_type(&Self::shared_value_data_type(shared_data_type)).unwrap(),
                    ],
                }));

                let column_id = leaf_index;
                let physical_name = Self::shared_column_name(
                    source_field.column_id,
                    &source_field.name,
                    shared_data_type,
                    column_id,
                );
                let field = TableField::new_from_column_id(&physical_name, map_type, column_id);
                virtual_columns.push(BlockEntry::Column(column));
                virtual_fields.push(field);
                let source_column_id = source_field.column_id;
                shared_column_names.insert((source_column_id, shared_data_type), physical_name);
                debug_assert_eq!(virtual_column_mappings.len(), column_id as usize);
                virtual_column_mappings.extend([LeafMapping::Shared, LeafMapping::Shared]);
                for (shared_index, (shared_path, _)) in shared_values.into_iter().enumerate() {
                    let leaf = if matches!(shared_data_type, VirtualColumnSharedDataType::Jsonb) {
                        VirtualColumnNameIndex::Shared(shared_index as u32)
                    } else {
                        VirtualColumnNameIndex::TypedShared {
                            data_type: shared_data_type,
                            index: shared_index as u32,
                        }
                    };
                    Self::insert_virtual_column_node(
                        node,
                        &shared_path,
                        leaf,
                        &mut string_table,
                        &mut string_table_index,
                    );
                }
                leaf_index += 2;
            }

            if !source_sql_null_rows.is_empty() {
                let column = Self::build_validity_column(total_rows, source_sql_null_rows);
                let physical_name = Self::physical_column_name(
                    source_field.column_id,
                    &source_field.name,
                    VIRTUAL_COLUMN_VALIDITY_NAME,
                );
                virtual_fields.push(TableField::new_from_column_id(
                    &physical_name,
                    TableDataType::Nullable(Box::new(TableDataType::Boolean)),
                    leaf_index,
                ));
                virtual_columns.push(BlockEntry::Column(column));
                debug_assert_eq!(virtual_column_mappings.len(), leaf_index as usize);
                virtual_column_mappings.push(LeafMapping::Validity {
                    source_column_id: source_field.column_id,
                });
                leaf_index += 1;
            }

            if !overflow_entries.is_empty() {
                let bucket_count = Self::overflow_bucket_count(overflow_entries.len());
                let mut buckets = vec![Vec::new(); bucket_count];
                for entry in overflow_entries {
                    let bucket = virtual_overflow_bucket(&entry.path, bucket_count);
                    buckets[bucket].push(entry);
                }
                for (bucket, mut entries) in buckets.into_iter().enumerate() {
                    if entries.is_empty() {
                        continue;
                    }
                    entries.sort_by(|a, b| (a.row, &a.path).cmp(&(b.row, &b.path)));
                    let value_state_mask = entries
                        .iter()
                        .fold(0, |mask, entry| mask | entry.state.mask_bit());
                    let column = Self::build_overflow_bucket_column(&entries, total_rows);
                    let physical_name = Self::physical_column_name(
                        source_field.column_id,
                        &source_field.name,
                        &format!("{VIRTUAL_COLUMN_OVERFLOW_NAME}{bucket}"),
                    );
                    virtual_fields.push(TableField::new_from_column_id(
                        &physical_name,
                        Self::overflow_map_type(),
                        leaf_index,
                    ));
                    virtual_columns.push(BlockEntry::Column(column));
                    debug_assert_eq!(virtual_column_mappings.len(), leaf_index as usize);
                    virtual_column_mappings.extend([
                        LeafMapping::OverflowKey {
                            source_column_id: source_field.column_id,
                            bucket,
                            bucket_count,
                            value_state_mask,
                        },
                        LeafMapping::OverflowValue,
                    ]);
                    leaf_index += 2;
                }
            }
        }
        let virtual_block_schema = TableSchemaRefExt::create(virtual_fields);
        debug_assert_eq!(
            virtual_column_mappings.len(),
            virtual_block_schema.leaf_fields().len()
        );
        let virtual_block = DataBlock::new(virtual_columns, total_rows);

        let typed_shared_column_ids =
            Self::build_typed_shared_column_ids(&virtual_block_schema, &shared_column_names);

        let mut metadata = Vec::new();
        // Parquet metadata stores only the trie, string table, and compact shared column ids.
        // Column metas (offset/len/num_values), column ids, and data types
        // are derived from the parquet schema + row group metadata during read.
        let (string_table_key, string_table_json) =
            encode_compact_virtual_column_string_table(&string_table)?;
        metadata.push(KeyValue {
            key: string_table_key,
            value: Some(string_table_json),
        });
        let nodes_json = encode_compact_virtual_column_nodes(&virtual_column_nodes)?;
        metadata.push(KeyValue {
            key: VIRTUAL_COLUMN_NODES_KEY.to_string(),
            value: Some(nodes_json),
        });
        if !typed_shared_column_ids.is_empty() {
            let shared_ids_json =
                encode_compact_virtual_column_shared_ids(&typed_shared_column_ids)?;
            metadata.push(KeyValue {
                key: VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY.to_string(),
                value: Some(shared_ids_json),
            });
        }
        let metadata = Some(metadata);

        // Create the virtual block and convert to parquet
        let columns_statistics = gen_columns_statistics(
            &virtual_block,
            None,
            &virtual_block_schema,
            &BTreeMap::new(),
            HashMap::new(),
        )?;

        let SerializedParquet {
            payload,
            metadata: file_meta,
        } = blocks_to_parquet_with_stats(
            virtual_block_schema.as_ref(),
            vec![virtual_block],
            write_settings.table_compression,
            write_settings.enable_parquet_dictionary,
            metadata,
            Some(&columns_statistics),
            write_settings.data_page_rows,
            write_settings.data_page_bytes,
        )?;

        let (draft_virtual_column_metas, sources) = self.file_meta_to_virtual_column_metas(
            file_meta,
            virtual_column_mappings,
            columns_statistics,
        )?;
        let data = opendal::Buffer::from(payload);
        let data_size = data.len() as u64;
        let virtual_column_location =
            TableMetaLocationGenerator::gen_virtual_block_location(&location.0);

        info!(
            "Generated virtual column data for block {}: virtual_columns={}, extracted_paths={}, rows={}, bytes={}",
            location.0,
            virtual_block_schema.num_fields(),
            extracted_path_count,
            total_rows,
            data_size
        );
        let draft_virtual_block_meta = DraftVirtualBlockMeta {
            virtual_columns: Some(DraftVirtualColumnBlockMeta {
                virtual_column_metas: draft_virtual_column_metas,
                virtual_columns_complete: !needs_footer,
                virtual_column_size: data_size,
                virtual_location: (virtual_column_location, 0),
                sources,
            }),
            path_statistics: (!path_statistics.is_empty()).then_some(path_statistics),
        };

        Ok(VirtualColumnState {
            data,
            draft_virtual_block_meta,
        })
    }

    fn inference_data_type<'v>(
        virtual_values: impl IntoIterator<Item = &'v JsonbScalarValue>,
    ) -> VirtualColumnPhysicalType {
        let mut value_types = HashSet::new();
        let mut element_types = HashSet::new();
        let mut has_array = false;
        for value in virtual_values {
            if let Scalar::Variant(bytes) = &value.scalar {
                if let Ok(JsonbValue::Array(elements)) = RawJsonb::new(bytes).to_value() {
                    has_array = true;
                    for element in elements {
                        // Only scalar arrays are typed. Nested arrays and objects
                        // remain whole Jsonb leaves, without recursive groups.
                        if matches!(&element, JsonbValue::Array(_) | JsonbValue::Object(_)) {
                            return VirtualColumnPhysicalType::Jsonb;
                        }
                        element_types.insert(
                            Self::jsonb_value_to_scalar(element)
                                .as_ref()
                                .infer_data_type(),
                        );
                    }
                    continue;
                }
            }
            value_types.insert(value.scalar.as_ref().infer_data_type());
        }
        if has_array {
            if value_types
                .iter()
                .any(|data_type| *data_type != DataType::Null)
            {
                return VirtualColumnPhysicalType::Jsonb;
            }
            // Empty/all-null arrays do not establish an element type, but may
            // inherit one from other arrays of the same leaf in this block.
            return Self::common_virtual_data_type(&element_types)
                .as_ref()
                .and_then(data_type_to_physical_type)
                .map(|inner| VirtualColumnPhysicalType::Array(Box::new(inner)))
                .unwrap_or(VirtualColumnPhysicalType::Jsonb);
        }

        Self::common_virtual_data_type(&value_types)
            .as_ref()
            .and_then(data_type_to_physical_type)
            .unwrap_or(VirtualColumnPhysicalType::Jsonb)
    }

    /// Infers a common type for JSON scalar values without applying SQL's
    /// cross-family implicit conversions. NULL does not affect inference.
    fn common_virtual_data_type(value_types: &HashSet<DataType>) -> Option<DataType> {
        let mut value_types = value_types
            .iter()
            .map(DataType::remove_nullable)
            .filter(|data_type| !matches!(data_type, DataType::Null));
        let mut common_type = value_types.next()?;
        for value_type in value_types {
            common_type = Self::merge_virtual_data_types(common_type, value_type)?;
        }
        Some(common_type)
    }

    /// Returns a Decimal type that can represent both inputs without reducing
    /// either integer capacity or scale. Values wider than Decimal256 fall back
    /// to Jsonb through the caller.
    fn lossless_decimal_common_type(left: DecimalSize, right: DecimalSize) -> Option<DataType> {
        let scale = left.scale().max(right.scale());
        let leading_digits = left.leading_digits().max(right.leading_digits());
        let precision = leading_digits.checked_add(scale)?;
        let size = DecimalSize::new(precision, scale).ok()?;
        Some(DataType::Decimal(size))
    }

    /// Merges distinct types only in two explicitly supported cases: compatible
    /// Number types, or exact Number/Decimal types. All other combinations
    /// return `None` without consulting broader SQL conversion rules.
    fn merge_virtual_data_types(left: DataType, right: DataType) -> Option<DataType> {
        if left == right {
            return Some(left);
        }

        match (left, right) {
            (DataType::Number(left), DataType::Number(right)) => {
                if left.is_float() != right.is_float() {
                    None
                } else {
                    Some(number_common_type(left, right))
                }
            }
            (DataType::Number(num), DataType::Decimal(decimal))
            | (DataType::Decimal(decimal), DataType::Number(num)) => {
                if num.is_float() {
                    return None;
                }
                let number = num.get_decimal_properties()?;
                Self::lossless_decimal_common_type(number, decimal)
            }
            (DataType::Decimal(left), DataType::Decimal(right)) => {
                Self::lossless_decimal_common_type(left, right)
            }
            _ => None,
        }
    }

    fn build_typed_shared_column_ids(
        schema: &TableSchemaRef,
        shared_column_names: &HashMap<(ColumnId, VirtualColumnSharedDataType), String>,
    ) -> VirtualColumnSharedColumnIdMap {
        if shared_column_names.is_empty() {
            return HashMap::new();
        }

        let mut leaf_name_to_id = HashMap::new();
        for (idx, field) in schema.leaf_fields().iter().enumerate() {
            leaf_name_to_id.insert(field.name.clone(), idx as u32);
        }

        let mut typed_shared_column_ids = HashMap::new();
        for ((source_id, data_type), name) in shared_column_names {
            let key_name = format!("{name}:key");
            let value_name = format!("{name}:value");
            let Some(key_id) = leaf_name_to_id.get(&key_name) else {
                continue;
            };
            let Some(value_id) = leaf_name_to_id.get(&value_name) else {
                continue;
            };
            typed_shared_column_ids
                .entry(*source_id)
                .or_insert_with(HashMap::new)
                .insert(*data_type, (*key_id, *value_id));
        }

        typed_shared_column_ids
    }

    /// Converts the Parquet column chunks of the virtual column file into
    /// draft direct column metas and per-source structure metas.
    fn file_meta_to_virtual_column_metas(
        &self,
        file_meta: ParquetMetaData,
        virtual_column_mappings: Vec<LeafMapping>,
        mut columns_statistics: StatisticsOfColumns,
    ) -> Result<(
        Vec<DraftVirtualColumnMeta>,
        HashMap<ColumnId, VirtualSourceBlockMeta>,
    )> {
        let num_row_groups = file_meta.row_groups().len();
        if num_row_groups != 1 {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "invalid parquet file, expects only one row group, but got {}",
                num_row_groups
            )));
        }
        let row_group = &file_meta.row_groups()[0];
        let chunks = row_group.columns();
        if virtual_column_mappings.len() != chunks.len() {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "virtual column mapping count {} does not match parquet column count {}",
                virtual_column_mappings.len(),
                chunks.len()
            )));
        }

        // Every source with virtual columns gets an entry, which marks the
        // block as written with structure columns.
        let mut sources = self
            .variant_fields
            .iter()
            .map(|field| (field.column_id, VirtualSourceBlockMeta::default()))
            .collect::<HashMap<_, _>>();
        let mut draft_virtual_column_metas = Vec::new();
        for (i, mapping) in virtual_column_mappings.into_iter().enumerate() {
            let chunk_meta = &chunks[i];
            let is_array_element = matches!(&mapping, LeafMapping::ArrayElement(_));
            match mapping {
                LeafMapping::Direct(DirectColumnMapping {
                    source_column_id,
                    path: key_name,
                    physical_type,
                    value_state_mask,
                })
                | LeafMapping::ArrayElement(DirectColumnMapping {
                    source_column_id,
                    path: key_name,
                    physical_type,
                    value_state_mask,
                }) => {
                    let tmp_column_id = i as u32;
                    let (offset, len) = chunk_meta.byte_range();
                    let (data_type, extended_physical_type) = physical_type.encode();
                    // List statistics/counts describe elements, not source rows.
                    let column_stat = columns_statistics.remove(&tmp_column_id).filter(|_| {
                        !is_array_element
                            && !matches!(&physical_type, VirtualColumnPhysicalType::Array(_))
                    });
                    let virtual_column_meta = VirtualColumnMeta {
                        offset,
                        len,
                        num_values: chunk_meta.num_values() as u64,
                        data_type,
                        extended_physical_type,
                        column_stat,
                        value_state_mask,
                    };
                    draft_virtual_column_metas.push(DraftVirtualColumnMeta {
                        source_column_id,
                        name: key_name,
                        data_type: physical_type,
                        column_meta: virtual_column_meta,
                        kind: VirtualPathKind::Value,
                    });
                }
                LeafMapping::Validity { source_column_id } => {
                    let (offset, len) = chunk_meta.byte_range();
                    let (data_type, extended_physical_type) =
                        VirtualColumnPhysicalType::Boolean.encode();
                    sources.entry(source_column_id).or_default().validity =
                        Some(VirtualColumnMeta {
                            offset,
                            len,
                            num_values: chunk_meta.num_values() as u64,
                            data_type,
                            extended_physical_type,
                            column_stat: None,
                            value_state_mask: 0,
                        });
                }
                LeafMapping::OverflowKey {
                    source_column_id,
                    bucket,
                    bucket_count,
                    value_state_mask,
                } => {
                    let value_chunk = chunks.get(i + 1).ok_or_else(|| {
                        ErrorCode::ParquetFileInvalid(
                            "virtual column overflow map is missing its value leaf",
                        )
                    })?;
                    let (key_offset, key_len) = chunk_meta.byte_range();
                    let (value_offset, value_len) = value_chunk.byte_range();
                    let source = sources.entry(source_column_id).or_default();
                    source.overflow_buckets.resize(bucket_count, None);
                    source.overflow_buckets[bucket] = Some(VirtualOverflowBucketMeta {
                        key_offset,
                        key_len,
                        value_offset,
                        value_len,
                        num_values: chunk_meta.num_values() as u64,
                        value_state_mask,
                    });
                }
                LeafMapping::OverflowValue | LeafMapping::Shared => {}
            }
        }
        Ok((draft_virtual_column_metas, sources))
    }
}

fn data_type_to_physical_type(data_type: &DataType) -> Option<VirtualColumnPhysicalType> {
    match data_type.remove_nullable() {
        DataType::Variant => Some(VirtualColumnPhysicalType::Jsonb),
        DataType::Boolean => Some(VirtualColumnPhysicalType::Boolean),
        DataType::Number(number) => Some(VirtualColumnPhysicalType::Number(number)),
        DataType::Decimal(size) => Some(VirtualColumnPhysicalType::Decimal(size.into())),
        DataType::String => Some(VirtualColumnPhysicalType::String),
        DataType::Binary => Some(VirtualColumnPhysicalType::Binary),
        DataType::Date => Some(VirtualColumnPhysicalType::Date),
        DataType::Timestamp => Some(VirtualColumnPhysicalType::Timestamp),
        DataType::TimestampTz => Some(VirtualColumnPhysicalType::TimestampTz),
        DataType::Interval => Some(VirtualColumnPhysicalType::Interval),
        DataType::Array(inner) => Some(VirtualColumnPhysicalType::Array(Box::new(
            data_type_to_physical_type(&inner)?,
        ))),
        _ => None,
    }
}

// Physical name suffixes of per-source structure columns. `#` cannot appear in
// an unquoted canonical path, so they never clash with path column names.
// Readers use metadata, not names.
const VIRTUAL_COLUMN_VALIDITY_NAME: &str = "#validity";
const VIRTUAL_COLUMN_OVERFLOW_NAME: &str = "#overflow";

#[derive(Debug, Clone)]
struct JsonbScalarValue {
    row: usize,
    scalar: Scalar,
    /// `Some` for JSON null, `{}` and `[]`. Such values of direct paths are
    /// stored in the overflow map; shared paths keep them in `scalar`.
    state: Option<VirtualValueState>,
}

type JsonbResult<T> = std::result::Result<T, JsonbError>;

struct ArrayElementColumn {
    /// Relative element path; arrays inside elements stay single leaves,
    /// typed as scalar Array(T) when possible and otherwise kept as Jsonb.
    path: OwnedKeyPaths,
    column: Column,
    table_type: TableDataType,
    physical_type: VirtualColumnPhysicalType,
}

/// Logical identity of one direct Parquet leaf of the virtual column file.
struct DirectColumnMapping {
    source_column_id: ColumnId,
    /// Canonical path; the root is the empty string.
    path: String,
    physical_type: VirtualColumnPhysicalType,
    /// States of the path's values stored in the overflow map.
    value_state_mask: u32,
}

/// The role of one Parquet leaf of the virtual column file.
enum LeafMapping {
    Direct(DirectColumnMapping),
    ArrayElement(DirectColumnMapping),
    /// A key or value leaf of a legacy shared map, described by
    /// `typed_shared_column_ids`.
    Shared,
    /// The root validity column of a source.
    Validity {
        source_column_id: ColumnId,
    },
    /// The key leaf of an overflow bucket; its value leaf follows.
    OverflowKey {
        source_column_id: ColumnId,
        bucket: usize,
        bucket_count: usize,
        value_state_mask: u32,
    },
    OverflowValue,
}

/// One value of a direct path stored in the overflow map.
#[derive(Clone)]
struct OverflowEntry {
    row: usize,
    /// Canonical path of the value; the root is the empty string.
    path: String,
    /// Jsonb bytes of the value.
    value: Vec<u8>,
    state: VirtualValueState,
}

/// Collects the virtual-column leaves of one Variant value of one source column.
///
/// Non-empty objects are expanded, while scalars (including JSON null), empty
/// objects, and whole arrays are leaves. The root is a leaf with an empty path.
/// JSON null, `{}` and `[]` leaves carry their `VirtualValueState`. SQL NULL
/// rows have no jsonb value and are recorded by the builder instead.
struct VirtualValueCollector<'a, 'b> {
    /// Row number across all blocks added to the builder.
    row: usize,
    /// Path hash -> index in `virtual_values`, for this source column.
    hash_to_index: &'b mut StackHashMap<u128, usize, 16>,
    /// Owned path -> index in `virtual_values`, for this source column.
    virtual_paths: &'b mut HashMap<OwnedKeyPaths, usize>,
    virtual_values: &'b mut Vec<Vec<JsonbScalarValue>>,
    /// Reused buffer of the current path as key paths, so the hash matches the
    /// one computed from `OwnedKeyPaths::as_key_paths` in `add_block`.
    key_paths: Vec<KeyPath<'a>>,
}

impl<'a> VirtualValueCollector<'a, '_> {
    fn add_leaf(
        &mut self,
        path: &[JsonbPathSegment<'a>],
        scalar: Scalar,
        state: Option<VirtualValueState>,
    ) -> JsonbResult<()> {
        self.key_paths.clear();
        for segment in path {
            match segment {
                JsonbPathSegment::Key(key) => {
                    self.key_paths.push(KeyPath::Name(Cow::Borrowed(key)))
                }
                // Arrays are leaves and never descended, see `enter_array`.
                JsonbPathSegment::Element => return Err(JsonbError::InvalidKeyPath),
            }
        }

        let scalar_value = JsonbScalarValue {
            row: self.row,
            scalar,
            state,
        };

        // Calculate the hash value and use the hash value as the key
        let mut hasher = SipHasher24::new();
        self.key_paths.as_slice().hash(&mut hasher);
        let hash_value = hasher.finish128().into();

        // Use hash value to lookup instead of key paths
        if let Some(index) = self.hash_to_index.get(&hash_value) {
            self.virtual_values[*index].push(scalar_value);
            return Ok(());
        }

        // The index was not found. Create a new key path
        let index = self.virtual_values.len();
        let owned_key_paths = jsonb::keypath::KeyPaths {
            paths: self.key_paths.clone(),
        }
        .to_owned();
        unsafe {
            match self.hash_to_index.insert_and_entry(hash_value) {
                Ok(e) => *e.get_mut() = index,
                Err(e) => *e.get_mut() = index,
            }
        }
        self.virtual_paths.insert(owned_key_paths, index);
        self.virtual_values.push(vec![scalar_value]);
        Ok(())
    }
}

impl<'a> JsonbStructureVisitor<'a> for VirtualValueCollector<'a, '_> {
    fn visit_scalar(
        &mut self,
        path: &[JsonbPathSegment<'a>],
        value: JsonbValue<'a>,
    ) -> JsonbResult<()> {
        let state = matches!(value, JsonbValue::Null).then_some(VirtualValueState::JsonNull);
        self.add_leaf(
            path,
            VirtualColumnBuilder::jsonb_value_to_scalar(value),
            state,
        )
    }

    fn enter_object(
        &mut self,
        path: &[JsonbPathSegment<'a>],
        len: usize,
        raw: RawJsonb<'a>,
    ) -> JsonbResult<VisitControl> {
        if len > 0 {
            return Ok(VisitControl::Descend);
        }
        // An empty object is a leaf.
        let scalar = VirtualColumnBuilder::jsonb_value_to_scalar(raw.to_value()?);
        self.add_leaf(path, scalar, Some(VirtualValueState::EmptyObject))?;
        Ok(VisitControl::Skip)
    }

    fn enter_array(
        &mut self,
        path: &[JsonbPathSegment<'a>],
        len: usize,
        raw: RawJsonb<'a>,
    ) -> JsonbResult<VisitControl> {
        // Arrays are not expanded; the whole array is a leaf.
        let scalar = VirtualColumnBuilder::jsonb_value_to_scalar(raw.to_value()?);
        let state = (len == 0).then_some(VirtualValueState::EmptyArray);
        self.add_leaf(path, scalar, state)?;
        Ok(VisitControl::Skip)
    }
}

#[cfg(test)]
mod type_inference_tests {
    use std::collections::HashSet;

    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::i256;
    use databend_storages_common_table_meta::meta::VirtualColumnPhysicalType;

    use super::JsonbScalarValue;
    use super::VirtualColumnBuilder;

    fn types(values: impl IntoIterator<Item = DataType>) -> HashSet<DataType> {
        values.into_iter().collect()
    }

    #[test]
    fn common_virtual_type_ignores_null_and_repeated_types() {
        let values = types([
            DataType::Null,
            DataType::Number(NumberDataType::UInt64),
            DataType::Number(NumberDataType::UInt64),
        ]);
        assert_eq!(
            VirtualColumnBuilder::common_virtual_data_type(&values),
            Some(DataType::Number(NumberDataType::UInt64))
        );
        assert_eq!(
            VirtualColumnBuilder::common_virtual_data_type(&types([DataType::Null])),
            None
        );
    }

    #[test]
    fn common_virtual_type_rejects_cross_json_scalar_families() {
        assert_eq!(
            VirtualColumnBuilder::common_virtual_data_type(&types([
                DataType::Number(NumberDataType::UInt64),
                DataType::String,
            ])),
            None
        );
        assert_eq!(
            VirtualColumnBuilder::common_virtual_data_type(&types([
                DataType::Boolean,
                DataType::Number(NumberDataType::UInt64),
            ])),
            None
        );
        assert_eq!(
            VirtualColumnBuilder::common_virtual_data_type(&types([
                DataType::Array(Box::new(DataType::String)),
                DataType::String,
            ])),
            None
        );
    }

    #[test]
    fn common_virtual_type_merges_exact_numbers() {
        let decimal = DecimalSize::new(18, 2).unwrap();
        let result = VirtualColumnBuilder::common_virtual_data_type(&types([
            DataType::Number(NumberDataType::Int64),
            DataType::Decimal(decimal),
        ]))
        .unwrap();
        assert_eq!(result, DataType::Decimal(DecimalSize::new(21, 2).unwrap()));

        let result = VirtualColumnBuilder::common_virtual_data_type(&types([
            DataType::Number(NumberDataType::Int64),
            DataType::Number(NumberDataType::UInt64),
        ]))
        .unwrap();
        assert!(matches!(result, DataType::Decimal(_)));
    }

    #[test]
    fn common_virtual_type_preserves_decimal_integer_capacity() {
        let result = VirtualColumnBuilder::common_virtual_data_type(&types([
            DataType::Decimal(DecimalSize::new(38, 0).unwrap()),
            DataType::Decimal(DecimalSize::new(38, 20).unwrap()),
        ]));
        let expected_size = DecimalSize::new(58, 20).unwrap();
        assert_eq!(result, Some(DataType::Decimal(expected_size)));
        assert_eq!(
            super::data_type_to_physical_type(result.as_ref().unwrap()),
            Some(
                databend_storages_common_table_meta::meta::VirtualColumnPhysicalType::Decimal(
                    DecimalDataType::Decimal256(expected_size)
                )
            )
        );
    }

    #[test]
    fn decimal_merge_is_order_independent() {
        const ORDERS: [[usize; 3]; 6] = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [
            2, 1, 0,
        ]];

        let exact = [
            DataType::Decimal(DecimalSize::new(38, 0).unwrap()),
            DataType::Decimal(DecimalSize::new(38, 20).unwrap()),
            DataType::Number(NumberDataType::Int64),
        ];
        let expected = Some(DataType::Decimal(DecimalSize::new(58, 20).unwrap()));
        for order in ORDERS {
            let first = VirtualColumnBuilder::merge_virtual_data_types(
                exact[order[0]].clone(),
                exact[order[1]].clone(),
            );
            let result = first.and_then(|common| {
                VirtualColumnBuilder::merge_virtual_data_types(common, exact[order[2]].clone())
            });
            assert_eq!(result, expected);
        }

        let unsafe_sets = [
            [
                DataType::Decimal(DecimalSize::new(76, 0).unwrap()),
                DataType::Decimal(DecimalSize::new(76, 1).unwrap()),
                DataType::Number(NumberDataType::Int8),
            ],
            [
                DataType::Decimal(DecimalSize::new(38, 0).unwrap()),
                DataType::Decimal(DecimalSize::new(38, 20).unwrap()),
                DataType::Number(NumberDataType::Float64),
            ],
        ];
        for values in unsafe_sets {
            for order in ORDERS {
                let first = VirtualColumnBuilder::merge_virtual_data_types(
                    values[order[0]].clone(),
                    values[order[1]].clone(),
                );
                let result = first.and_then(|common| {
                    VirtualColumnBuilder::merge_virtual_data_types(common, values[order[2]].clone())
                });
                assert_eq!(result, None);
            }
        }
    }

    #[test]
    fn common_virtual_type_rejects_decimal_beyond_decimal256_capacity() {
        assert_eq!(
            VirtualColumnBuilder::common_virtual_data_type(&types([
                DataType::Decimal(DecimalSize::new(76, 0).unwrap()),
                DataType::Decimal(DecimalSize::new(76, 1).unwrap()),
            ])),
            None
        );
    }

    #[test]
    fn inference_falls_back_to_jsonb_beyond_decimal256_capacity() {
        let values = vec![
            JsonbScalarValue {
                row: 0,
                scalar: Scalar::Decimal(DecimalScalar::Decimal256(
                    i256::from(1),
                    DecimalSize::new(76, 0).unwrap(),
                )),
                state: None,
            },
            JsonbScalarValue {
                row: 1,
                scalar: Scalar::Decimal(DecimalScalar::Decimal256(
                    i256::from(1),
                    DecimalSize::new(76, 1).unwrap(),
                )),
                state: None,
            },
        ];

        assert_eq!(
            VirtualColumnBuilder::inference_data_type(&values),
            VirtualColumnPhysicalType::Jsonb
        );
    }

    #[test]
    fn common_virtual_type_merges_only_compatible_floats() {
        assert_eq!(
            VirtualColumnBuilder::common_virtual_data_type(&types([
                DataType::Number(NumberDataType::Float32),
                DataType::Number(NumberDataType::Float64),
            ])),
            Some(DataType::Number(NumberDataType::Float64))
        );
        assert_eq!(
            VirtualColumnBuilder::common_virtual_data_type(&types([
                DataType::Number(NumberDataType::Float64),
                DataType::Number(NumberDataType::Int64),
            ])),
            None
        );
        assert_eq!(
            VirtualColumnBuilder::common_virtual_data_type(&types([
                DataType::Number(NumberDataType::Float64),
                DataType::Decimal(DecimalSize::new(18, 2).unwrap()),
            ])),
            None
        );
    }

    #[test]
    fn decimal_physical_type_preserves_size() {
        let size = DecimalSize::new(18, 3).unwrap();
        let physical = super::data_type_to_physical_type(&DataType::Decimal(size)).unwrap();
        assert_eq!(
            physical,
            databend_storages_common_table_meta::meta::VirtualColumnPhysicalType::Decimal(
                DecimalDataType::Decimal64(size)
            )
        );
    }
}
