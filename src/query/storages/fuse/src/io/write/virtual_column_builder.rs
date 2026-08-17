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
use databend_common_expression::type_check::common_super_type;
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
use jsonb::Date as JsonbDate;
use jsonb::Decimal64 as JsonbDecimal64;
use jsonb::Decimal128 as JsonbDecimal128;
use jsonb::Decimal256 as JsonbDecimal256;
use jsonb::Interval as JsonbInterval;
use jsonb::Number as JsonbNumber;
use jsonb::RawJsonb;
use jsonb::Timestamp as JsonbTimestamp;
use jsonb::TimestampTz as JsonbTimestampTz;
use jsonb::Value as JsonbValue;
use jsonb::keypath::OwnedKeyPath;
use jsonb::keypath::OwnedKeyPaths;
use log::info;
use parquet::file::metadata::KeyValue;
use parquet::file::metadata::ParquetMetaData;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use crate::MAX_VIRTUAL_COLUMN_DIRECT_COLUMNS;
use crate::MAX_VIRTUAL_COLUMN_PATH_STATISTICS;
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
        Ok(VirtualColumnBuilder {
            variant_offsets,
            variant_fields,
            virtual_paths,
            virtual_values,
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
                let ScalarRef::Variant(jsonb_bytes) = val else {
                    continue;
                };
                let raw_jsonb = RawJsonb::new(jsonb_bytes);

                raw_jsonb
                    .visit_scalar_key_values(true, |key_paths, jsonb_value| {
                        let scalar = Self::jsonb_value_to_scalar(jsonb_value);
                        // Blocks are added repeatedly, so the actual rows need to add the rows of the previous blocks
                        let scalar_value = JsonbScalarValue {
                            row: self.total_rows + row,
                            scalar,
                        };

                        // Calculate the hash value and use the hash value as the key
                        let mut hasher = SipHasher24::new();
                        key_paths.hash(&mut hasher);
                        let hash128 = hasher.finish128();
                        let hash_value = hash128.into();

                        // Use hash value to lookup instead of key paths
                        if let Some(index) = hash_to_index[i].get(&hash_value) {
                            self.virtual_values[*index].push(scalar_value);
                        } else {
                            // The index was not found. Create a new key path
                            let index = self.virtual_values.len();
                            let owned_key_paths = jsonb::keypath::KeyPaths {
                                paths: key_paths.to_vec(),
                            }
                            .to_owned();

                            unsafe {
                                match hash_to_index[i].insert_and_entry(hash_value) {
                                    Ok(e) => {
                                        let v = e.get_mut();
                                        *v = index;
                                    }
                                    Err(e) => {
                                        let v = e.get_mut();
                                        *v = index;
                                    }
                                }
                            }

                            self.virtual_paths[i].insert(owned_key_paths, index);
                            self.virtual_values.push(vec![scalar_value]);
                        }
                        Ok(())
                    })
                    .map_err(|error| {
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

    fn insert_virtual_column_node(
        root: &mut VirtualColumnNode,
        path: &OwnedKeyPaths,
        leaf: VirtualColumnNameIndex,
        string_table: &mut Vec<String>,
        string_table_index: &mut HashMap<String, u32>,
    ) {
        // Build a trie from key path segments. Each segment is stored once in the
        // string table and referenced by id to keep parquet metadata compact.
        let mut current = root;
        for segment in &path.paths {
            let segment_name = Self::key_path_segment(segment);
            let segment_id =
                Self::get_string_table_id(&segment_name, string_table, string_table_index);
            current = current
                .children
                .entry(segment_id)
                .or_insert_with(|| VirtualColumnNode {
                    children: HashMap::new(),
                    leaf: None,
                });
        }
        current.leaf = Some(leaf);
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

    fn build_variant_column(total_rows: usize, values: &[JsonbScalarValue]) -> Column {
        let mut bitmap = MutableBitmap::from_len_zeroed(total_rows);
        let mut builder =
            BinaryColumnBuilder::with_capacity(total_rows, values.len().saturating_mul(10));
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

    fn build_direct_column(
        total_rows: usize,
        values: &[JsonbScalarValue],
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

    fn build_typed_column(
        total_rows: usize,
        values: &[JsonbScalarValue],
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
                let scalar = databend_common_expression::cast_scalar(
                    None,
                    val.scalar.clone(),
                    &data_type.remove_nullable(),
                    &BUILTIN_FUNCTIONS,
                )?;
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

    fn shared_column_name(source_name: &str, data_type: VirtualColumnSharedDataType) -> String {
        let suffix = match data_type {
            VirtualColumnSharedDataType::Boolean => "__shared_bool_virtual_column_data__",
            VirtualColumnSharedDataType::UInt64 => "__shared_uint64_virtual_column_data__",
            VirtualColumnSharedDataType::Int64 => "__shared_int64_virtual_column_data__",
            VirtualColumnSharedDataType::Float64 => "__shared_float64_virtual_column_data__",
            VirtualColumnSharedDataType::String => "__shared_string_virtual_column_data__",
            VirtualColumnSharedDataType::Jsonb => "__shared_virtual_column_data__",
        };
        format!("{source_name}.{suffix}")
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
        let path_statistics = Self::build_path_statistics(
            &self.variant_fields,
            &virtual_paths,
            &virtual_values,
            &direct_paths,
            self.max_path_statistics,
        );

        let mut virtual_column_names = HashMap::new();
        let mut virtual_fields = Vec::new();
        let mut virtual_columns = Vec::new();
        let mut string_table = Vec::new();
        let mut string_table_index = HashMap::new();
        let mut virtual_column_nodes = HashMap::new();
        let mut shared_column_names = HashMap::new();
        let mut has_shared_paths = false;
        // leaf_index tracks the parquet column id for virtual columns and shared maps.
        // It stays aligned with the VirtualColumnNameIndex used by the trie.
        let mut leaf_index: u32 = 0;
        for (source_field, field_virtual_paths) in
            self.variant_fields.iter().zip(virtual_paths.into_iter())
        {
            let sorted_virtual_paths: BTreeMap<_, _> = field_virtual_paths.into_iter().collect();
            let mut shared_values_by_type: BTreeMap<
                VirtualColumnSharedDataType,
                Vec<(OwnedKeyPaths, usize)>,
            > = BTreeMap::new();
            let node = virtual_column_nodes
                .entry(source_field.column_id)
                .or_insert_with(|| VirtualColumnNode {
                    children: HashMap::new(),
                    leaf: None,
                });
            for (path, value_index) in sorted_virtual_paths {
                let values = &virtual_values[value_index];
                if values.is_empty() {
                    continue;
                }
                let canonical_path = path.to_canonical_path();
                let physical_type = if values.iter().any(|value| value.scalar.is_null()) {
                    // Nullable typed columns cannot distinguish an absent path
                    // from an explicit JSON null. Jsonb preserves all three
                    // states: missing, JSON null, and a present non-null value.
                    VirtualColumnPhysicalType::Jsonb
                } else {
                    Self::inference_data_type(values)
                };
                if effective_layout.contains(source_field.column_id, &canonical_path) {
                    let (column, table_type) =
                        Self::build_direct_column(total_rows, values, &physical_type)?;
                    let virtual_name = format!("{}.{}", source_field.name, canonical_path);
                    let column_id = leaf_index;
                    let field =
                        TableField::new_from_column_id(&virtual_name, table_type, column_id);
                    virtual_columns.push(BlockEntry::Column(column));
                    virtual_fields.push(field);
                    virtual_column_names.insert(
                        virtual_name,
                        (source_field.column_id, canonical_path, physical_type),
                    );
                    Self::insert_virtual_column_node(
                        node,
                        &path,
                        VirtualColumnNameIndex::Column(leaf_index),
                        &mut string_table,
                        &mut string_table_index,
                    );
                    leaf_index += 1;
                } else {
                    let shared_type = Self::shared_data_type_from_variant_type(&physical_type);
                    shared_values_by_type
                        .entry(shared_type)
                        .or_default()
                        .push((path, value_index));
                }
            }

            for (shared_data_type, shared_values) in shared_values_by_type {
                if shared_values.is_empty() {
                    continue;
                }
                has_shared_paths = true;

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

                let virtual_name = Self::shared_column_name(&source_field.name, shared_data_type);
                let column_id = leaf_index;
                let field = TableField::new_from_column_id(&virtual_name, map_type, column_id);
                virtual_columns.push(BlockEntry::Column(column));
                virtual_fields.push(field);
                let source_column_id = source_field.column_id;
                shared_column_names.insert((source_column_id, shared_data_type), virtual_name);
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
        }
        let virtual_block_schema = TableSchemaRefExt::create(virtual_fields);
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

        let draft_virtual_column_metas = self.file_meta_to_virtual_column_metas(
            file_meta,
            virtual_column_names,
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
                virtual_columns_complete: !has_shared_paths,
                virtual_column_size: data_size,
                virtual_location: (virtual_column_location, 0),
            }),
            path_statistics: (!path_statistics.is_empty()).then_some(path_statistics),
        };

        Ok(VirtualColumnState {
            data,
            draft_virtual_block_meta,
        })
    }

    fn inference_data_type(virtual_values: &[JsonbScalarValue]) -> VirtualColumnPhysicalType {
        let value_types = virtual_values
            .iter()
            .map(|value| value.scalar.as_ref().infer_data_type())
            .collect::<HashSet<_>>();

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
            (left @ DataType::Number(num), right @ DataType::Decimal(_))
            | (left @ DataType::Decimal(_), right @ DataType::Number(num)) => {
                if !num.is_float() {
                    common_super_type(left, right, &[])
                } else {
                    None
                }
            }
            (left @ DataType::Decimal(_), right @ DataType::Decimal(_)) => {
                common_super_type(left, right, &[])
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

    fn file_meta_to_virtual_column_metas(
        &self,
        file_meta: ParquetMetaData,
        mut virtual_column_names: HashMap<String, (u32, String, VirtualColumnPhysicalType)>,
        mut columns_statistics: StatisticsOfColumns,
    ) -> Result<Vec<DraftVirtualColumnMeta>> {
        let num_row_groups = file_meta.row_groups().len();
        if num_row_groups != 1 {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "invalid parquet file, expects only one row group, but got {}",
                num_row_groups
            )));
        }
        let row_group = &file_meta.row_groups()[0];

        let mut draft_virtual_column_metas = Vec::with_capacity(virtual_column_names.len());
        for (i, chunk_meta) in row_group.columns().iter().enumerate() {
            let tmp_column_id = i as u32;
            let Some((source_column_id, key_name, variant_type)) =
                virtual_column_names.remove(&chunk_meta.column_path().parts()[0])
            else {
                continue;
            };

            let (offset, len) = chunk_meta.byte_range();
            let physical_type = variant_type;
            let (data_type, extended_physical_type) = physical_type.encode();
            let column_stat = columns_statistics.remove(&tmp_column_id);
            let virtual_column_meta = VirtualColumnMeta {
                offset,
                len,
                num_values: chunk_meta.num_values() as u64,
                data_type,
                extended_physical_type,
                column_stat,
            };

            let draft_virtual_column_meta = DraftVirtualColumnMeta {
                source_column_id,
                name: key_name,
                data_type: physical_type,
                column_meta: virtual_column_meta,
            };
            draft_virtual_column_metas.push(draft_virtual_column_meta);
        }
        Ok(draft_virtual_column_metas)
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

#[derive(Debug, Clone)]
struct JsonbScalarValue {
    row: usize,
    scalar: Scalar,
}

#[cfg(test)]
mod type_inference_tests {
    use std::collections::HashSet;

    use databend_common_expression::types::DataType;
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberDataType;

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
        assert!(matches!(result, DataType::Decimal(_)));

        let result = VirtualColumnBuilder::common_virtual_data_type(&types([
            DataType::Number(NumberDataType::Int64),
            DataType::Number(NumberDataType::UInt64),
        ]))
        .unwrap();
        assert!(matches!(result, DataType::Decimal(_)));
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
