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

use databend_common_catalog::table_context::TableContext;
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
use databend_common_expression::VIRTUAL_COLUMNS_LIMIT;
use databend_common_expression::VariantDataType;
use databend_common_expression::infer_schema_type;
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
use databend_common_expression::types::variant_parquet::parquet_variant_bytes_to_jsonb;
use databend_common_hashtable::StackHashMap;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_storages_common_blocks::blocks_to_parquet_with_stats;
use databend_storages_common_index::VirtualColumnNameIndex;
use databend_storages_common_index::VirtualColumnNode;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::DraftVirtualBlockMeta;
use databend_storages_common_table_meta::meta::DraftVirtualColumnMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
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
use parquet::file::metadata::KeyValue;
use parquet::format::FileMetaData;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use crate::index::VIRTUAL_COLUMN_NODES_KEY;
use crate::index::VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY;
use crate::index::VIRTUAL_COLUMN_STRING_TABLE_KEY;
use crate::io::TableMetaLocationGenerator;
use crate::io::parquet_variant::OwnedKeyPath;
use crate::io::parquet_variant::OwnedKeyPaths;
use crate::io::write::WriteSettings;
use crate::statistics::gen_columns_statistics;

const DEFAULT_VIRTUAL_COLUMN_NUMBER: usize = 32;
const DYNAMIC_PRESENCE_THRESHOLD: f64 = 0.3;
const SAMPLE_ROWS: usize = 10;
const NULL_PERCENTAGE: f64 = 0.7;

#[derive(Debug, Clone)]
pub struct VirtualColumnState {
    pub data: Vec<u8>,
    pub draft_virtual_block_meta: DraftVirtualBlockMeta,
}

#[derive(Debug)]
pub(crate) struct InlineVirtualColumns {
    pub block: DataBlock,
    pub schema: TableSchemaRef,
    pub columns: Vec<InlineVirtualColumn>,
    pub columns_statistics: StatisticsOfColumns,
    pub parquet_metadata: Option<Vec<KeyValue>>,
}

#[derive(Debug, Clone)]
pub(crate) struct InlineVirtualColumn {
    pub source_column_id: ColumnId,
    pub key_paths: OwnedKeyPaths,
    pub key_name: String,
    pub data_type: VariantDataType,
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
    // Paths that are observed as non-scalar (object/array) for each variant field
    non_scalar_paths: Vec<HashSet<OwnedKeyPaths>>,
    // Ignored fields
    ignored_fields: HashSet<usize>,
    // Total number of rows processed
    total_rows: usize,
}

impl VirtualColumnBuilder {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        schema: TableSchemaRef,
    ) -> Result<VirtualColumnBuilder> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(ctx.get_license_key(), Feature::VirtualColumn)?;
        let enable_virtual_column = ctx
            .get_settings()
            .get_enable_experimental_virtual_column()
            .unwrap_or_default();
        let enable_variant_shredding = ctx
            .get_settings()
            .get_enable_experimental_variant_shredding()
            .unwrap_or_default();
        if !enable_virtual_column && !enable_variant_shredding {
            return Err(ErrorCode::VirtualColumnError(
                "Virtual column is an experimental feature, `set enable_experimental_virtual_column=1` \
                 or `set enable_experimental_variant_shredding=1` to use this feature.",
            ));
        }

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
        let mut non_scalar_paths = Vec::with_capacity(variant_fields.len());
        for _ in 0..variant_fields.len() {
            virtual_paths.push(HashMap::with_capacity(DEFAULT_VIRTUAL_COLUMN_NUMBER));
            non_scalar_paths.push(HashSet::new());
        }
        let ignored_fields = HashSet::new();
        let virtual_values = Vec::with_capacity(DEFAULT_VIRTUAL_COLUMN_NUMBER);
        Ok(VirtualColumnBuilder {
            variant_offsets,
            variant_fields,
            virtual_paths,
            virtual_values,
            non_scalar_paths,
            ignored_fields,
            total_rows: 0,
        })
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
                let borrowed_key_paths = virtual_path.to_borrowed_key_paths();

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

        // use first 10 rows as sample to check whether the block is suitable for generating virtual columns
        if self.total_rows < SAMPLE_ROWS {
            let sample_rows = num_rows.min(SAMPLE_ROWS);
            self.extract_virtual_values(block, 0, sample_rows, &mut hash_to_index)?;

            self.check_sample_virtual_values(self.total_rows + sample_rows);
            if sample_rows < num_rows {
                self.extract_virtual_values(block, sample_rows, num_rows, &mut hash_to_index)?;
            }
        } else {
            self.extract_virtual_values(block, 0, num_rows, &mut hash_to_index)?;
        }

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
        let variant_offsets = self.variant_offsets.clone();
        for (i, offset) in variant_offsets.into_iter().enumerate() {
            if self.ignored_fields.contains(&i) {
                continue;
            }
            let column = block.get_by_offset(offset);
            for row in start_row..end_row {
                let val = unsafe { column.index_unchecked(row) };
                let ScalarRef::Variant(jsonb_bytes) = val else {
                    continue;
                };
                let mut current_paths = Vec::with_capacity(4);
                if let Ok(parsed) = jsonb::from_slice(jsonb_bytes) {
                    self.collect_scalar_and_non_scalar_paths(
                        i,
                        &parsed,
                        &mut current_paths,
                        self.total_rows + row,
                        &mut hash_to_index[i],
                    );
                } else {
                    let jsonb = parquet_variant_bytes_to_jsonb(jsonb_bytes).map_err(|e| {
                        ErrorCode::Internal(format!("Failed to decode parquet variant value: {e}"))
                    })?;
                    let parsed = jsonb::from_slice(&jsonb).map_err(|e| {
                        ErrorCode::Internal(format!("Failed to parse JSONB value: {e}"))
                    })?;
                    self.collect_scalar_and_non_scalar_paths(
                        i,
                        &parsed,
                        &mut current_paths,
                        self.total_rows + row,
                        &mut hash_to_index[i],
                    );
                }
            }
        }
        Ok(())
    }

    fn collect_scalar_and_non_scalar_paths(
        &mut self,
        field_index: usize,
        value: &JsonbValue<'_>,
        current_paths: &mut Vec<OwnedKeyPath>,
        row: usize,
        hash_to_index: &mut StackHashMap<u128, usize, 16>,
    ) {
        match value {
            JsonbValue::Array(values) => {
                if !current_paths.is_empty() {
                    self.mark_non_scalar_path(field_index, current_paths);
                }
                for (idx, val) in values.iter().enumerate() {
                    current_paths.push(OwnedKeyPath::Index(idx as i32));
                    self.collect_scalar_and_non_scalar_paths(
                        field_index,
                        val,
                        current_paths,
                        row,
                        hash_to_index,
                    );
                    current_paths.pop();
                }
            }
            JsonbValue::Object(map) => {
                if !current_paths.is_empty() {
                    self.mark_non_scalar_path(field_index, current_paths);
                }
                for (key, val) in map.iter() {
                    current_paths.push(OwnedKeyPath::Name(key.to_string()));
                    self.collect_scalar_and_non_scalar_paths(
                        field_index,
                        val,
                        current_paths,
                        row,
                        hash_to_index,
                    );
                    current_paths.pop();
                }
            }
            _ => {
                if current_paths.is_empty() {
                    return;
                }
                let owned_key_paths = OwnedKeyPaths {
                    paths: current_paths.clone(),
                };
                let scalar = Self::jsonb_value_to_scalar(value);
                let scalar_value = JsonbScalarValue { row, scalar };

                let borrowed_key_paths = owned_key_paths.to_borrowed_key_paths();
                let mut hasher = SipHasher24::new();
                borrowed_key_paths.hash(&mut hasher);
                let hash128 = hasher.finish128();
                let hash_value = hash128.into();

                if let Some(index) = hash_to_index.get(&hash_value) {
                    self.virtual_values[*index].push(scalar_value);
                } else {
                    let index = self.virtual_values.len();
                    unsafe {
                        match hash_to_index.insert_and_entry(hash_value) {
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

                    self.virtual_paths[field_index].insert(owned_key_paths, index);
                    self.virtual_values.push(vec![scalar_value]);
                }
            }
        }
    }

    fn mark_non_scalar_path(&mut self, field_index: usize, current_paths: &[OwnedKeyPath]) {
        let owned_key_paths = OwnedKeyPaths {
            paths: current_paths.to_vec(),
        };
        self.non_scalar_paths[field_index].insert(owned_key_paths);
    }

    fn jsonb_value_to_scalar(value: &JsonbValue<'_>) -> Scalar {
        match value {
            JsonbValue::Null => Scalar::Null,
            JsonbValue::Bool(v) => Scalar::Boolean(*v),
            JsonbValue::String(s) => Scalar::String(s.to_string()),
            JsonbValue::Number(n) => match n {
                JsonbNumber::Int64(v) => Scalar::Number(NumberScalar::Int64(*v)),
                JsonbNumber::UInt64(v) => Scalar::Number(NumberScalar::UInt64(*v)),
                JsonbNumber::Float64(v) => Scalar::Number(NumberScalar::Float64((*v).into())),
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
            JsonbValue::Binary(v) => Scalar::Binary((*v).to_vec()),
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

    fn format_key_name(field_virtual_path: &OwnedKeyPaths) -> String {
        let mut key_name = String::new();
        for path in &field_virtual_path.paths {
            key_name.push('[');
            match path {
                OwnedKeyPath::Index(idx) => {
                    key_name.push_str(&format!("{idx}"));
                }
                OwnedKeyPath::Name(name) => {
                    key_name.push('\'');
                    key_name.push_str(name);
                    key_name.push('\'');
                }
            }
            key_name.push(']');
        }
        key_name
    }

    fn key_path_segment(path: &OwnedKeyPath) -> String {
        match path {
            OwnedKeyPath::Index(idx) => idx.to_string(),
            OwnedKeyPath::Name(name) => name.to_string(),
        }
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

    fn classify_path(
        total_rows: usize,
        value_len: usize,
        value_type: VariantDataType,
    ) -> PathClass {
        let presence = value_len as f64 / total_rows as f64;
        if presence >= DYNAMIC_PRESENCE_THRESHOLD {
            if matches!(value_type, VariantDataType::Jsonb) {
                PathClass::Dynamic
            } else {
                PathClass::Typed(value_type)
            }
        } else {
            PathClass::Shared
        }
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

    fn build_typed_column(
        total_rows: usize,
        values: &[JsonbScalarValue],
        value_type: VariantDataType,
    ) -> (Column, TableDataType) {
        let data_type = match value_type {
            VariantDataType::Boolean => DataType::Nullable(Box::new(DataType::Boolean)),
            VariantDataType::UInt64 => {
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64)))
            }
            VariantDataType::Int64 => {
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64)))
            }
            VariantDataType::Float64 => {
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Float64)))
            }
            VariantDataType::String => DataType::Nullable(Box::new(DataType::String)),
            _ => DataType::Nullable(Box::new(DataType::Variant)),
        };

        let mut builder = ColumnBuilder::with_capacity(&data_type, total_rows);
        let mut last_row = 0usize;
        let null_scalar = ScalarRef::Null;
        for val in values {
            if val.row > last_row {
                let default_len = val.row - last_row;
                builder.push_repeat(&null_scalar, default_len);
                last_row = val.row;
            }
            builder.push(val.scalar.as_ref());
            last_row += 1;
        }
        if last_row < total_rows {
            builder.push_repeat(&null_scalar, total_rows - last_row);
        }
        let column = builder.build();
        let table_type = infer_schema_type(&data_type).unwrap();
        (column, table_type)
    }

    fn build_shared_map_column(
        shared_value_indexes: Vec<usize>,
        virtual_values: &[Vec<JsonbScalarValue>],
        total_rows: usize,
    ) -> Column {
        let mut shared_jsonb_values: HashMap<usize, Vec<(u32, Vec<u8>)>> = HashMap::new();
        for (index, value_index) in shared_value_indexes.into_iter().enumerate() {
            let key_name_index = index as u32;
            let values = &virtual_values[value_index];
            for val in values {
                let jsonb_bytes = Self::scalar_to_variant_bytes(val.scalar.as_ref());
                if let Some(shared_rows) = shared_jsonb_values.get_mut(&val.row) {
                    shared_rows.push((key_name_index, jsonb_bytes));
                } else {
                    let shared_rows = vec![(key_name_index, jsonb_bytes)];
                    shared_jsonb_values.insert(val.row, shared_rows);
                }
            }
        }

        let mut key_builder =
            ColumnBuilder::with_capacity(&DataType::Number(NumberDataType::UInt32), total_rows);
        let mut value_builder = ColumnBuilder::with_capacity(&DataType::Variant, total_rows);
        let mut offsets = Vec::with_capacity(total_rows + 1);
        offsets.push(0);
        let mut current = 0u64;
        for row in 0..total_rows {
            if let Some(shared_rows) = shared_jsonb_values.remove(&row) {
                for (key_name_index, jsonb_bytes) in shared_rows {
                    key_builder.push(ScalarRef::Number(NumberScalar::UInt32(key_name_index)));
                    value_builder.push(ScalarRef::Variant(jsonb_bytes.as_slice()));
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

    #[async_backtrace::framed]
    pub fn finalize(
        &mut self,
        write_settings: &WriteSettings,
        location: &Location,
    ) -> Result<VirtualColumnState> {
        let built = match self.build_virtual_columns()? {
            Some(built) => built,
            None => {
                let draft_virtual_block_meta = DraftVirtualBlockMeta {
                    virtual_column_metas: vec![],
                    virtual_column_size: 0,
                    virtual_location: ("".to_string(), 0),
                };

                return Ok(VirtualColumnState {
                    data: vec![],
                    draft_virtual_block_meta,
                });
            }
        };

        let columns_statistics = built.columns_statistics;
        let virtual_columns = built.columns;
        let virtual_block_schema = built.schema;
        let virtual_block = built.block;
        let parquet_metadata = built.parquet_metadata;

        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let file_meta = blocks_to_parquet_with_stats(
            virtual_block_schema.as_ref(),
            vec![virtual_block],
            &mut data,
            write_settings.table_compression,
            write_settings.enable_parquet_dictionary,
            parquet_metadata,
            Some(&columns_statistics),
        )?;

        let mut source_id_to_name = HashMap::with_capacity(self.variant_fields.len());
        for field in &self.variant_fields {
            source_id_to_name.insert(field.column_id, field.name.clone());
        }
        let mut virtual_column_names = HashMap::with_capacity(virtual_columns.len());
        for column in &virtual_columns {
            if let Some(source_name) = source_id_to_name.get(&column.source_column_id) {
                let virtual_name = format!("{}{}", source_name, column.key_name);
                virtual_column_names.insert(
                    virtual_name,
                    (
                        column.source_column_id,
                        column.key_name.clone(),
                        column.data_type.clone(),
                    ),
                );
            }
        }
        let draft_virtual_column_metas = self.file_meta_to_virtual_column_metas(
            file_meta,
            virtual_column_names,
            columns_statistics,
        )?;

        let data_size = data.len() as u64;
        let virtual_column_location =
            TableMetaLocationGenerator::gen_virtual_block_location(&location.0);

        let draft_virtual_block_meta = DraftVirtualBlockMeta {
            virtual_column_metas: draft_virtual_column_metas,
            virtual_column_size: data_size,
            virtual_location: (virtual_column_location, 0),
        };

        Ok(VirtualColumnState {
            data,
            draft_virtual_block_meta,
        })
    }

    pub(crate) fn build_inline_virtual_columns(&mut self) -> Result<Option<InlineVirtualColumns>> {
        let Some(inline) = self.build_virtual_columns()? else {
            return Ok(None);
        };
        self.filter_inline_virtual_columns_for_shredding(inline)
    }

    pub(crate) fn build_inline_virtual_block_for_columns(
        block: &DataBlock,
        schema: &TableSchemaRef,
        columns: &[InlineVirtualColumn],
    ) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        if columns.is_empty() {
            return Ok(DataBlock::new(Vec::new(), num_rows));
        }

        let mut column_id_to_offset = HashMap::with_capacity(schema.fields().len());
        for (idx, field) in schema.fields().iter().enumerate() {
            column_id_to_offset.insert(field.column_id(), idx);
        }

        let mut source_offsets = Vec::with_capacity(columns.len());
        let mut data_types = Vec::with_capacity(columns.len());
        let mut builders = Vec::with_capacity(columns.len());
        let mut borrowed_paths = Vec::with_capacity(columns.len());
        for column in columns {
            let offset = column_id_to_offset
                .get(&column.source_column_id)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "missing source column id {} for inline virtual column",
                        column.source_column_id
                    ))
                })?;
            source_offsets.push(*offset);
            data_types.push(column.data_type.clone());
            let data_type = match column.data_type {
                VariantDataType::Boolean => DataType::Nullable(Box::new(DataType::Boolean)),
                VariantDataType::UInt64 => {
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64)))
                }
                VariantDataType::Int64 => {
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64)))
                }
                VariantDataType::Float64 => {
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::Float64)))
                }
                VariantDataType::String => DataType::Nullable(Box::new(DataType::String)),
                _ => {
                    return Err(ErrorCode::Internal(
                        "unsupported virtual column type for inline shredding".to_string(),
                    ));
                }
            };
            builders.push(ColumnBuilder::with_capacity(&data_type, num_rows));
            borrowed_paths.push(column.key_paths.to_borrowed_key_paths());
        }

        for row in 0..num_rows {
            let mut decoded_jsonb: Option<Vec<u8>> = None;
            let mut validated_jsonb = false;
            for (idx, builder) in builders.iter_mut().enumerate() {
                let entry = block.get_by_offset(source_offsets[idx]);
                let value = unsafe { entry.index_unchecked(row) };
                let ScalarRef::Variant(jsonb_bytes) = value else {
                    builder.push(ScalarRef::Null);
                    continue;
                };
                if !validated_jsonb {
                    validated_jsonb = true;
                    if jsonb::from_slice(jsonb_bytes).is_err() {
                        if let Ok(jsonb) = parquet_variant_bytes_to_jsonb(jsonb_bytes) {
                            decoded_jsonb = Some(jsonb);
                        }
                    }
                }
                let jsonb_bytes = decoded_jsonb.as_deref().unwrap_or(jsonb_bytes);
                let raw_jsonb = RawJsonb::new(jsonb_bytes);
                let path = &borrowed_paths[idx];
                let raw_value = match raw_jsonb.get_by_keypath(path.paths.iter()) {
                    Ok(Some(value)) => Some(value),
                    Ok(None) => None,
                    Err(_) => None,
                };
                let Some(raw_value) = raw_value else {
                    builder.push(ScalarRef::Null);
                    continue;
                };
                let raw_value = raw_value.as_raw();

                let scalar = match data_types[idx] {
                    VariantDataType::Boolean => match raw_value.as_bool() {
                        Ok(Some(v)) => Some(Scalar::Boolean(v)),
                        _ => None,
                    },
                    VariantDataType::UInt64 => match raw_value.as_number() {
                        Ok(Some(JsonbNumber::UInt64(v))) => {
                            Some(Scalar::Number(NumberScalar::UInt64(v)))
                        }
                        _ => None,
                    },
                    VariantDataType::Int64 => match raw_value.as_number() {
                        Ok(Some(JsonbNumber::Int64(v))) => {
                            Some(Scalar::Number(NumberScalar::Int64(v)))
                        }
                        _ => None,
                    },
                    VariantDataType::Float64 => match raw_value.as_number() {
                        Ok(Some(JsonbNumber::Float64(v))) => {
                            Some(Scalar::Number(NumberScalar::Float64(v.into())))
                        }
                        _ => None,
                    },
                    VariantDataType::String => match raw_value.as_str() {
                        Ok(Some(v)) => Some(Scalar::String(v.into_owned())),
                        _ => None,
                    },
                    _ => None,
                };

                if let Some(scalar) = scalar {
                    builder.push(scalar.as_ref());
                } else {
                    builder.push(ScalarRef::Null);
                }
            }
        }

        let mut virtual_columns = Vec::with_capacity(builders.len());
        for builder in builders {
            virtual_columns.push(builder.build().into());
        }

        Ok(DataBlock::new(virtual_columns, num_rows))
    }

    fn build_virtual_columns(&mut self) -> Result<Option<InlineVirtualColumns>> {
        let mut virtual_paths = Vec::with_capacity(self.variant_fields.len());
        for _ in 0..self.variant_fields.len() {
            virtual_paths.push(HashMap::with_capacity(DEFAULT_VIRTUAL_COLUMN_NUMBER));
        }
        std::mem::swap(&mut self.virtual_paths, &mut virtual_paths);

        let mut virtual_values = Vec::with_capacity(self.virtual_values.len());
        std::mem::swap(&mut self.virtual_values, &mut virtual_values);

        let mut non_scalar_paths = Vec::with_capacity(self.variant_fields.len());
        for _ in 0..self.variant_fields.len() {
            non_scalar_paths.push(HashSet::new());
        }
        std::mem::swap(&mut self.non_scalar_paths, &mut non_scalar_paths);

        let total_rows = self.total_rows;
        self.total_rows = 0;

        if total_rows == 0 || virtual_values.iter().all(|vals| vals.is_empty()) {
            return Ok(None);
        }

        // Process the collected virtual values.
        let virtual_field_num = Self::discard_virtual_values(
            total_rows,
            &virtual_paths,
            &mut virtual_values,
            &non_scalar_paths,
        );
        if virtual_field_num == 0 {
            return Ok(None);
        }

        let mut virtual_columns_meta = Vec::with_capacity(virtual_field_num);
        let mut virtual_fields = Vec::with_capacity(virtual_field_num);
        let mut virtual_columns = Vec::with_capacity(virtual_field_num);
        let mut string_table = Vec::new();
        let mut string_table_index = HashMap::new();
        let mut virtual_column_nodes = HashMap::new();
        let mut shared_column_names = HashMap::new();
        // leaf_index tracks the parquet column id for virtual columns and shared maps.
        // It stays aligned with the VirtualColumnNameIndex used by the trie.
        let mut leaf_index: u32 = 0;
        for (source_field, field_virtual_paths) in
            self.variant_fields.iter().zip(virtual_paths.into_iter())
        {
            // Collect virtual paths and index as BTreeMap to keep order
            let sorted_virtual_paths: BTreeMap<_, _> = field_virtual_paths.into_iter().collect();
            let mut shared_value_indexes: Vec<usize> = Vec::new();
            let mut shared_paths: Vec<OwnedKeyPaths> = Vec::new();
            let node = virtual_column_nodes
                .entry(source_field.column_id)
                .or_insert_with(|| VirtualColumnNode {
                    children: HashMap::new(),
                    leaf: None,
                });
            for (field_virtual_path, index) in sorted_virtual_paths {
                let values = &virtual_values[index];
                if values.is_empty() {
                    continue;
                }
                let key_name = Self::format_key_name(&field_virtual_path);
                let val_type = Self::inference_data_type(values);
                match Self::classify_path(total_rows, values.len(), val_type) {
                    PathClass::Typed(value_type) => {
                        let (column, table_type) =
                            Self::build_typed_column(total_rows, values, value_type.clone());
                        let virtual_name = format!("{}{}", source_field.name, key_name);
                        let column_id = leaf_index;
                        let field =
                            TableField::new_from_column_id(&virtual_name, table_type, column_id);
                        virtual_columns.push(BlockEntry::Column(column));
                        virtual_fields.push(field);
                        let source_column_id = source_field.column_id;
                        virtual_columns_meta.push(InlineVirtualColumn {
                            source_column_id,
                            key_paths: field_virtual_path.clone(),
                            key_name: key_name.clone(),
                            data_type: value_type.clone(),
                        });
                        Self::insert_virtual_column_node(
                            node,
                            &field_virtual_path,
                            VirtualColumnNameIndex::Column(leaf_index),
                            &mut string_table,
                            &mut string_table_index,
                        );
                        leaf_index += 1;
                    }
                    PathClass::Dynamic => {
                        let column = Self::build_variant_column(total_rows, values);
                        let virtual_type =
                            infer_schema_type(&DataType::Nullable(Box::new(DataType::Variant)))
                                .unwrap();
                        let virtual_name = format!("{}{}", source_field.name, key_name);
                        let column_id = leaf_index;
                        let field =
                            TableField::new_from_column_id(&virtual_name, virtual_type, column_id);
                        virtual_columns.push(BlockEntry::Column(column));
                        virtual_fields.push(field);
                        let source_column_id = source_field.column_id;
                        virtual_columns_meta.push(InlineVirtualColumn {
                            source_column_id,
                            key_paths: field_virtual_path.clone(),
                            key_name: key_name.clone(),
                            data_type: VariantDataType::Jsonb,
                        });
                        Self::insert_virtual_column_node(
                            node,
                            &field_virtual_path,
                            VirtualColumnNameIndex::Column(leaf_index),
                            &mut string_table,
                            &mut string_table_index,
                        );
                        leaf_index += 1;
                    }
                    PathClass::Shared => {
                        shared_value_indexes.push(index);
                        shared_paths.push(field_virtual_path);
                    }
                }
            }

            if !shared_value_indexes.is_empty() {
                let column = Self::build_shared_map_column(
                    shared_value_indexes,
                    &virtual_values,
                    total_rows,
                );

                let map_type = TableDataType::Map(Box::new(TableDataType::Tuple {
                    fields_name: vec!["key".to_string(), "value".to_string()],
                    fields_type: vec![
                        TableDataType::Number(NumberDataType::UInt32),
                        TableDataType::Variant,
                    ],
                }));

                let virtual_name = format!("{}.__shared_virtual_column_data__", source_field.name);
                let column_id = leaf_index;
                let field = TableField::new_from_column_id(&virtual_name, map_type, column_id);
                virtual_columns.push(BlockEntry::Column(column));
                virtual_fields.push(field);
                let source_column_id = source_field.column_id;
                shared_column_names.insert(source_column_id, virtual_name);
                for (shared_index, shared_path) in shared_paths.into_iter().enumerate() {
                    Self::insert_virtual_column_node(
                        node,
                        &shared_path,
                        VirtualColumnNameIndex::Shared(shared_index as u32),
                        &mut string_table,
                        &mut string_table_index,
                    );
                }
                leaf_index += 2;
            }
        }
        let virtual_block_schema = TableSchemaRefExt::create(virtual_fields);
        let virtual_block = DataBlock::new(virtual_columns, total_rows);
        let columns_statistics =
            gen_columns_statistics(&virtual_block, None, &virtual_block_schema)?;

        let shared_column_ids =
            Self::build_shared_column_ids(&virtual_block_schema, &shared_column_names);

        let mut metadata = Vec::new();
        // Parquet metadata stores only the trie (virtual_column_nodes), string table, and
        // shared column ids. Column metas (offset/len/num_values), column ids, and data types
        // are derived from the parquet schema + row group metadata during read.
        let string_table_json = serde_json::to_string(&string_table)?;
        metadata.push(KeyValue {
            key: VIRTUAL_COLUMN_STRING_TABLE_KEY.to_string(),
            value: Some(string_table_json),
        });
        let nodes_json = serde_json::to_string(&virtual_column_nodes)?;
        metadata.push(KeyValue {
            key: VIRTUAL_COLUMN_NODES_KEY.to_string(),
            value: Some(nodes_json),
        });
        if !shared_column_ids.is_empty() {
            let shared_ids_json = serde_json::to_string(&shared_column_ids)?;
            metadata.push(KeyValue {
                key: VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY.to_string(),
                value: Some(shared_ids_json),
            });
        }

        Ok(Some(InlineVirtualColumns {
            block: virtual_block,
            schema: virtual_block_schema,
            columns: virtual_columns_meta,
            columns_statistics,
            parquet_metadata: Some(metadata),
        }))
    }

    fn filter_inline_virtual_columns_for_shredding(
        &self,
        inline: InlineVirtualColumns,
    ) -> Result<Option<InlineVirtualColumns>> {
        let mut keep_indices = Vec::with_capacity(inline.columns.len());
        let mut kept_columns = Vec::with_capacity(inline.columns.len());
        for (idx, column) in inline.columns.iter().enumerate() {
            if matches!(column.data_type, VariantDataType::Jsonb) {
                continue;
            }
            if column.key_paths.has_index() {
                continue;
            }
            keep_indices.push(idx);
            kept_columns.push(column.clone());
        }

        if keep_indices.is_empty() {
            return Ok(None);
        }

        let mut new_fields = Vec::with_capacity(keep_indices.len());
        let mut new_columns = Vec::with_capacity(keep_indices.len());
        for (new_idx, old_idx) in keep_indices.iter().enumerate() {
            let old_field = inline.schema.fields().get(*old_idx).ok_or_else(|| {
                ErrorCode::Internal("inline virtual column field index out of bounds".to_string())
            })?;
            let new_field = TableField::new_from_column_id(
                old_field.name(),
                old_field.data_type().clone(),
                new_idx as u32,
            );
            new_fields.push(new_field);
            let entry = inline
                .block
                .columns()
                .get(*old_idx)
                .ok_or_else(|| {
                    ErrorCode::Internal(
                        "inline virtual column entry index out of bounds".to_string(),
                    )
                })?
                .clone();
            new_columns.push(entry);
        }

        let new_schema = TableSchemaRefExt::create(new_fields);
        let new_block = DataBlock::new(new_columns, inline.block.num_rows());
        let columns_statistics = gen_columns_statistics(&new_block, None, &new_schema)?;

        Ok(Some(InlineVirtualColumns {
            block: new_block,
            schema: new_schema,
            columns: kept_columns,
            columns_statistics,
            parquet_metadata: None,
        }))
    }

    fn check_sample_virtual_values(&mut self, sample_rows: usize) {
        // ignore small samples
        if sample_rows < SAMPLE_ROWS {
            return;
        }

        for (i, _) in self.variant_offsets.iter().enumerate() {
            // all samples are scalar value
            if self.virtual_paths[i].is_empty() {
                self.ignored_fields.insert(i);
            }
            let mut most_null_count = 0;
            for (_, index) in self.virtual_paths[i].iter() {
                let value_count = self.virtual_values[*index].len();
                let null_count = sample_rows - value_count;
                let null_percentage = null_count as f64 / sample_rows as f64;
                if null_percentage > NULL_PERCENTAGE {
                    most_null_count += 1;
                }
            }
            let most_null_percentage = most_null_count as f64 / self.virtual_paths[i].len() as f64;
            // most virtual values are null
            if most_null_percentage > NULL_PERCENTAGE {
                for (_, index) in self.virtual_paths[i].iter() {
                    self.virtual_values[*index].clear();
                }
                self.ignored_fields.insert(i);
            }
        }
    }

    fn discard_virtual_values(
        num_rows: usize,
        virtual_paths: &[HashMap<OwnedKeyPaths, usize>],
        virtual_values: &mut [Vec<JsonbScalarValue>],
        non_scalar_paths: &[HashSet<OwnedKeyPaths>],
    ) -> usize {
        if virtual_values.is_empty() {
            return 0;
        }

        // 0. Discard virtual columns that have any non-scalar values.
        for (idx, virtual_paths) in virtual_paths.iter().enumerate() {
            let Some(non_scalar) = non_scalar_paths.get(idx) else {
                continue;
            };
            if non_scalar.is_empty() {
                continue;
            }
            for (virtual_path, index) in virtual_paths {
                if non_scalar.contains(virtual_path) {
                    virtual_values[*index].clear();
                }
            }
        }

        // 1. Discard virtual columns with most values are Null values.
        for values in virtual_values.iter_mut() {
            if values.is_empty() {
                continue;
            }
            let not_null_count = values
                .iter()
                .filter(|x| !matches!(x.scalar, Scalar::Null))
                .count();
            let null_count = num_rows - not_null_count;
            let null_percentage = null_count as f64 / num_rows as f64;
            if null_percentage > NULL_PERCENTAGE {
                values.clear();
            }
        }

        // 2. Discard names with the same prefix and ensure that the values of the virtual columns are leaf nodes
        // for example, we have following variant values.
        // {"k1":{"k2":"val"}}
        // {"k1":100}
        // we should not create virtual column for `k1`.
        for virtual_paths in virtual_paths {
            for (virtual_path, index) in virtual_paths {
                if virtual_values[*index].is_empty() {
                    continue;
                }
                for other_virtual_path in virtual_paths.keys() {
                    if virtual_path.is_prefix_path(other_virtual_path) {
                        virtual_values[*index].clear();
                    }
                }
            }
        }

        // 3. Discard redundant virtual values to avoid generating too much virtual fields.
        let virtual_values_count = virtual_values.iter().filter(|x| !x.is_empty()).count();
        if virtual_values_count > VIRTUAL_COLUMNS_LIMIT {
            let mut redundant_num = virtual_values_count - VIRTUAL_COLUMNS_LIMIT;
            for virtual_value in virtual_values.iter_mut().rev() {
                if redundant_num == 0 {
                    break;
                }
                if !virtual_value.is_empty() {
                    virtual_value.clear();
                    redundant_num -= 1;
                }
            }
            VIRTUAL_COLUMNS_LIMIT
        } else {
            virtual_values_count
        }
    }
    fn inference_data_type(virtual_values: &[JsonbScalarValue]) -> VariantDataType {
        let mut val_type_set = HashSet::new();
        for val in virtual_values.iter() {
            let ty = match val.scalar {
                Scalar::Boolean(_) => VariantDataType::Boolean,
                Scalar::Number(NumberScalar::UInt64(_)) => VariantDataType::UInt64,
                Scalar::Number(NumberScalar::Int64(_)) => VariantDataType::Int64,
                Scalar::Number(NumberScalar::Float64(_)) => VariantDataType::Float64,
                Scalar::String(_) => VariantDataType::String,
                // Decimal, binary, date, timestamp, and interval types will not
                // be generated for now, because older meta cannot recognize these types.
                // Support for these types will be added after the meta upgrade.
                _ => {
                    return VariantDataType::Jsonb;
                }
            };
            if !val_type_set.contains(&ty) {
                val_type_set.insert(ty);
            }
            if val_type_set.len() == 2 {
                break;
            }
        }
        if val_type_set.len() == 1 {
            val_type_set.into_iter().next().unwrap()
        } else {
            VariantDataType::Jsonb
        }
    }

    fn build_shared_column_ids(
        schema: &TableSchemaRef,
        shared_column_names: &HashMap<ColumnId, String>,
    ) -> HashMap<u32, (u32, u32)> {
        if shared_column_names.is_empty() {
            return HashMap::new();
        }

        let mut leaf_name_to_id = HashMap::new();
        for (idx, field) in schema.leaf_fields().iter().enumerate() {
            leaf_name_to_id.insert(field.name.clone(), idx as u32);
        }

        let mut shared_column_ids = HashMap::new();
        for (source_id, name) in shared_column_names {
            let key_name = format!("{name}:key");
            let value_name = format!("{name}:value");
            let Some(key_id) = leaf_name_to_id.get(&key_name) else {
                continue;
            };
            let Some(value_id) = leaf_name_to_id.get(&value_name) else {
                continue;
            };
            shared_column_ids.insert(*source_id, (*key_id, *value_id));
        }

        shared_column_ids
    }

    fn file_meta_to_virtual_column_metas(
        &self,
        file_meta: FileMetaData,
        mut virtual_column_names: HashMap<String, (u32, String, VariantDataType)>,
        mut columns_statistics: StatisticsOfColumns,
    ) -> Result<Vec<DraftVirtualColumnMeta>> {
        let num_row_groups = file_meta.row_groups.len();
        if num_row_groups != 1 {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "invalid parquet file, expects only one row group, but got {}",
                num_row_groups
            )));
        }
        let row_group = &file_meta.row_groups[0];

        let mut draft_virtual_column_metas = Vec::with_capacity(virtual_column_names.len());
        for (i, col_chunk) in row_group.columns.iter().enumerate() {
            let tmp_column_id = i as u32;
            match &col_chunk.meta_data {
                Some(chunk_meta) => {
                    let Some((source_column_id, key_name, variant_type)) =
                        virtual_column_names.remove(&chunk_meta.path_in_schema[0])
                    else {
                        continue;
                    };

                    let col_start =
                        if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
                            dict_page_offset
                        } else {
                            chunk_meta.data_page_offset
                        };
                    let col_len = chunk_meta.total_compressed_size;
                    assert!(
                        col_start >= 0 && col_len >= 0,
                        "column start and length should not be negative"
                    );
                    let num_values = chunk_meta.num_values as u64;

                    let variant_type_code = VirtualColumnMeta::data_type_code(&variant_type);
                    let column_stat = columns_statistics.remove(&tmp_column_id);
                    let virtual_column_meta = VirtualColumnMeta {
                        offset: col_start as u64,
                        len: col_len as u64,
                        num_values,
                        data_type: variant_type_code,
                        column_stat,
                    };

                    let draft_virtual_column_meta = DraftVirtualColumnMeta {
                        source_column_id,
                        name: key_name,
                        data_type: variant_type,
                        column_meta: virtual_column_meta,
                    };
                    draft_virtual_column_metas.push(draft_virtual_column_meta);
                }
                None => {
                    return Err(ErrorCode::ParquetFileInvalid(format!(
                        "invalid parquet file, meta data of column is empty",
                    )));
                }
            }
        }
        Ok(draft_virtual_column_metas)
    }

    pub(crate) fn column_metas_to_virtual_column_metas(
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        virtual_columns: Vec<InlineVirtualColumn>,
        mut columns_statistics: StatisticsOfColumns,
        column_id_base: ColumnId,
        _value_all_null_sources: Option<&HashSet<ColumnId>>,
    ) -> Result<Vec<DraftVirtualColumnMeta>> {
        let mut draft_virtual_column_metas = Vec::with_capacity(virtual_columns.len());
        for (i, virtual_column) in virtual_columns.into_iter().enumerate() {
            let tmp_column_id = i as u32;
            let column_id = column_id_base + i as u32;
            let column_meta = column_metas.get(&column_id).ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "missing column meta for virtual column id {}",
                    column_id
                ))
            })?;
            let ColumnMeta::Parquet(col_meta) = column_meta else {
                return Err(ErrorCode::ParquetFileInvalid(
                    "virtual columns should be stored in parquet format".to_string(),
                ));
            };
            let variant_type_code = VirtualColumnMeta::data_type_code(&virtual_column.data_type);
            let column_stat = columns_statistics.remove(&tmp_column_id);
            let virtual_column_meta = VirtualColumnMeta {
                offset: col_meta.offset,
                len: col_meta.len,
                num_values: col_meta.num_values,
                data_type: variant_type_code,
                column_stat,
            };

            let draft_virtual_column_meta = DraftVirtualColumnMeta {
                source_column_id: virtual_column.source_column_id,
                name: virtual_column.key_name,
                data_type: virtual_column.data_type,
                column_meta: virtual_column_meta,
            };
            draft_virtual_column_metas.push(draft_virtual_column_meta);
        }
        Ok(draft_virtual_column_metas)
    }
}

#[derive(Debug, Clone)]
struct JsonbScalarValue {
    row: usize,
    scalar: Scalar,
}

// PathClass decides how a JSON path is materialized:
// - Typed: extracted as a dedicated column with a concrete scalar type.
// - Dynamic: extracted as a dedicated column but kept as JSONB/Variant.
// - Shared: stored in the shared map column because it is too sparse.
enum PathClass {
    Typed(VariantDataType),
    Dynamic,
    Shared,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use databend_common_expression::Scalar;
    use databend_common_expression::VIRTUAL_COLUMN_ID_START;
    use databend_common_expression::types::NumberScalar;
    use databend_storages_common_table_meta::meta::ColumnMeta;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use databend_storages_common_table_meta::meta::StatisticsOfColumns;
    use serde_json;

    use super::*;

    #[test]
    fn test_inline_virtual_column_stats_kept_for_scalar_paths() -> Result<()> {
        let column_id_base = VIRTUAL_COLUMN_ID_START;
        let mut column_metas = HashMap::new();
        let column_meta: ColumnMeta =
            serde_json::from_str(r#"{"Parquet":{"offset":0,"len":10,"num_values":3}}"#)
                .map_err(|e| ErrorCode::Internal(format!("invalid column meta json: {e}")))?;
        column_metas.insert(column_id_base, column_meta);

        let inline_column = InlineVirtualColumn {
            source_column_id: 1,
            key_paths: OwnedKeyPaths {
                paths: vec![OwnedKeyPath::Name("a".to_string())],
            },
            key_name: "['a']".to_string(),
            data_type: VariantDataType::Int64,
        };

        let mut stats: StatisticsOfColumns = HashMap::new();
        stats.insert(
            0,
            ColumnStatistics::new(
                Scalar::Number(NumberScalar::Int64(1)),
                Scalar::Number(NumberScalar::Int64(2)),
                0,
                0,
                None,
            ),
        );

        let value_all_null_sources = HashSet::from([2_u32]);
        let stats_kept = VirtualColumnBuilder::column_metas_to_virtual_column_metas(
            &column_metas,
            vec![inline_column.clone()],
            stats.clone(),
            column_id_base,
            Some(&value_all_null_sources),
        )?;
        assert!(stats_kept[0].column_meta.column_stat.is_some());

        let value_all_null_sources = HashSet::from([1_u32]);
        let stats_kept = VirtualColumnBuilder::column_metas_to_virtual_column_metas(
            &column_metas,
            vec![inline_column],
            stats,
            column_id_base,
            Some(&value_all_null_sources),
        )?;
        assert!(stats_kept[0].column_meta.column_stat.is_some());

        Ok(())
    }
}
