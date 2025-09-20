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
use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::i256;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
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
use databend_common_expression::VariantDataType;
use databend_common_expression::VIRTUAL_COLUMNS_LIMIT;
use databend_common_hashtable::StackHashMap;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_table_meta::meta::DraftVirtualBlockMeta;
use databend_storages_common_table_meta::meta::DraftVirtualColumnMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use jsonb::keypath::KeyPath as JsonbKeyPath;
use jsonb::keypath::KeyPaths as JsonbKeyPaths;
use jsonb::Date as JsonbDate;
use jsonb::Decimal128 as JsonbDecimal128;
use jsonb::Decimal256 as JsonbDecimal256;
use jsonb::Decimal64 as JsonbDecimal64;
use jsonb::Interval as JsonbInterval;
use jsonb::Number as JsonbNumber;
use jsonb::RawJsonb;
use jsonb::Timestamp as JsonbTimestamp;
use jsonb::Value as JsonbValue;
use parquet::format::FileMetaData;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use crate::io::write::WriteSettings;
use crate::io::TableMetaLocationGenerator;
use crate::statistics::gen_columns_statistics;

const SAMPLE_ROWS: usize = 10;
const NULL_PERCENTAGE: f64 = 0.7;

#[derive(Debug, Clone)]
pub struct VirtualColumnState {
    pub data: Vec<u8>,
    pub draft_virtual_block_meta: DraftVirtualBlockMeta,
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
        if !ctx
            .get_settings()
            .get_enable_experimental_virtual_column()
            .unwrap_or_default()
        {
            return Err(ErrorCode::VirtualColumnError(
                "Virtual column is an experimental feature, `set enable_experimental_virtual_column=1` to use this feature."
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
            return Err(ErrorCode::VirtualColumnError("Virtual column only support variant type, but this table don't have variant type fields"));
        }
        let mut virtual_paths = Vec::with_capacity(variant_fields.len());
        for _ in 0..variant_fields.len() {
            virtual_paths.push(HashMap::with_capacity(32));
        }
        let virtual_values = Vec::with_capacity(32);
        let ignored_fields = HashSet::new();
        Ok(VirtualColumnBuilder {
            variant_offsets,
            variant_fields,
            virtual_paths,
            virtual_values,
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
            self.extract_virtual_values(block, 0, sample_rows, &mut hash_to_index);

            self.check_sample_virtual_values(self.total_rows + sample_rows);
            if sample_rows < num_rows {
                self.extract_virtual_values(block, sample_rows, num_rows, &mut hash_to_index);
            }
        } else {
            self.extract_virtual_values(block, 0, num_rows, &mut hash_to_index);
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
    ) {
        for (i, offset) in self.variant_offsets.iter().enumerate() {
            if self.ignored_fields.contains(&i) {
                continue;
            }
            let column = block.get_by_offset(*offset);
            for row in start_row..end_row {
                let val = unsafe { column.index_unchecked(row) };
                let ScalarRef::Variant(jsonb_bytes) = val else {
                    continue;
                };
                let raw_jsonb = RawJsonb::new(jsonb_bytes);

                let key_values = raw_jsonb.extract_scalar_key_values().unwrap();
                for (key_paths, jsonb_value) in key_values {
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
                        let owned_key_paths = OwnedKeyPaths::from_borrowed_key_paths(&key_paths);

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
                }
            }
        }
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
            JsonbValue::TimestampTz(v) => Scalar::Timestamp(v.value),
            JsonbValue::Interval(v) => {
                Scalar::Interval(months_days_micros::new(v.months, v.days, v.micros))
            }
            _ => unreachable!(),
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
            ScalarRef::Interval(v) => JsonbValue::Interval(JsonbInterval {
                months: v.months(),
                days: v.days(),
                micros: v.microseconds(),
            }),
            _ => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    pub fn finalize(
        &mut self,
        write_settings: &WriteSettings,
        location: &Location,
    ) -> Result<VirtualColumnState> {
        let mut virtual_values = Vec::with_capacity(self.virtual_values.len());
        for _ in 0..self.virtual_values.len() {
            virtual_values.push(Vec::new());
        }
        std::mem::swap(&mut self.virtual_values, &mut virtual_values);

        let total_rows = self.total_rows;
        self.total_rows = 0;
        self.ignored_fields.clear();

        // Process the collected virtual values
        let virtual_field_num = self.discard_virtual_values(total_rows, &mut virtual_values);

        // If after discarding, no virtual values remain, return empty state
        if virtual_field_num == 0 {
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

        let mut virtual_column_names = Vec::with_capacity(virtual_field_num);
        let mut virtual_fields = Vec::with_capacity(virtual_field_num);
        let mut virtual_columns = Vec::with_capacity(virtual_field_num);

        // use a tmp column id to generate statistics for virtual columns.
        let mut tmp_column_id = 0;
        for (source_field, field_virtual_paths) in
            self.variant_fields.iter().zip(self.virtual_paths.iter())
        {
            // Collect virtual paths and index as BTreeMap to keep order
            let sorted_virtual_paths: BTreeMap<_, _> = field_virtual_paths.iter().collect();

            for (field_virtual_path, index) in sorted_virtual_paths {
                if virtual_values[*index].is_empty() {
                    continue;
                }
                let val_type = Self::inference_data_type(&virtual_values[*index]);
                let virtual_type = match val_type {
                    VariantDataType::Jsonb => DataType::Nullable(Box::new(DataType::Variant)),
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
                    VariantDataType::Decimal(ty) => {
                        DataType::Nullable(Box::new(DataType::Decimal(ty.size())))
                    }
                    VariantDataType::Binary => DataType::Nullable(Box::new(DataType::Binary)),
                    VariantDataType::Date => DataType::Nullable(Box::new(DataType::Date)),
                    VariantDataType::Timestamp => DataType::Nullable(Box::new(DataType::Timestamp)),
                    VariantDataType::Interval => DataType::Nullable(Box::new(DataType::Interval)),
                    _ => unreachable!(),
                };

                let mut last_row = 0;
                let first_row = virtual_values[*index][0].row;
                let column = if matches!(val_type, VariantDataType::Jsonb) {
                    let mut bitmap = MutableBitmap::from_len_zeroed(total_rows);
                    let mut builder = BinaryColumnBuilder::with_capacity(total_rows, 0);
                    for _ in 0..first_row {
                        builder.commit_row();
                    }
                    for val in &virtual_values[*index] {
                        if val.row - last_row > 1 {
                            for _ in last_row..val.row {
                                builder.commit_row();
                            }
                        }
                        bitmap.set(val.row, true);
                        let jsonb_value = Self::scalar_to_jsonb_value(val.scalar.as_ref());
                        jsonb_value.write_to_vec(&mut builder.data);
                        builder.commit_row();
                        last_row = val.row;
                    }
                    if last_row < total_rows - 1 {
                        for _ in last_row..total_rows {
                            builder.commit_row();
                        }
                    }
                    let nullable_column = NullableColumn {
                        column: Column::Variant(builder.build()),
                        validity: bitmap.into(),
                    };
                    Column::Nullable(Box::new(nullable_column))
                } else {
                    let mut builder = ColumnBuilder::with_capacity(&virtual_type, total_rows);
                    if first_row != 0 {
                        let default_len = first_row;
                        builder.push_repeat(&ScalarRef::Null, default_len);
                        last_row = first_row;
                    }
                    for val in &virtual_values[*index] {
                        if val.row - last_row > 1 {
                            let default_len = val.row - last_row - 1;
                            builder.push_repeat(&ScalarRef::Null, default_len);
                        }
                        builder.push(val.scalar.as_ref());
                        last_row = val.row;
                    }
                    if last_row < total_rows - 1 {
                        let default_len = total_rows - last_row - 1;
                        builder.push_repeat(&ScalarRef::Null, default_len);
                    }
                    builder.build()
                };

                let virtual_table_type = infer_schema_type(&virtual_type).unwrap();
                virtual_columns.push(column.into());

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

                let virtual_name = format!("{}{}", source_field.name, key_name);
                let virtual_field = TableField::new_from_column_id(
                    &virtual_name,
                    virtual_table_type,
                    tmp_column_id,
                );
                virtual_fields.push(virtual_field);
                tmp_column_id += 1;

                let source_column_id = source_field.column_id;
                virtual_column_names.push((source_column_id, key_name, val_type));

                if virtual_fields.len() >= VIRTUAL_COLUMNS_LIMIT {
                    break;
                }
            }
        }

        // Create the virtual block and convert to parquet
        let virtual_block_schema = TableSchemaRefExt::create(virtual_fields);
        let virtual_block = DataBlock::new(virtual_columns, total_rows);

        let columns_statistics =
            gen_columns_statistics(&virtual_block, None, &virtual_block_schema)?;

        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let file_meta = blocks_to_parquet(
            virtual_block_schema.as_ref(),
            vec![virtual_block],
            &mut data,
            write_settings.table_compression,
            None,
        )?;

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
        &mut self,
        num_rows: usize,
        virtual_values: &mut [Vec<JsonbScalarValue>],
    ) -> usize {
        if virtual_values.is_empty() {
            return 0;
        }

        // 1. Discard virtual columns with most values are Null values.
        // let mut keys_to_remove_none = Vec::new();
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
            if null_percentage > 0.7 {
                values.clear();
            }
        }

        // 2. Discard names with the same prefix and ensure that the values of the virtual columns are leaf nodes
        // for example, we have following variant values.
        // {"k1":{"k2":"val"}}
        // {"k1":100}
        // we should not create virtual column for `k1`.
        for virtual_paths in &self.virtual_paths {
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
                Scalar::Decimal(decimal_scalar) => {
                    let size = decimal_scalar.size();
                    VariantDataType::Decimal(DecimalDataType::from(size))
                }
                Scalar::String(_) => VariantDataType::String,
                Scalar::Binary(_) => VariantDataType::Binary,
                Scalar::Date(_) => VariantDataType::Date,
                Scalar::Timestamp(_) => VariantDataType::Timestamp,
                Scalar::Interval(_) => VariantDataType::Interval,
                _ => VariantDataType::Jsonb,
            };
            if !val_type_set.contains(&ty) {
                val_type_set.insert(ty);
            }
            if val_type_set.len() == 2 {
                return VariantDataType::Jsonb;
            }
        }
        if val_type_set.len() == 1 {
            val_type_set.into_iter().next().unwrap()
        } else {
            VariantDataType::Jsonb
        }
    }

    fn file_meta_to_virtual_column_metas(
        &self,
        file_meta: FileMetaData,
        virtual_column_names: Vec<(ColumnId, String, VariantDataType)>,
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
        for ((i, (source_column_id, name, variant_type)), col_chunk) in virtual_column_names
            .into_iter()
            .enumerate()
            .zip(row_group.columns.iter())
        {
            let tmp_column_id = i as u32;
            match &col_chunk.meta_data {
                Some(chunk_meta) => {
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

                    let (variant_type_code, scale) =
                        VirtualColumnMeta::data_type_code(&variant_type);
                    let column_stat = columns_statistics.remove(&tmp_column_id);
                    let virtual_column_meta = VirtualColumnMeta {
                        offset: col_start as u64,
                        len: col_len as u64,
                        num_values,
                        data_type: variant_type_code,
                        scale,
                        column_stat,
                    };

                    let draft_virtual_column_meta = DraftVirtualColumnMeta {
                        source_column_id,
                        name,
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
    fn to_borrowed_key_path(&self) -> JsonbKeyPath<'_> {
        match self {
            OwnedKeyPath::Index(idx) => JsonbKeyPath::Index(*idx),
            OwnedKeyPath::Name(name) => JsonbKeyPath::Name(Cow::Borrowed(name.as_str())),
        }
    }

    fn from_borrowed_key_path<'a>(key_path: &JsonbKeyPath<'a>) -> OwnedKeyPath {
        match key_path {
            JsonbKeyPath::Index(idx) => OwnedKeyPath::Index(*idx),
            JsonbKeyPath::QuotedName(name) => OwnedKeyPath::Name(name.to_string()),
            JsonbKeyPath::Name(name) => OwnedKeyPath::Name(name.to_string()),
        }
    }
}

impl OwnedKeyPaths {
    fn to_borrowed_key_paths(&self) -> JsonbKeyPaths<'_> {
        let paths = self
            .paths
            .iter()
            .map(|path| path.to_borrowed_key_path())
            .collect::<Vec<_>>();
        JsonbKeyPaths { paths }
    }

    fn from_borrowed_key_paths(key_paths: &JsonbKeyPaths) -> OwnedKeyPaths {
        let paths = key_paths
            .paths
            .iter()
            .map(|path| OwnedKeyPath::from_borrowed_key_path(path))
            .collect::<Vec<_>>();
        OwnedKeyPaths { paths }
    }

    fn is_prefix_path(&self, other: &OwnedKeyPaths) -> bool {
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
}

#[derive(Debug, Clone)]
struct JsonbScalarValue {
    row: usize,
    scalar: Scalar,
}
