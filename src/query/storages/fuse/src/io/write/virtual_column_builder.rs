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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_common_expression::VariantDataType;
use databend_common_expression::VIRTUAL_COLUMNS_LIMIT;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_cache::Table;
use databend_storages_common_table_meta::meta::DraftVirtualBlockMeta;
use databend_storages_common_table_meta::meta::DraftVirtualColumnMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use jsonb::from_slice;
use jsonb::Number as JsonbNumber;
use jsonb::Value as JsonbValue;
use parquet::format::FileMetaData;

use crate::io::write::WriteSettings;
use crate::io::TableMetaLocationGenerator;
use crate::statistics::gen_columns_statistics;
use crate::FuseTable;

#[derive(Debug, Clone)]
pub struct VirtualColumnState {
    pub data: Vec<u8>,
    pub draft_virtual_block_meta: DraftVirtualBlockMeta,
}

#[derive(Clone)]
pub struct VirtualColumnBuilder {
    // variant field offset and ColumnId
    variant_fields: Vec<(usize, TableField)>,
}

impl VirtualColumnBuilder {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        table: &FuseTable,
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
        if !table.support_virtual_columns() {
            return Err(ErrorCode::VirtualColumnError(format!(
                "storage format {:?} don't support virtual column",
                table.get_storage_format()
            )));
        }

        // ignore persistent system tables {
        if let Ok(database_name) = table.table_info.database_name() {
            if database_name == "persistent_system" {
                return Err(ErrorCode::VirtualColumnError(format!(
                    "system database {} don't support virtual column",
                    database_name
                )));
            }
        }

        let mut variant_fields = Vec::new();
        for (i, field) in schema.fields.iter().enumerate() {
            if field.data_type().remove_nullable() == TableDataType::Variant {
                variant_fields.push((i, field.clone()));
            }
        }
        if variant_fields.is_empty() {
            return Err(ErrorCode::VirtualColumnError("Virtual column only support variant type, but this table don't have variant type fields"));
        }
        Ok(VirtualColumnBuilder { variant_fields })
    }

    pub fn add_block(
        &self,
        block: &DataBlock,
        write_settings: &WriteSettings,
        location: &Location,
    ) -> Result<VirtualColumnState> {
        let num_rows = block.num_rows();
        let mut virtual_column_names = Vec::new();
        let mut virtual_fields = Vec::new();
        let mut virtual_columns = Vec::new();
        // use a tmp column id to generate statistics for virtual columns.
        let mut tmp_column_id = 0;
        let mut paths = VecDeque::new();
        // use first 10 rows as sample to check whether the block is suitable for generating virtual columns
        let sample_rows = num_rows.min(10);
        for (offset, source_field) in &self.variant_fields {
            let source_column_id = source_field.column_id;
            let column = block.get_by_offset(*offset);

            let mut virtual_values = BTreeMap::new();
            for row in 0..sample_rows {
                let val = unsafe { column.index_unchecked(row) };
                if let ScalarRef::Variant(jsonb_bytes) = val {
                    let val = from_slice(jsonb_bytes).unwrap();
                    paths.clear();
                    Self::collect_virtual_values(
                        &val,
                        row,
                        virtual_fields.len(),
                        &mut paths,
                        &mut virtual_values,
                    );
                }
            }
            if Self::check_sample_virtual_values(sample_rows, &mut virtual_values) {
                continue;
            }
            for row in sample_rows..num_rows {
                let val = unsafe { column.index_unchecked(row) };
                if let ScalarRef::Variant(jsonb_bytes) = val {
                    let val = from_slice(jsonb_bytes).unwrap();
                    paths.clear();
                    Self::collect_virtual_values(
                        &val,
                        row,
                        virtual_fields.len(),
                        &mut paths,
                        &mut virtual_values,
                    );
                }
            }
            Self::discard_virtual_values(num_rows, virtual_fields.len(), &mut virtual_values);
            if virtual_values.is_empty() {
                continue;
            }

            let value_types = Self::inference_data_type(&virtual_values);
            for ((key_paths, vals), val_type) in
                virtual_values.into_iter().zip(value_types.into_iter())
            {
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
                    _ => todo!(),
                };

                // create column
                let column = match val_type {
                    VariantDataType::Jsonb => VariantType::from_opt_data(
                        vals.into_iter().map(|v| v.map(|v| v.to_vec())).collect(),
                    ),
                    VariantDataType::Boolean => BooleanType::from_opt_data(
                        vals.into_iter()
                            .map(|v| v.map(|v| v.as_bool().unwrap()))
                            .collect(),
                    ),
                    VariantDataType::UInt64 => UInt64Type::from_opt_data(
                        vals.into_iter()
                            .map(|v| v.map(|v| v.as_u64().unwrap()))
                            .collect(),
                    ),
                    VariantDataType::Int64 => Int64Type::from_opt_data(
                        vals.into_iter()
                            .map(|v| v.map(|v| v.as_i64().unwrap()))
                            .collect(),
                    ),
                    VariantDataType::Float64 => Float64Type::from_opt_data(
                        vals.into_iter()
                            .map(|v| v.map(|v| v.as_f64().unwrap()))
                            .collect(),
                    ),
                    VariantDataType::String => StringType::from_opt_data(
                        vals.into_iter()
                            .map(|v| v.map(|v| v.as_str().unwrap().to_string()))
                            .collect(),
                    ),
                    _ => todo!(),
                };
                let virtual_table_type = infer_schema_type(&virtual_type).unwrap();
                virtual_columns.push(BlockEntry::new(virtual_type, Value::Column(column)));

                let mut key_name = String::new();
                for path in key_paths {
                    key_name.push('[');
                    match path {
                        KeyPath::Index(idx) => {
                            key_name.push_str(&format!("{idx}"));
                        }
                        KeyPath::Name(name) => {
                            key_name.push('\'');
                            key_name.push_str(&name);
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

                virtual_column_names.push((source_column_id, key_name, val_type));
            }
            if virtual_fields.len() >= VIRTUAL_COLUMNS_LIMIT {
                break;
            }
        }

        // There are no suitable virtual columns, returning empty data.
        if virtual_fields.is_empty() {
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

        let virtual_block_schema = TableSchemaRefExt::create(virtual_fields);
        let virtual_block = DataBlock::new(virtual_columns, block.num_rows());

        let columns_statistics =
            gen_columns_statistics(&virtual_block, None, &virtual_block_schema)?;

        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let file_meta = blocks_to_parquet(
            virtual_block_schema.as_ref(),
            vec![virtual_block],
            &mut data,
            write_settings.table_compression,
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

    fn collect_virtual_values<'a>(
        val: &JsonbValue<'a>,
        row: usize,
        virtual_field_num: usize,
        paths: &mut VecDeque<KeyPath>,
        virtual_values: &mut BTreeMap<Vec<KeyPath>, Vec<Option<JsonbValue<'a>>>>,
    ) {
        if virtual_values.len() + virtual_field_num > VIRTUAL_COLUMNS_LIMIT {
            return;
        }
        match val {
            JsonbValue::Object(obj) => {
                for (key, val) in obj {
                    paths.push_back(KeyPath::Name(key.clone()));
                    Self::collect_virtual_values(
                        val,
                        row,
                        virtual_field_num,
                        paths,
                        virtual_values,
                    );
                    paths.pop_back();
                }
                return;
            }
            JsonbValue::Array(arr) => {
                for (i, val) in arr.iter().enumerate() {
                    paths.push_back(KeyPath::Index(i as u32));
                    Self::collect_virtual_values(
                        val,
                        row,
                        virtual_field_num,
                        paths,
                        virtual_values,
                    );
                    paths.pop_back();
                }
                return;
            }
            _ => {}
        }

        // ignore root scalar values
        if paths.is_empty() {
            return;
        }

        // only collect leaf node scalar values.
        let path_names = paths.iter().cloned().collect();

        if let Some(vals) = virtual_values.get_mut(&path_names) {
            while vals.len() < row {
                vals.push(None);
            }
            vals.push(Some(val.clone()))
        } else {
            let mut vals = Vec::with_capacity(row + 1);
            for _ in 0..row {
                vals.push(None);
            }
            vals.push(Some(val.clone()));
            virtual_values.insert(path_names, vals);
        }
    }

    fn check_sample_virtual_values(
        sample_rows: usize,
        virtual_values: &mut BTreeMap<Vec<KeyPath>, Vec<Option<JsonbValue<'_>>>>,
    ) -> bool {
        // All values are NULL or scalar Variant value.
        if virtual_values.is_empty() {
            return true;
        }
        // Fill in the NULL values, keeping each column the same length.
        for (_, vals) in virtual_values.iter_mut() {
            while vals.len() < sample_rows {
                vals.push(None);
            }
        }

        let mut most_null_count = 0;
        for (_, value) in virtual_values.iter() {
            let null_count = value.iter().filter(|x| x.is_none()).count();
            let null_percentage = null_count as f64 / value.len() as f64;
            if null_percentage > 0.7 {
                most_null_count += 1;
            }
        }
        let most_null_percentage = most_null_count as f64 / virtual_values.len() as f64;
        most_null_percentage > 0.5
    }

    fn discard_virtual_values(
        num_rows: usize,
        virtual_field_num: usize,
        virtual_values: &mut BTreeMap<Vec<KeyPath>, Vec<Option<JsonbValue<'_>>>>,
    ) {
        if virtual_values.is_empty() {
            return;
        }
        // Fill in the NULL values, keeping each column the same length.
        for (_, vals) in virtual_values.iter_mut() {
            while vals.len() < num_rows {
                vals.push(None);
            }
        }

        // 1. Discard virtual columns with most values are Null values.
        let mut keys_to_remove_none = Vec::new();
        for (key, value) in virtual_values.iter() {
            let null_count = value.iter().filter(|x| x.is_none()).count();
            let null_percentage = null_count as f64 / value.len() as f64;
            if null_percentage > 0.7 {
                keys_to_remove_none.push(key.clone());
            }
        }
        for key in keys_to_remove_none {
            virtual_values.remove(&key);
        }

        // 2. Discard names with the same prefix and ensure that the values of the virtual columns are leaf nodes
        // for example, we have following variant values.
        // {"k1":{"k2":"val"}}
        // {"k1":100}
        // we should not create virtual column for `k1`.
        let mut keys_to_remove_prefix = Vec::new();
        let mut keys: Vec<Vec<KeyPath>> = virtual_values.keys().cloned().collect();
        keys.sort_by_key(|k| k.len());

        for i in 0..keys.len() {
            let key1 = &keys[i];
            for key2 in keys.iter().skip(i + 1) {
                if key2.starts_with(key1) {
                    keys_to_remove_prefix.push(key1.clone());
                    break;
                }
            }
        }

        keys_to_remove_prefix.sort();
        keys_to_remove_prefix.dedup();

        for key in keys_to_remove_prefix {
            virtual_values.remove(&key);
        }

        // 3. Discard redundant virtual values to avoid generating too much virtual fields.
        if virtual_field_num + virtual_values.len() > VIRTUAL_COLUMNS_LIMIT {
            let redundant_num = virtual_field_num + virtual_values.len() - VIRTUAL_COLUMNS_LIMIT;
            for _ in 0..redundant_num {
                let _ = virtual_values.pop_last();
            }
        }
    }

    fn inference_data_type(
        virtual_values: &BTreeMap<Vec<KeyPath>, Vec<Option<JsonbValue>>>,
    ) -> Vec<VariantDataType> {
        let mut val_types = Vec::with_capacity(virtual_values.len());
        let mut val_type_set = BTreeSet::new();
        for (_, vals) in virtual_values.iter() {
            val_type_set.clear();
            let mut max_u64 = u64::MIN;
            let mut min_i64 = i64::MAX;
            for val in vals.iter().flatten() {
                let ty = match val {
                    JsonbValue::Bool(_) => VariantDataType::Boolean,
                    JsonbValue::Number(JsonbNumber::UInt64(n)) => {
                        if *n >= max_u64 {
                            max_u64 = *n;
                        }
                        VariantDataType::UInt64
                    }
                    JsonbValue::Number(JsonbNumber::Int64(n)) => {
                        if *n <= min_i64 {
                            min_i64 = *n;
                        }
                        VariantDataType::Int64
                    }
                    JsonbValue::Number(JsonbNumber::Float64(_)) => VariantDataType::Float64,
                    JsonbValue::String(_) => VariantDataType::String,
                    _ => VariantDataType::Jsonb,
                };
                if !val_type_set.contains(&ty) {
                    val_type_set.insert(ty);
                }
            }
            // Try to combine Uint64 and Int64 into one type
            if val_type_set.len() == 2
                && val_type_set.contains(&VariantDataType::UInt64)
                && val_type_set.contains(&VariantDataType::Int64)
            {
                if min_i64 >= 0 {
                    val_type_set.remove(&VariantDataType::Int64);
                } else if max_u64 <= i64::MAX as u64 {
                    val_type_set.remove(&VariantDataType::UInt64);
                }
            }
            let common_type = if val_type_set.len() != 1 {
                VariantDataType::Jsonb
            } else {
                val_type_set.pop_first().unwrap()
            };
            val_types.push(common_type);
        }
        val_types
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

/// Represents a valid key path.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
enum KeyPath {
    /// represents the index of an Array
    Index(u32),
    /// represents the field name of an Object.
    Name(String),
}
