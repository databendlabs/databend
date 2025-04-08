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
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_common_expression::VariantDataType;
use databend_common_expression::VIRTUAL_COLUMNS_LIMIT;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::TableMeta;
use databend_storages_common_blocks::blocks_to_parquet;
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
        table_meta: &TableMeta,
    ) -> Option<VirtualColumnBuilder> {
        if LicenseManagerSwitch::instance()
            .check_enterprise_enabled(ctx.get_license_key(), Feature::VirtualColumn)
            .is_err()
        {
            return None;
        }

        let mut variant_fields = Vec::new();
        for (i, field) in table_meta.schema.fields.iter().enumerate() {
            if field.data_type().remove_nullable() == TableDataType::Variant {
                variant_fields.push((i, field.clone()));
            }
        }

        if !variant_fields.is_empty() {
            Some(VirtualColumnBuilder { variant_fields })
        } else {
            None
        }
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
        for (offset, source_field) in &self.variant_fields {
            let source_column_id = source_field.column_id;
            let column = block.get_by_offset(*offset);

            let mut virtual_values = BTreeMap::new();
            for row in 0..num_rows {
                let val = unsafe { column.value.index_unchecked(row) };
                if let ScalarRef::Variant(jsonb_bytes) = val {
                    let val = from_slice(jsonb_bytes).unwrap();
                    paths.clear();
                    Self::collect_virtual_values(&val, row, &mut paths, &mut virtual_values);
                }
            }
            if virtual_values.is_empty() {
                continue;
            }

            // Discard redundant virtual values to avoid generating too much virtual fields.
            if virtual_fields.len() + virtual_values.len() > VIRTUAL_COLUMNS_LIMIT {
                let redundant_num =
                    virtual_fields.len() + virtual_values.len() - VIRTUAL_COLUMNS_LIMIT;
                for _ in 0..redundant_num {
                    let _ = virtual_values.pop_last();
                }
            }

            // TODO: Ignore columns that are mostly NULL value and JSON scalar value.

            // Fill in the NULL values, keeping each column the same length.
            for (_, vals) in virtual_values.iter_mut() {
                while vals.len() < num_rows {
                    vals.push(None);
                }
            }

            let value_types = Self::inference_data_type(&virtual_values);
            for ((key, vals), val_type) in virtual_values.into_iter().zip(value_types.into_iter()) {
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

                let virtual_name = format!("{}{}", source_field.name, key);
                let virtual_field = TableField::new_from_column_id(
                    &virtual_name,
                    virtual_table_type,
                    tmp_column_id,
                );
                virtual_fields.push(virtual_field);
                tmp_column_id += 1;

                virtual_column_names.push((source_column_id, key, val_type));
            }
            if virtual_fields.len() >= VIRTUAL_COLUMNS_LIMIT {
                break;
            }
        }

        // There are no suitable virtual columns, returning empty data.
        if virtual_fields.is_empty() {
            let draft_virtual_block_meta = DraftVirtualBlockMeta {
                virtual_col_metas: vec![],
                virtual_col_size: 0,
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
            virtual_col_metas: draft_virtual_column_metas,
            virtual_col_size: data_size,
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
        paths: &mut VecDeque<String>,
        virtual_values: &mut BTreeMap<String, Vec<Option<JsonbValue<'a>>>>,
    ) {
        if let JsonbValue::Object(obj) = val {
            for (key, val) in obj {
                paths.push_back(key.clone());
                Self::collect_virtual_values(val, row, paths, virtual_values);
                paths.pop_back();
            }
            return;
        }

        // ignore root scalar values
        if paths.is_empty() {
            return;
        }

        // only collect leaf node scalar values.
        let mut name = String::new();
        for path in paths {
            name.push('[');
            name.push('\'');
            name.push_str(path);
            name.push('\'');
            name.push(']');
        }

        if let Some(vals) = virtual_values.get_mut(&name) {
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
            virtual_values.insert(name, vals);
        }
    }

    fn inference_data_type(
        virtual_values: &BTreeMap<String, Vec<Option<JsonbValue>>>,
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
        for ((i, (source_column_id, name, virtual_type)), col_chunk) in virtual_column_names
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

                    let virtual_type_num = match virtual_type {
                        VariantDataType::Jsonb => 1,
                        VariantDataType::Boolean => 2,
                        VariantDataType::UInt64 => 3,
                        VariantDataType::Int64 => 4,
                        VariantDataType::Float64 => 5,
                        VariantDataType::String => 6,
                        _ => todo!(),
                    };

                    let column_stat = columns_statistics.remove(&tmp_column_id);
                    let virtual_column_meta = VirtualColumnMeta {
                        offset: col_start as u64,
                        len: col_len as u64,
                        num_values,
                        data_type: virtual_type_num,
                        column_stat,
                    };

                    let draft_virtual_column_meta = DraftVirtualColumnMeta {
                        source_column_id,
                        name,
                        data_type: virtual_type,
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
