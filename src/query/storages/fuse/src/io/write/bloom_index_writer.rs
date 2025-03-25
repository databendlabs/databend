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
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MapType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::ValueType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionContext;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_index::filters::FilterBuilder;
use databend_storages_common_index::filters::Xor8Builder;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::Index;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::TableCompression;
use jsonb::RawJsonb;
use opendal::Operator;

use crate::io::BlockReader;
use crate::FuseStorageFormat;

pub struct BloomIndexState {
    pub(crate) data: Vec<u8>,
    pub(crate) size: u64,
    pub(crate) location: Location,
    pub(crate) column_distinct_count: HashMap<ColumnId, usize>,
}

impl BloomIndexState {
    pub fn from_bloom_index(bloom_index: &BloomIndex, location: Location) -> Result<Self> {
        let index_block = bloom_index.serialize_to_data_block()?;
        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let _ = blocks_to_parquet(
            &bloom_index.filter_schema,
            vec![index_block],
            &mut data,
            TableCompression::None,
        )?;
        let data_size = data.len() as u64;
        Ok(Self {
            data,
            size: data_size,
            location,
            column_distinct_count: bloom_index.column_distinct_count.clone(),
        })
    }

    pub fn from_data_block(
        ctx: Arc<dyn TableContext>,
        block: &DataBlock,
        location: Location,
        bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    ) -> Result<Option<Self>> {
        // write index
        let mut builder = BloomIndexBuilder::create(ctx.get_function_context()?, bloom_columns_map);
        builder.add_block(block)?;
        let maybe_bloom_index = builder.finalize()?;
        if let Some(bloom_index) = maybe_bloom_index {
            Ok(Some(Self::from_bloom_index(&bloom_index, location)?))
        } else {
            Ok(None)
        }
    }
}

pub struct BloomIndexBuilder {
    func_ctx: FunctionContext,
    columns: Vec<ColumnXor8Builder>,
}

struct ColumnXor8Builder {
    index: FieldIndex,
    field: TableField,
    builder: Xor8Builder,
}

impl BloomIndexBuilder {
    pub fn create(
        func_ctx: FunctionContext,
        bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    ) -> Self {
        let columns = bloom_columns_map
            .iter()
            .map(|(&index, field)| ColumnXor8Builder {
                index,
                field: field.clone(),
                builder: Xor8Builder::create(),
            })
            .collect();
        Self { func_ctx, columns }
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        if block.is_empty() {
            return Err(ErrorCode::BadArguments("block is empty"));
        }
        if block.num_columns() == 0 {
            return Ok(());
        }

        let mut keys_to_remove = Vec::with_capacity(self.columns.len());
        'FOR: for (index, bloom_index_column) in self.columns.iter_mut().enumerate() {
            let field_type = &block.get_by_offset(bloom_index_column.index).data_type;
            if !Xor8Filter::supported_type(field_type) {
                keys_to_remove.push(index);
                continue;
            }

            let column = match &block.get_by_offset(bloom_index_column.index).value {
                Value::Scalar(s) => {
                    let builder = ColumnBuilder::repeat(&s.as_ref(), 1, field_type);
                    builder.build()
                }
                Value::Column(c) => c.clone(),
            };

            let (column, data_type) = match field_type.remove_nullable() {
                DataType::Map(box inner_ty) => {
                    // Add bloom filter for the value of map type
                    let map_column = if field_type.is_nullable() {
                        let nullable_column =
                            NullableType::<MapType<AnyType, AnyType>>::try_downcast_column(&column)
                                .unwrap();
                        nullable_column.column
                    } else {
                        MapType::<AnyType, AnyType>::try_downcast_column(&column).unwrap()
                    };
                    let column = map_column.underlying_column().values;

                    let DataType::Tuple(kv_tys) = inner_ty else {
                        unreachable!();
                    };
                    let val_type = kv_tys[1].clone();
                    // Extract JSON value of string type to create bloom index,
                    // other types of JSON value will be ignored.
                    if val_type.remove_nullable() == DataType::Variant {
                        let mut builder = ColumnBuilder::with_capacity(
                            &DataType::Nullable(Box::new(DataType::String)),
                            column.len(),
                        );
                        for val in column.iter() {
                            if let ScalarRef::Variant(v) = val {
                                let raw_jsonb = RawJsonb::new(v);
                                if let Ok(Some(str_val)) = raw_jsonb.as_str() {
                                    builder.push(ScalarRef::String(&str_val));
                                } else {
                                    keys_to_remove.push(index);
                                    continue 'FOR;
                                }
                            } else {
                                builder.push_default();
                            }
                        }
                        let str_column = builder.build();
                        if BloomIndex::check_large_string(&str_column) {
                            keys_to_remove.push(index);
                            continue;
                        }
                        let str_type = DataType::Nullable(Box::new(DataType::String));
                        (str_column, str_type)
                    } else {
                        if BloomIndex::check_large_string(&column) {
                            keys_to_remove.push(index);
                            continue;
                        }
                        (column, val_type)
                    }
                }
                _ => {
                    if BloomIndex::check_large_string(&column) {
                        keys_to_remove.push(index);
                        continue;
                    }
                    (column, field_type.clone())
                }
            };

            let (column, validity) =
                BloomIndex::calculate_nullable_column_digest(&self.func_ctx, &column, &data_type)?;

            // create filter per column
            if validity.as_ref().map(|v| v.null_count()).unwrap_or(0) > 0 {
                let validity = validity.unwrap();
                let it = column.deref().iter().zip(validity.iter()).map(
                    |(v, b)| {
                        if !b {
                            &0
                        } else {
                            v
                        }
                    },
                );
                bloom_index_column.builder.add_digests(it);
            } else {
                bloom_index_column.builder.add_digests(column.deref());
            }
        }
        for k in keys_to_remove {
            self.columns.remove(k);
        }
        Ok(())
    }

    pub fn finalize(mut self) -> Result<Option<BloomIndex>> {
        let mut column_distinct_count = HashMap::with_capacity(self.columns.len());
        let mut filters = Vec::with_capacity(self.columns.len());
        let mut filter_fields = Vec::with_capacity(self.columns.len());
        for column in self.columns.iter_mut() {
            let filter = column.builder.build()?;
            if let Some(len) = filter.len() {
                if !matches!(
                    column.field.data_type().remove_nullable(),
                    TableDataType::Map(_) | TableDataType::Variant
                ) {
                    column_distinct_count.insert(column.field.column_id, len);
                }
            }
            let filter_name =
                BloomIndex::build_filter_column_name(BlockFilter::VERSION, &column.field)?;
            filter_fields.push(TableField::new(&filter_name, TableDataType::Binary));
            filters.push(Arc::new(filter));
        }

        if filter_fields.is_empty() {
            return Ok(None);
        }
        let filter_schema = Arc::new(TableSchema::new(filter_fields));
        Ok(Some(BloomIndex {
            func_ctx: self.func_ctx,
            version: BlockFilter::VERSION,
            filter_schema,
            filters,
            column_distinct_count,
        }))
    }
}

pub struct BloomIndexRebuilder {
    pub table_ctx: Arc<dyn TableContext>,
    pub table_schema: TableSchemaRef,
    pub table_dal: Operator,
    pub storage_format: FuseStorageFormat,
    pub bloom_columns_map: BTreeMap<FieldIndex, TableField>,
}

impl BloomIndexRebuilder {
    pub async fn bloom_index_state_from_block_meta(
        &self,
        bloom_index_location: &Location,
        block_read_info: &BlockReadInfo,
    ) -> Result<Option<(BloomIndexState, BloomIndex)>> {
        let ctx = self.table_ctx.clone();

        let projection =
            Projection::Columns((0..self.table_schema.fields().len()).collect::<Vec<usize>>());

        let block_reader = BlockReader::create(
            ctx,
            self.table_dal.clone(),
            self.table_schema.clone(),
            projection,
            false,
            false,
            false,
        )?;

        let settings = ReadSettings::from_ctx(&self.table_ctx)?;

        let merge_io_read_result = block_reader
            .read_columns_data_by_merge_io(
                &settings,
                &block_read_info.location,
                &block_read_info.col_metas,
                &None,
            )
            .await?;
        let data_block = block_reader.deserialize_chunks_with_meta(
            block_read_info,
            &self.storage_format,
            merge_io_read_result,
        )?;

        assert_eq!(bloom_index_location.1, BlockFilter::VERSION);
        let mut builder = BloomIndexBuilder::create(
            self.table_ctx.get_function_context()?,
            self.bloom_columns_map.clone(),
        );
        builder.add_block(&data_block)?;
        let maybe_bloom_index = builder.finalize()?;

        match maybe_bloom_index {
            None => Ok(None),
            Some(bloom_index) => Ok(Some((
                BloomIndexState::from_bloom_index(&bloom_index, bloom_index_location.clone())?,
                bloom_index,
            ))),
        }
    }
}
