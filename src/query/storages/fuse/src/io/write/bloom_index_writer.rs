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
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::TableCompression;
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
        ngram_args: &[NgramArgs],
    ) -> Result<Option<Self>> {
        // write index
        let mut builder =
            BloomIndexBuilder::create(ctx.get_function_context()?, bloom_columns_map, ngram_args)?;
        builder.add_block(block)?;
        let maybe_bloom_index = builder.finalize()?;
        if let Some(bloom_index) = maybe_bloom_index {
            Ok(Some(Self::from_bloom_index(&bloom_index, location)?))
        } else {
            Ok(None)
        }
    }
}

pub struct BloomIndexRebuilder {
    pub table_ctx: Arc<dyn TableContext>,
    pub table_schema: TableSchemaRef,
    pub table_dal: Operator,
    pub storage_format: FuseStorageFormat,
    pub bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    pub ngram_args: Vec<NgramArgs>,
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
            &self.ngram_args,
        )?;
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
