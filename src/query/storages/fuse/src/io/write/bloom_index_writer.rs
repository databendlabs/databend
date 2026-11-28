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
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::BloomIndexType;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Buffer;
use opendal::Operator;

use crate::FuseStorageFormat;
use crate::io::BlockReader;
use crate::io::write::block_index::BlockIndexSpec;
use crate::io::write::block_index::BlockIndexWriteContext;
use crate::io::write::block_index::BlockIndexWriter;
use crate::io::write::block_index::PendingBlockIndexOutput;
use crate::io::write::block_index::PendingBloomIndex;
use crate::io::write::block_index::PendingIndexFile;

#[derive(Debug)]
pub struct BloomIndexState {
    pub(crate) data: Buffer,
    pub(crate) size: u64,
    pub(crate) ngram_size: Option<u64>,
    pub(crate) location: Location,
    pub(crate) column_distinct_count: HashMap<ColumnId, usize>,
}

fn bloom_index_block(bloom_index: &BloomIndex) -> Result<(DataBlock, Option<u64>)> {
    let index_block = bloom_index.serialize_to_data_block()?;
    let ngram_indexes = bloom_index
        .filter_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name.starts_with("Ngram"))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let ngram_size = if ngram_indexes.is_empty() {
        None
    } else {
        Some(
            ngram_indexes
                .into_iter()
                .map(|index| index_block.get_by_offset(index).value().memory_size(false) as u64)
                .sum(),
        )
    };
    Ok((index_block, ngram_size))
}

impl BloomIndexState {
    pub fn from_bloom_index(bloom_index: &BloomIndex, location: Location) -> Result<Self> {
        let (index_block, ngram_size) = bloom_index_block(bloom_index)?;
        let serialized = blocks_to_parquet(
            &bloom_index.filter_schema,
            vec![index_block],
            TableCompression::None,
            false,
            None,
        )?;
        let data_size = serialized.len() as u64;
        let data = Buffer::from(serialized.payload);
        Ok(Self {
            data,
            size: data_size,
            ngram_size,
            location,
            column_distinct_count: bloom_index.column_distinct_count.clone(),
        })
    }

    pub fn from_data_block(
        ctx: Arc<dyn TableContext>,
        block: &DataBlock,
        location: Location,
        bloom_index_type: BloomIndexType,
        bloom_columns_map: BTreeMap<FieldIndex, TableField>,
        ngram_args: &[NgramArgs],
    ) -> Result<Option<Self>> {
        // write index
        let mut builder = BloomIndexBuilder::create(
            ctx.get_function_context()?,
            bloom_index_type,
            bloom_columns_map,
            ngram_args,
        )?;
        builder.add_block(block)?;
        let maybe_bloom_index = builder.finalize()?;
        if let Some(bloom_index) = maybe_bloom_index {
            Ok(Some(Self::from_bloom_index(&bloom_index, location)?))
        } else {
            Ok(None)
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn data(self) -> Buffer {
        self.data
    }

    pub fn ngram_size(&self) -> Option<u64> {
        self.ngram_size
    }
}

pub struct BloomIndexWriteSpec {
    columns: BTreeMap<FieldIndex, TableField>,
    ngram_args: Vec<NgramArgs>,
}

impl BloomIndexWriteSpec {
    pub fn new(columns: BTreeMap<FieldIndex, TableField>, ngram_args: Vec<NgramArgs>) -> Self {
        Self {
            columns,
            ngram_args,
        }
    }

    fn create_builder(
        &self,
        func_ctx: databend_common_expression::FunctionContext,
        bloom_index_type: BloomIndexType,
    ) -> Result<BloomIndexBuilder> {
        BloomIndexBuilder::create(
            func_ctx,
            bloom_index_type,
            self.columns.clone(),
            &self.ngram_args,
        )
    }
}

impl BlockIndexSpec for BloomIndexWriteSpec {
    fn new_writer(&self, context: BlockIndexWriteContext) -> Result<Box<dyn BlockIndexWriter>> {
        Ok(Box::new(BloomIndexWriter {
            builder: self
                .create_builder(context.func_ctx, context.write_settings.bloom_index_type)?,
            location: context.bloom_location,
        }))
    }
}

struct BloomIndexWriter {
    builder: BloomIndexBuilder,
    location: Location,
}

impl BlockIndexWriter for BloomIndexWriter {
    fn write(&mut self, block: &DataBlock) -> Result<()> {
        self.builder.add_block(block)
    }

    fn finish(self: Box<Self>) -> Result<PendingBlockIndexOutput> {
        let Some(index) = self.builder.finalize()? else {
            return Ok(PendingBlockIndexOutput::default());
        };
        let state = BloomIndexState::from_bloom_index(&index, self.location)?;
        Ok(PendingBlockIndexOutput {
            bloom: Some(PendingBloomIndex {
                file: PendingIndexFile {
                    location: state.location,
                    data: state.data,
                },
                ngram_size: state.ngram_size,
                column_distinct_count: state.column_distinct_count,
            }),
            ..Default::default()
        })
    }
}

#[derive(Clone)]
pub struct BloomIndexRebuilder {
    pub table_ctx: Arc<dyn TableContext>,
    pub table_schema: TableSchemaRef,
    pub table_dal: Operator,
    pub storage_format: FuseStorageFormat,
    pub bloom_index_type: BloomIndexType,
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
            CachePolicy::READ_ONLY,
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
            self.bloom_index_type,
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
