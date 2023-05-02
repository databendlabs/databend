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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::plan::block_id_in_segment;
use common_catalog::plan::compute_row_id_prefix;
use common_catalog::plan::split_row_id;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_storage::ColumnNodes;
use storages_common_table_meta::meta::SegmentInfo;

use super::fuse_rows_fetcher::RowsFetcher;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::UncompressedBuffer;
use crate::FusePartInfo;
use crate::FuseTable;
use crate::MergeIOReadResult;

pub(super) struct ParquetRowsFetcher<const BLOCKING_IO: bool> {
    settings: ReadSettings,
    reader: Arc<BlockReader>,
    uncompressed_buffer: Arc<UncompressedBuffer>,
    part_map: HashMap<u64, PartInfoPtr>,
}

#[async_trait::async_trait]
impl<const BLOCKING_IO: bool> RowsFetcher for ParquetRowsFetcher<BLOCKING_IO> {
    #[async_backtrace::framed]
    async fn fetch(&self, row_ids: &[u64]) -> Result<DataBlock> {
        let num_rows = row_ids.len();
        let mut part_set = HashSet::new();
        let mut row_set = Vec::with_capacity(num_rows);
        for row_id in row_ids {
            let (prefix, idx) = split_row_id(*row_id);
            part_set.insert(prefix);
            row_set.push((prefix, idx));
        }

        let (blocks, idx_map) = self.fetch_blocks(part_set).await?;
        let indices = row_set
            .iter()
            .map(|(prefix, row_idx)| {
                let block_idx = idx_map[prefix];
                (block_idx, *row_idx as usize, 1_usize)
            })
            .collect::<Vec<_>>();

        let blocks = blocks.iter().collect::<Vec<_>>();
        Ok(DataBlock::take_blocks(&blocks, &indices, num_rows))
    }

    fn set_metas(
        &mut self,
        segments: Vec<(u64, Arc<SegmentInfo>)>,
        table_schema: TableSchemaRef,
        projection: &Projection,
    ) {
        let arrow_schema = table_schema.to_arrow();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&table_schema));

        let size_hint = if let Some((_, segment)) = segments.first() {
            segment.blocks.len()
        } else {
            1
        };
        let mut prefix_map = HashMap::with_capacity(segments.len() * size_hint);

        for (seg_id, segment) in segments {
            let block_num = segment.blocks.len();
            for (block_idx, block_meta) in segment.blocks.iter().enumerate() {
                let part_info =
                    FuseTable::projection_part(block_meta, &None, &column_nodes, None, projection);
                let block_id = block_id_in_segment(block_num, block_idx) as u64;
                let prefix = compute_row_id_prefix(seg_id, block_id);
                prefix_map.insert(prefix, part_info);
            }
        }

        self.part_map = prefix_map;
    }
}

impl<const BLOCKING_IO: bool> ParquetRowsFetcher<BLOCKING_IO> {
    pub fn create(reader: Arc<BlockReader>, settings: ReadSettings, buffer_size: usize) -> Self {
        let uncompressed_buffer = UncompressedBuffer::new(buffer_size);
        Self {
            reader,
            settings,
            uncompressed_buffer,
            part_map: HashMap::new(),
        }
    }

    #[async_backtrace::framed]
    async fn fetch_blocks(
        &self,
        part_set: HashSet<u64>,
    ) -> Result<(Vec<DataBlock>, HashMap<u64, usize>)> {
        let mut chunks = Vec::with_capacity(part_set.len());
        if BLOCKING_IO {
            for prefix in part_set.into_iter() {
                let part = self.part_map[&prefix].clone();
                let chunk = self
                    .reader
                    .sync_read_columns_data_by_merge_io(&self.settings, part)?;
                chunks.push((prefix, chunk));
            }
        } else {
            for prefix in part_set.into_iter() {
                let part = self.part_map[&prefix].clone();
                let part = FusePartInfo::from_part(&part)?;
                let chunk = self
                    .reader
                    .read_columns_data_by_merge_io(
                        &self.settings,
                        &part.location,
                        &part.columns_meta,
                    )
                    .await?;
                chunks.push((prefix, chunk));
            }
        }
        let mut idx_map = HashMap::with_capacity(chunks.len());
        let fetched_blocks = chunks
            .into_iter()
            .enumerate()
            .map(|(idx, (part, chunk))| {
                idx_map.insert(part, idx);
                self.build_block(&self.part_map[&part], chunk)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((fetched_blocks, idx_map))
    }

    fn build_block(&self, part: &PartInfoPtr, chunk: MergeIOReadResult) -> Result<DataBlock> {
        let columns_chunks = chunk.columns_chunks()?;
        let part = FusePartInfo::from_part(part)?;
        self.reader.deserialize_parquet_chunks_with_buffer(
            &part.location,
            part.nums_rows,
            &part.compression,
            &part.columns_meta,
            columns_chunks,
            Some(self.uncompressed_buffer.clone()),
        )
    }
}
