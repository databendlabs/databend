//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_expression::DataBlock;

use super::fuse_rows_fetcher::RowsFetcher;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::UncompressedBuffer;
use crate::FusePartInfo;
use crate::MergeIOReadResult;

pub(super) struct ParquetRowsFetcher<const BLOCKING_IO: bool> {
    settings: ReadSettings,
    reader: Arc<BlockReader>,
    uncompressed_buffer: Arc<UncompressedBuffer>,
}

#[async_trait::async_trait]
impl<const BLOCKING_IO: bool> RowsFetcher for ParquetRowsFetcher<BLOCKING_IO> {
    async fn fetch(
        &self,
        part_map: &HashMap<u64, PartInfoPtr>,
        part_set: HashSet<u64>,
    ) -> Result<(Vec<DataBlock>, HashMap<u64, usize>)> {
        let mut chunks = Vec::with_capacity(part_set.len());
        if BLOCKING_IO {
            for part_id in part_set.into_iter() {
                let part = part_map[&part_id].clone();
                let chunk = self
                    .reader
                    .sync_read_columns_data_by_merge_io(&self.settings, part)?;
                chunks.push((part_id, chunk));
            }
        } else {
            for part_id in part_set.into_iter() {
                let part = part_map[&part_id].clone();
                let part = FusePartInfo::from_part(&part)?;
                let chunk = self
                    .reader
                    .read_columns_data_by_merge_io(
                        &self.settings,
                        &part.location,
                        &part.columns_meta,
                    )
                    .await?;
                chunks.push((part_id, chunk));
            }
        }
        let mut part_idx_map = HashMap::with_capacity(chunks.len());
        let fetched_blocks = chunks
            .into_iter()
            .enumerate()
            .map(|(idx, (part, chunk))| {
                part_idx_map.insert(part, idx);
                self.build_block(&part_map[&part], chunk)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((fetched_blocks, part_idx_map))
    }

    fn reader(&self) -> &BlockReader {
        &self.reader
    }
}

impl<const BLOCKING_IO: bool> ParquetRowsFetcher<BLOCKING_IO> {
    pub fn create(reader: Arc<BlockReader>, settings: ReadSettings, buffer_size: usize) -> Self {
        let uncompressed_buffer = UncompressedBuffer::new(buffer_size);
        Self {
            reader,
            settings,
            uncompressed_buffer,
        }
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
