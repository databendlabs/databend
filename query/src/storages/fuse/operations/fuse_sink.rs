//  Copyright 2021 Datafuse Labs.
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
//

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_arrow::parquet::FileMetaData;
use common_cache::Cache;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use futures::Future;
use opendal::Operator;

use super::AppendOperationLogEntry;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::AsyncSink;
use crate::pipelines::new::processors::AsyncSinker;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::write_block;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::ColumnId;
use crate::storages::fuse::meta::ColumnMeta;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::Statistics;
use crate::storages::fuse::statistics::StatisticsAccumulator;

pub struct FuseSink {
    ctx: Arc<QueryContext>,
    num_block_threshold: usize,
    data_accessor: Operator,
    data_schema: Arc<DataSchema>,
    number_of_blocks_accumulated: usize,
    meta_locations: TableMetaLocationGenerator,
    statistics_accumulator: Arc<RwLock<Option<StatisticsAccumulator>>>,
}

#[async_trait]
impl AsyncSink for FuseSink {
    const NAME: &'static str = "FuseSink";
    type ConsumeFuture<'a> = impl Future<Output = Result<()>> where Self: 'a;

    /// We don't use async_trait for consume method, using GAT instead to make it more static dispatchable.
    fn consume(&mut self, data_block: DataBlock) -> Self::ConsumeFuture<'_> {
        async move {
            if let Some(seg_info) = self.write_block(data_block).await? {
                let block = self
                    .seg_info_to_log_entry(seg_info)
                    .await
                    .and_then(DataBlock::try_from)?;
                self.ctx.push_precommit_block(block);
            }
            Ok(())
        }
    }

    async fn on_finish(&mut self) -> Result<()> {
        let seg_info = self.flush().await?;

        if let Some(seg_info) = seg_info {
            let block = self
                .seg_info_to_log_entry(seg_info)
                .await
                .and_then(DataBlock::try_from)?;
            self.ctx.push_precommit_block(block);
        }
        Ok(())
    }
}

impl FuseSink {
    pub fn create_processor(
        input: Arc<InputPort>,
        ctx: Arc<QueryContext>,
        num_block_threshold: usize,
        data_accessor: Operator,
        data_schema: Arc<DataSchema>,
        meta_locations: TableMetaLocationGenerator,
        statistics_accumulator: Arc<RwLock<Option<StatisticsAccumulator>>>,
    ) -> ProcessorPtr {
        let sink = FuseSink {
            ctx,
            num_block_threshold,
            data_accessor,
            data_schema,
            meta_locations,
            statistics_accumulator,

            number_of_blocks_accumulated: 0,
        };

        AsyncSinker::create(input, sink)
    }

    async fn write_block(&mut self, block: DataBlock) -> Result<Option<SegmentInfo>> {
        let schema = block.schema().to_arrow();
        let location = self.meta_locations.gen_block_location();
        let (file_size, file_meta_data) = write_block(
            &schema,
            block.clone(),
            self.data_accessor.clone(),
            &location,
        )
        .await?;

        let mut acc_writer = self.statistics_accumulator.write();
        let mut acc = acc_writer.take().unwrap_or_default();
        let partial_acc = acc.begin(&block)?;

        let col_metas = Self::column_metas(&file_meta_data)?;
        acc = partial_acc.end(file_size, location, col_metas);
        self.number_of_blocks_accumulated += 1;
        if self.number_of_blocks_accumulated >= self.num_block_threshold {
            let summary = acc.summary(self.data_schema.as_ref())?;
            let seg = SegmentInfo::new(acc.blocks_metas, Statistics {
                row_count: acc.summary_row_count,
                block_count: acc.summary_block_count,
                uncompressed_byte_size: acc.in_memory_size,
                compressed_byte_size: acc.file_size,
                col_stats: summary,
            });

            // Reset state
            self.number_of_blocks_accumulated = 0;
            *acc_writer = None;

            Ok(Some(seg))
        } else {
            // Stash the state
            *acc_writer = Some(acc);
            Ok(None)
        }
    }

    async fn flush(&mut self) -> Result<Option<SegmentInfo>> {
        let mut acc_writer = self.statistics_accumulator.write();
        let acc = acc_writer.take();

        if let Some(acc) = acc {
            let summary = acc.summary(self.data_schema.as_ref())?;
            let seg = SegmentInfo::new(acc.blocks_metas, Statistics {
                row_count: acc.summary_row_count,
                block_count: acc.summary_block_count,
                uncompressed_byte_size: acc.in_memory_size,
                compressed_byte_size: acc.file_size,
                col_stats: summary,
            });

            Ok(Some(seg))
        } else {
            Ok(None)
        }
    }

    async fn seg_info_to_log_entry(
        &self,
        seg_info: SegmentInfo,
    ) -> Result<AppendOperationLogEntry> {
        let locs = self.meta_locations.clone();
        let segment_info_cache = self
            .ctx
            .get_storage_cache_manager()
            .get_table_segment_cache();

        let seg_loc = locs.gen_segment_info_location();
        let bytes = serde_json::to_vec(&seg_info)?;
        self.data_accessor.object(&seg_loc).write(bytes).await?;
        let seg = Arc::new(seg_info);
        let log_entry = AppendOperationLogEntry::new(seg_loc.clone(), seg.clone());

        if let Some(ref cache) = segment_info_cache {
            let cache = &mut cache.write().await;
            cache.put(seg_loc, seg);
        }
        Ok(log_entry)
    }

    fn column_metas(file_meta: &FileMetaData) -> Result<HashMap<ColumnId, ColumnMeta>> {
        // currently we use one group only
        let num_row_groups = file_meta.row_groups.len();
        if num_row_groups != 1 {
            return Err(ErrorCode::ParquetError(format!(
                "invalid parquet file, expects only one row group, but got {}",
                num_row_groups
            )));
        }
        let row_group = &file_meta.row_groups[0];
        let mut col_metas = HashMap::with_capacity(row_group.columns.len());
        for (idx, col_chunk) in row_group.columns.iter().enumerate() {
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
                    let res = ColumnMeta {
                        offset: col_start as u64,
                        len: col_len as u64,
                        num_values,
                    };
                    col_metas.insert(idx as u32, res);
                }
                None => {
                    return Err(ErrorCode::ParquetError(format!(
                        "invalid parquet file, meta data of column idx {} is empty",
                        idx
                    )))
                }
            }
        }
        Ok(col_metas)
    }
}
