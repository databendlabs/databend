// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use backon::ExponentialBackoff;
use backon::Retryable;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::serialize_to_parquet;
use common_datablocks::DataBlock;
use common_datablocks::SendableDataBlockStream;
use common_exception::Result;
use common_storages_fuse::operations::util;
use common_storages_fuse::statistics::BlockStatistics;
use common_storages_fuse::statistics::StatisticsAccumulator;
use common_storages_fuse::FuseTable;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics as FuseMetaStatistics;
use futures::StreamExt;
use opendal::Operator;
use tracing::warn;

use crate::result_locations::ResultLocations;
use crate::result_table::ResultStorageInfo;
use crate::result_table::ResultTableMeta;
use crate::ResultQueryInfo;

pub struct ResultTableWriter {
    stopped: bool,
    pub query_info: ResultQueryInfo,
    pub data_accessor: Operator,
    pub locations: ResultLocations,
    pub accumulator: StatisticsAccumulator,
}

impl ResultTableWriter {
    pub async fn new(ctx: Arc<dyn TableContext>, query_info: ResultQueryInfo) -> Result<Self> {
        let data_accessor = ctx.get_data_operator()?.operator();
        let query_id = query_info.query_id.clone();
        Ok(ResultTableWriter {
            query_info,
            locations: ResultLocations::new(&query_id),
            data_accessor,
            accumulator: StatisticsAccumulator::default(),
            stopped: false,
        })
    }

    pub async fn abort(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        for meta in &self.accumulator.blocks_metas {
            self.data_accessor.object(&meta.location.0).delete().await?;
        }
        self.stopped = true;
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        let acc = std::mem::take(&mut self.accumulator);
        let col_stats = acc.summary()?;
        let segment_info = SegmentInfo::new(acc.blocks_metas, FuseMetaStatistics {
            row_count: acc.summary_row_count,
            block_count: acc.summary_block_count,
            perfect_block_count: acc.perfect_block_count,
            uncompressed_byte_size: acc.in_memory_size,
            compressed_byte_size: acc.file_size,
            col_stats,
            index_size: 0,
        });

        let meta = ResultTableMeta {
            query: self.query_info.clone(),
            storage: ResultStorageInfo::FuseSegment(segment_info),
        };
        let meta_data = serde_json::to_vec(&meta)?;
        let meta_location = self.locations.get_meta_location();
        self.data_accessor
            .object(&meta_location)
            .write(meta_data)
            .await?;
        self.stopped = true;
        Ok(())
    }

    pub async fn append_block(&mut self, block: DataBlock) -> Result<PartInfoPtr> {
        let location = self.locations.gen_block_location();
        let mut data = Vec::with_capacity(100 * 1024 * 1024);
        let block_statistics = BlockStatistics::from(&block, location.clone(), None, None)?;
        let schema = block.schema().clone();
        let (size, meta_data) = serialize_to_parquet(vec![block], &schema, &mut data)?;

        let object = self.data_accessor.object(&location);
        { || object.write(data.as_slice()) }
            .retry(ExponentialBackoff::default().with_jitter())
            .when(|err| err.is_temporary())
            .notify(|err, dur| {
                warn!(
                    "append block write retry after {}s for error {:?}",
                    dur.as_secs(),
                    err
                )
            })
            .await?;

        let meta = util::column_metas(&meta_data)?;
        self.accumulator
            .add_block(size, meta, block_statistics, None, 0)?;
        Ok(self.get_last_part_info())
    }

    pub async fn write_stream(&mut self, mut stream: SendableDataBlockStream) -> Result<()> {
        while let Some(Ok(block)) = stream.next().await {
            self.append_block(block).await?;
        }
        self.commit().await?;
        Ok(())
    }

    pub fn get_last_part_info(&mut self) -> PartInfoPtr {
        let meta = self.accumulator.blocks_metas.last().unwrap();
        FuseTable::all_columns_part(meta)
    }
}
