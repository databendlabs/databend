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

use common_datablocks::DataBlock;
use common_exception::Result;
use common_planners::PartInfoPtr;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;
use opendal::Operator;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::serialize_data_block;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::Statistics as FuseMetaStatistics;
use crate::storages::fuse::statistics::accumulator::BlockStatistics;
use crate::storages::fuse::statistics::StatisticsAccumulator;
use crate::storages::fuse::FuseTable;
use crate::storages::result::result_locations::ResultLocations;
use crate::storages::result::result_table::ResultStorageInfo;
use crate::storages::result::result_table::ResultTableMeta;
use crate::storages::result::ResultQueryInfo;

pub struct ResultTableWriter {
    pub query_info: ResultQueryInfo,
    pub data_accessor: Operator,
    pub locations: ResultLocations,
    pub accumulator: StatisticsAccumulator,
}

impl ResultTableWriter {
    pub async fn new(ctx: Arc<QueryContext>, query_info: ResultQueryInfo) -> Result<Self> {
        let data_accessor = ctx.get_storage_operator()?;
        let query_id = query_info.query_id.clone();
        Ok(ResultTableWriter {
            query_info,
            locations: ResultLocations::new(&query_id),
            data_accessor,
            accumulator: StatisticsAccumulator::new(),
        })
    }

    pub async fn abort(&self) -> Result<()> {
        for meta in &self.accumulator.blocks_metas {
            self.data_accessor.object(&meta.location.0).delete().await?;
        }
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<()> {
        let acc = std::mem::take(&mut self.accumulator);
        let col_stats = acc.summary()?;
        let segment_info = SegmentInfo::new(acc.blocks_metas, FuseMetaStatistics {
            row_count: acc.summary_row_count,
            block_count: acc.summary_block_count,
            uncompressed_byte_size: acc.in_memory_size,
            compressed_byte_size: acc.file_size,
            col_stats,
            cluster_stats: None,
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
        Ok(())
    }

    pub async fn append_block(&mut self, block: DataBlock) -> Result<PartInfoPtr> {
        let location = self.locations.gen_block_location();
        let mut data = Vec::with_capacity(100 * 1024 * 1024);
        let block_statistics = BlockStatistics::from(&block, location.clone(), None)?;
        let (size, meta_data) = serialize_data_block(block, &mut data)?;
        self.data_accessor
            .object(&location)
            .write(data)
            .await
            .map_err(|e| {
                println!("error {}", e);
                e
            })?;
        self.accumulator
            .add_block(size, meta_data, block_statistics)?;
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
