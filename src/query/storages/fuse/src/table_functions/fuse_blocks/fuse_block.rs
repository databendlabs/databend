//  Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_base::base::tokio::sync::Semaphore;
use common_base::base::Runtime;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::TableSnapshot;
use futures_util::future;
use futures_util::TryStreamExt;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::sessions::TableContext;
use crate::FuseTable;

pub struct FuseBlock<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
    pub snapshot_id: Option<String>,
}

impl<'a> FuseBlock<'a> {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        table: &'a FuseTable,
        snapshot_id: Option<String>,
    ) -> Self {
        Self {
            ctx,
            table,
            snapshot_id,
        }
    }

    pub async fn get_blocks(&self) -> Result<DataBlock> {
        let tbl = self.table;
        let maybe_snapshot = tbl.read_table_snapshot(self.ctx.clone()).await?;
        if let Some(snapshot) = maybe_snapshot {
            if self.snapshot_id.is_none() {
                return self.to_block(snapshot).await;
            }

            // prepare the stream of snapshot
            let snapshot_version = tbl.snapshot_format_version();
            let snapshot_location = tbl
                .meta_location_generator
                .snapshot_location_from_uuid(&snapshot.snapshot_id, snapshot_version)?;
            let reader = MetaReaders::table_snapshot_reader(self.ctx.clone());
            let mut snapshot_stream = reader.snapshot_history(
                snapshot_location,
                snapshot_version,
                tbl.meta_location_generator().clone(),
            );

            // find the element by snapshot_id in stream
            while let Some(snapshot) = snapshot_stream.try_next().await? {
                if snapshot.snapshot_id.simple().to_string() == self.snapshot_id.clone().unwrap() {
                    return self.to_block(snapshot).await;
                }
            }
        }

        Ok(DataBlock::empty_with_schema(Self::schema()))
    }

    async fn get_segment(ctx: Arc<dyn TableContext>, loc: Location) -> Result<Arc<SegmentInfo>> {
        let (path, ver) = loc;
        let reader = MetaReaders::segment_info_reader(ctx.as_ref());
        reader.read(path, None, ver).await
    }

    async fn to_block(&self, snapshot: Arc<TableSnapshot>) -> Result<DataBlock> {
        let len = snapshot.summary.block_count as usize;
        let snapshot_id = vec![snapshot.snapshot_id.simple().to_string().into_bytes()];
        let timestamp = vec![snapshot.timestamp.map(|dt| (dt.timestamp_micros()) as i64)];
        let mut block_location: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut block_size: Vec<u64> = Vec::with_capacity(len);
        let mut bloom_filter_location: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
        let mut bloom_filter_size: Vec<u64> = Vec::with_capacity(len);

        // 1.1 combine all the tasks.
        let tasks = std::iter::from_fn(|| {
            let ctx = self.ctx.clone();
            let segments = snapshot.segments.clone();
            if let Some(location) = segments.into_iter().next() {
                return Some(async move { Self::get_segment(ctx, location.clone()).await });
            }
            None
        });

        // 1.2 build the runtime.
        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        let max_concurrent_prune_setting =
            self.ctx.get_settings().get_max_concurrent_prune()? as usize;
        let semaphore = Arc::new(Semaphore::new(max_concurrent_prune_setting));
        let segments_runtime = Arc::new(Runtime::with_worker_threads(
            max_threads,
            Some("fuse-block-segments-worker".to_owned()),
        )?);

        // 1.3 spawn all the tasks to the runtime.
        let join_handlers = segments_runtime
            .try_spawn_batch(semaphore.clone(), tasks)
            .await?;

        // 1.4 get all the result.
        let joint: Vec<Result<Arc<SegmentInfo>>> = future::try_join_all(join_handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("get segments failure, {}", e)))?;
        for segment in joint {
            let segment = segment?;
            segment.blocks.clone().into_iter().for_each(|block| {
                block_location.push(block.location.0.into_bytes());
                block_size.push(block.block_size);
                bloom_filter_location.push(
                    block
                        .bloom_filter_index_location
                        .map(|(s, _)| s.into_bytes()),
                );
                bloom_filter_size.push(block.bloom_filter_index_size);
            });
        }

        Ok(DataBlock::create(FuseBlock::schema(), vec![
            Arc::new(ConstColumn::new(Series::from_data(snapshot_id), len)),
            Arc::new(ConstColumn::new(Series::from_data(timestamp), len)),
            Series::from_data(block_location),
            Series::from_data(block_size),
            Series::from_data(bloom_filter_location),
            Series::from_data(bloom_filter_size),
        ]))
    }

    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("snapshot_id", Vu8::to_data_type()),
            DataField::new_nullable("timestamp", TimestampType::new_impl(6)),
            DataField::new("block_location", Vu8::to_data_type()),
            DataField::new("block_size", u64::to_data_type()),
            DataField::new_nullable("bloom_filter_location", Vu8::to_data_type()),
            DataField::new("bloom_filter_size", u64::to_data_type()),
        ])
    }
}
