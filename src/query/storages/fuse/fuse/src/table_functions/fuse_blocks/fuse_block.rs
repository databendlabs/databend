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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_storages_table_meta::meta::TableSnapshot;
use futures_util::TryStreamExt;

use crate::io::MetaReaders;
use crate::io::SegmentsIO;
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
        let maybe_snapshot = tbl.read_table_snapshot().await?;
        if let Some(snapshot) = maybe_snapshot {
            if self.snapshot_id.is_none() {
                return self.to_block(snapshot).await;
            }

            // prepare the stream of snapshot
            let snapshot_version = tbl.snapshot_format_version().await?;
            let snapshot_location = tbl
                .meta_location_generator
                .snapshot_location_from_uuid(&snapshot.snapshot_id, snapshot_version)?;
            let reader = MetaReaders::table_snapshot_reader(tbl.get_operator());
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

    async fn to_block(&self, snapshot: Arc<TableSnapshot>) -> Result<DataBlock> {
        let len = snapshot.summary.block_count as usize;
        let snapshot_id = vec![snapshot.snapshot_id.simple().to_string().into_bytes()];
        let timestamp = vec![snapshot.timestamp.map(|dt| (dt.timestamp_micros()))];
        let mut block_location: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut block_size: Vec<u64> = Vec::with_capacity(len);
        let mut file_size: Vec<u64> = Vec::with_capacity(len);
        let mut row_count: Vec<u64> = Vec::with_capacity(len);
        let mut bloom_filter_location: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
        let mut bloom_filter_size: Vec<u64> = Vec::with_capacity(len);

        let segments_io = SegmentsIO::create(self.ctx.clone(), self.table.operator.clone());
        let segments = segments_io.read_segments(&snapshot.segments).await?;
        for segment in segments {
            let segment = segment?;
            segment.blocks.iter().for_each(|block| {
                let block = block.as_ref();
                block_location.push(block.location.0.clone().into_bytes());
                block_size.push(block.block_size);
                file_size.push(block.file_size);
                row_count.push(block.row_count);
                bloom_filter_location.push(
                    block
                        .bloom_filter_index_location
                        .as_ref()
                        .map(|(s, _)| s.to_owned().into_bytes()),
                );
                bloom_filter_size.push(block.bloom_filter_index_size);
            });
        }

        Ok(DataBlock::create(FuseBlock::schema(), vec![
            Arc::new(ConstColumn::new(Series::from_data(snapshot_id), len)),
            Arc::new(ConstColumn::new(Series::from_data(timestamp), len)),
            Series::from_data(block_location),
            Series::from_data(block_size),
            Series::from_data(file_size),
            Series::from_data(row_count),
            Series::from_data(bloom_filter_location),
            Series::from_data(bloom_filter_size),
        ]))
    }

    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("snapshot_id", Vu8::to_data_type()),
            DataField::new_nullable("timestamp", TimestampType::new_impl()),
            DataField::new("block_location", Vu8::to_data_type()),
            DataField::new("block_size", u64::to_data_type()),
            DataField::new("file_size", u64::to_data_type()),
            DataField::new("row_count", u64::to_data_type()),
            DataField::new_nullable("bloom_filter_location", Vu8::to_data_type()),
            DataField::new("bloom_filter_size", u64::to_data_type()),
        ])
    }
}
