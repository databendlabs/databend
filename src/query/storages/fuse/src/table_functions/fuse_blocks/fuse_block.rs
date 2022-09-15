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
use futures_util::TryStreamExt;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::sessions::TableContext;
use crate::FuseTable;

pub struct FuseBlock<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
    pub snapshot_id: String,
}

impl<'a> FuseBlock<'a> {
    pub fn new(ctx: Arc<dyn TableContext>, table: &'a FuseTable, snapshot_id: String) -> Self {
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
                if snapshot.snapshot_id.simple().to_string() == self.snapshot_id {
                    let len = snapshot.summary.block_count as usize;
                    let snapshot_id = vec![self.snapshot_id.clone().into_bytes()];
                    let timestamp =
                        vec![snapshot.timestamp.map(|dt| (dt.timestamp_micros()) as i64)];
                    let mut block_location: Vec<Vec<u8>> = Vec::with_capacity(len);
                    let mut block_size: Vec<u64> = Vec::with_capacity(len);
                    let mut bloom_filter_location: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
                    let mut bloom_filter_size: Vec<u64> = Vec::with_capacity(len);

                    let reader = MetaReaders::segment_info_reader(self.ctx.as_ref());
                    for (x, ver) in &snapshot.segments {
                        let segment = reader.read(x, None, *ver).await?;
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

                    return Ok(DataBlock::create(FuseBlock::schema(), vec![
                        Arc::new(ConstColumn::new(Series::from_data(snapshot_id), len)),
                        Arc::new(ConstColumn::new(Series::from_data(timestamp), len)),
                        Series::from_data(block_location),
                        Series::from_data(block_size),
                        Series::from_data(bloom_filter_location),
                        Series::from_data(bloom_filter_size),
                    ]));
                }
            }
        }

        Ok(DataBlock::empty_with_schema(Self::schema()))
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
