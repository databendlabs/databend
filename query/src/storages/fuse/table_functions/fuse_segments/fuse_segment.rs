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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::FuseTable;

pub struct FuseSegment<'a> {
    pub ctx: Arc<QueryContext>,
    pub table: &'a FuseTable,
    pub snapshot_id: String,
}

impl<'a> FuseSegment<'a> {
    pub fn new(ctx: Arc<QueryContext>, table: &'a FuseTable, snapshot_id: String) -> Self {
        Self {
            ctx,
            table,
            snapshot_id,
        }
    }

    pub async fn get_segments(&self) -> Result<DataBlock> {
        let tbl = self.table;
        let snapshot_location = tbl.snapshot_loc();
        let snapshot_version = tbl.snapshot_format_version();
        let reader = MetaReaders::table_snapshot_reader(self.ctx.as_ref());
        let limit = None;
        let snapshots = reader
            .read_snapshot_history(
                snapshot_location,
                snapshot_version,
                tbl.meta_location_generator().clone(),
                limit,
            )
            .await?;

        for snapshot in snapshots {
            if snapshot.snapshot_id.simple().to_string() == self.snapshot_id {
                return self.segments_to_block(snapshot).await;
            }
        }

        Ok(DataBlock::empty())
    }

    async fn segments_to_block(&self, snapshot: Arc<TableSnapshot>) -> Result<DataBlock> {
        let len = snapshot.segments.len();
        let mut format_versions: Vec<u64> = Vec::with_capacity(len);
        let mut block_count: Vec<u64> = Vec::with_capacity(len);
        let mut row_count: Vec<u64> = Vec::with_capacity(len);
        let mut compressed: Vec<u64> = Vec::with_capacity(len);
        let mut uncompressed: Vec<u64> = Vec::with_capacity(len);
        let mut file_location: Vec<Vec<u8>> = Vec::with_capacity(len);

        for segment_location in &snapshot.segments {
            let (location, version) = (segment_location.0.clone(), segment_location.1);
            let reader = MetaReaders::segment_info_reader(&self.ctx);
            let segment_info = reader.read(&location, None, version).await?;

            format_versions.push(version);
            block_count.push(segment_info.summary.block_count);
            row_count.push(segment_info.summary.row_count);
            compressed.push(segment_info.summary.compressed_byte_size);
            uncompressed.push(segment_info.summary.uncompressed_byte_size);
            file_location.push(location.into_bytes());
        }

        Ok(DataBlock::create(FuseSegment::schema(), vec![
            Series::from_data(file_location),
            Series::from_data(format_versions),
            Series::from_data(block_count),
            Series::from_data(row_count),
            Series::from_data(uncompressed),
            Series::from_data(compressed),
        ]))
    }

    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("file_location", Vu8::to_data_type()),
            DataField::new("format_version", u64::to_data_type()),
            DataField::new("block_count", u64::to_data_type()),
            DataField::new("row_count", u64::to_data_type()),
            DataField::new("bytes_uncompressed", u64::to_data_type()),
            DataField::new("bytes_compressed", u64::to_data_type()),
        ])
    }
}
