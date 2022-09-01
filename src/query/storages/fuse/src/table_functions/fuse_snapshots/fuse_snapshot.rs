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

use common_catalog::table::Table;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_fuse_meta::meta::TableSnapshot;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::io::TableMetaLocationGenerator;
use crate::sessions::TableContext;
use crate::FuseTable;

pub struct FuseSnapshot {
    pub ctx: Arc<dyn TableContext>,
    pub table: Arc<dyn Table>,
}

impl FuseSnapshot {
    pub fn new(ctx: Arc<dyn TableContext>, table: Arc<dyn Table>) -> Self {
        Self { ctx, table }
    }

    /// Get table snapshot history as stream of data block
    /// For cases that caller inside sync context, i.e. using async catalog api is not convenient
    pub fn new_snapshot_history_stream(
        ctx: Arc<dyn TableContext>,
        database_name: String,
        table_name: String,
        catalog_name: String,
        limit: Option<usize>,
    ) -> SendableDataBlockStream {
        // prepare the future that resolved the table
        let table_instance = {
            let ctx = ctx.clone();
            async move {
                let tenant_id = ctx.get_tenant();
                let tbl = ctx
                    .get_catalog(catalog_name.as_str())?
                    .get_table(
                        tenant_id.as_str(),
                        database_name.as_str(),
                        table_name.as_str(),
                    )
                    .await?;
                Ok(tbl)
            }
        };

        // chain the future with a stream of snapshot, for the resolved table,  as data blocks
        let resolved_table = futures::stream::once(table_instance);
        let stream = resolved_table.map(move |item: Result<_>| match item {
            Ok(table) => {
                let fuse_snapshot = FuseSnapshot::new(ctx.clone(), table);
                Ok(fuse_snapshot.get_history_stream_as_blocks(limit)?)
            }
            Err(e) => Err(e),
        });

        // flat it into single stream of data blocks
        stream.try_flatten().boxed()
    }

    pub fn get_history_stream_as_blocks(
        self,
        limit: Option<usize>,
    ) -> Result<SendableDataBlockStream> {
        let tbl = FuseTable::try_from_table(self.table.as_ref())?;
        let snapshot_location = tbl.snapshot_loc();
        let meta_location_generator = tbl.meta_location_generator.clone();
        if let Some(snapshot_location) = snapshot_location {
            let snapshot_version = tbl.snapshot_format_version();
            let snapshot_reader = MetaReaders::table_snapshot_reader(self.ctx.clone());
            let snapshot_stream = snapshot_reader.snapshot_history(
                snapshot_location,
                snapshot_version,
                tbl.meta_location_generator().clone(),
            );

            // map snapshot to data block
            let block_stream = snapshot_stream.map(move |snapshot| match snapshot {
                Ok(snapshot) => Self::snapshots_to_block(
                    &meta_location_generator,
                    vec![snapshot],
                    snapshot_version,
                ),
                Err(e) => Err(e),
            });

            // limit if necessary
            if let Some(limit) = limit {
                Ok(block_stream.take(limit).boxed())
            } else {
                Ok(block_stream.boxed())
            }
        } else {
            // to carries the schema info back to caller, instead of an empty stream,
            // a stream of single empty block item returned
            let data_block = DataBlock::empty_with_schema(FuseSnapshot::schema());
            Ok(DataBlockStream::create(FuseSnapshot::schema(), None, vec![data_block]).boxed())
        }
    }

    fn snapshots_to_block(
        location_generator: &TableMetaLocationGenerator,
        snapshots: Vec<Arc<TableSnapshot>>,
        latest_snapshot_version: u64,
    ) -> Result<DataBlock> {
        let len = snapshots.len();
        let mut snapshot_ids: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut snapshot_locations: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut prev_snapshot_ids: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
        let mut format_versions: Vec<u64> = Vec::with_capacity(len);
        let mut segment_count: Vec<u64> = Vec::with_capacity(len);
        let mut block_count: Vec<u64> = Vec::with_capacity(len);
        let mut row_count: Vec<u64> = Vec::with_capacity(len);
        let mut compressed: Vec<u64> = Vec::with_capacity(len);
        let mut uncompressed: Vec<u64> = Vec::with_capacity(len);
        let mut timestamps: Vec<Option<i64>> = Vec::with_capacity(len);
        let mut current_snapshot_version = latest_snapshot_version;
        for s in snapshots {
            snapshot_ids.push(s.snapshot_id.simple().to_string().into_bytes());
            snapshot_locations.push(
                location_generator
                    .snapshot_location_from_uuid(&s.snapshot_id, current_snapshot_version)?
                    .into_bytes(),
            );
            let (id, ver) = match s.prev_snapshot_id {
                Some((id, v)) => (Some(id.simple().to_string().into_bytes()), v),
                None => (None, 0),
            };
            prev_snapshot_ids.push(id);
            format_versions.push(current_snapshot_version);
            segment_count.push(s.segments.len() as u64);
            block_count.push(s.summary.block_count);
            row_count.push(s.summary.row_count);
            compressed.push(s.summary.compressed_byte_size);
            uncompressed.push(s.summary.uncompressed_byte_size);
            timestamps.push(s.timestamp.map(|dt| (dt.timestamp_micros()) as i64));
            current_snapshot_version = ver;
        }

        Ok(DataBlock::create(FuseSnapshot::schema(), vec![
            Series::from_data(snapshot_ids),
            Series::from_data(snapshot_locations),
            Series::from_data(format_versions),
            Series::from_data(prev_snapshot_ids),
            Series::from_data(segment_count),
            Series::from_data(block_count),
            Series::from_data(row_count),
            Series::from_data(uncompressed),
            Series::from_data(compressed),
            Series::from_data(timestamps),
        ]))
    }

    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("snapshot_id", Vu8::to_data_type()),
            DataField::new("snapshot_location", Vu8::to_data_type()),
            DataField::new("format_version", u64::to_data_type()),
            DataField::new_nullable("previous_snapshot_id", Vu8::to_data_type()),
            DataField::new("segment_count", u64::to_data_type()),
            DataField::new("block_count", u64::to_data_type()),
            DataField::new("row_count", u64::to_data_type()),
            DataField::new("bytes_uncompressed", u64::to_data_type()),
            DataField::new("bytes_compressed", u64::to_data_type()),
            DataField::new_nullable("timestamp", TimestampType::new_impl(6)),
        ])
    }
}
