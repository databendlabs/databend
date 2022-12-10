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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::ColumnFrom;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRefExt;
use common_expression::TableDataType;
use common_expression::Value;
use common_storages_table_meta::meta::TableSnapshotLite;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::io::SnapshotsIO;
use crate::io::TableMetaLocationGenerator;
use crate::sessions::TableContext;
use crate::FuseTable;

pub struct FuseSnapshot<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
}

impl<'a> FuseSnapshot<'a> {
    pub fn new(ctx: Arc<dyn TableContext>, table: &'a FuseTable) -> Self {
        Self { ctx, table }
    }

    pub async fn get_snapshots(self, limit: Option<usize>) -> Result<Chunk> {
        let meta_location_generator = self.table.meta_location_generator.clone();
        let snapshot_location = self.table.snapshot_loc().await?;
        let snapshot = self.table.read_table_snapshot().await?;
        if let Some(snapshot_location) = snapshot_location {
            let snapshot_version = self.table.snapshot_format_version().await?;
            let snapshots_io = SnapshotsIO::create(
                self.ctx.clone(),
                self.table.operator.clone(),
                snapshot_version,
            );
            let snapshot_lite = match limit {
                None => {
                    // Use SnapshotIO only if limitation is None.
                    //
                    // SnapshotsIO lists snapshots from object storage, if we limit the number of
                    // items being list , there might be the case that the snapshots returned
                    // can not be chained together.
                    let (snapshots, _) = snapshots_io
                        .read_snapshot_lites(
                            snapshot_location,
                            None,
                            false,
                            snapshot.and_then(|s| s.timestamp),
                            &|_| {},
                        )
                        .await?;
                    Ok(snapshots)
                }
                Some(l) => {
                    // SnapshotHistoryReader (which TableSnapshotReader impls) traverses the history
                    // of snapshot sequentially, by using the TableSnapshot::prev_snapshot_id, which
                    // guarantees the snapshot returned can be chained together
                    let table_snapshot_reader = MetaReaders::table_snapshot_reader(
                        self.ctx.get_data_operator()?.operator(),
                    );
                    table_snapshot_reader
                        .snapshot_history(
                            snapshot_location,
                            snapshot_version,
                            meta_location_generator.clone(),
                        )
                        .map_ok(|snapshot| TableSnapshotLite::from(snapshot.as_ref()))
                        .take(l)
                        .try_collect::<Vec<_>>()
                        .await
                }
            }?;

            return self.to_block(&meta_location_generator, &snapshot_lite, snapshot_version);
        }
        Ok(Chunk::empty())
    }

    fn to_chunk(
        &self,
        location_generator: &TableMetaLocationGenerator,
        snapshots: &[TableSnapshotLite],
        latest_snapshot_version: u64,
    ) -> Result<Chunk> {
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
        let mut index_size: Vec<u64> = Vec::with_capacity(len);
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
            format_versions.push(s.format_version);
            segment_count.push(s.segment_count);
            block_count.push(s.block_count);
            row_count.push(s.row_count);
            compressed.push(s.compressed_byte_size);
            uncompressed.push(s.uncompressed_byte_size);
            index_size.push(s.index_size);
            timestamps.push(s.timestamp.map(|dt| (dt.timestamp_micros())));
            current_snapshot_version = ver;
        }

        Ok(Chunk::new_from_sequence(
            vec![
                (
                    Value::Column(Column::from_data(snapshot_ids)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(snapshot_locations)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(format_versions)),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::from_data(prev_snapshot_ids)),
                    DataType::String.wrap_nullable(),
                ),
                (
                    Value::Column(Column::from_data(segment_count)),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::from_data(block_count)),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::from_data(row_count)),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::from_data(uncompressed)),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::from_data(compressed)),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::from_data(index_size)),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::from_data(timestamps)),
                    DataType::Timestamp.wrap_nullable(),
                ),
            ],
            len,
        ))
    }

    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("snapshot_id", TableDataType::String),
            DataField::new("snapshot_location", TableDataType::String),
            DataField::new(
                "format_version",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new(
                "previous_snapshot_id",
                TableDataType::String.wrap_nullable(),
            ),
            DataField::new(
                "segment_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new("block_count", TableDataType::Number(NumberDataType::UInt64)),
            DataField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            DataField::new(
                "bytes_uncompressed",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new(
                "bytes_compressed",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new("index_size", TableDataType::Number(NumberDataType::UInt64)),
            DataField::new("timestamp", TableDataType::Timestamp.wrap_nullable()),
        ])
    }
}
