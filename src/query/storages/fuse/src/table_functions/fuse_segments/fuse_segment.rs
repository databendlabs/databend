// Copyright 2021 Datafuse Labs
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

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use futures_util::TryStreamExt;

use crate::io::MetaReaders;
use crate::io::SegmentsIO;
use crate::io::SnapshotHistoryReader;
use crate::sessions::TableContext;
use crate::FuseTable;

pub struct FuseSegment<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
    pub snapshot_id: Option<String>,
    pub limit: Option<usize>,
}

impl<'a> FuseSegment<'a> {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        table: &'a FuseTable,
        snapshot_id: Option<String>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            ctx,
            table,
            snapshot_id,
            limit,
        }
    }

    #[async_backtrace::framed]
    pub async fn get_segments(&self) -> Result<DataBlock> {
        let tbl = self.table;
        let snapshot_id = self.snapshot_id.clone();
        let maybe_snapshot = tbl.read_table_snapshot(self.ctx.txn_mgr()).await?;
        if let Some(snapshot) = maybe_snapshot {
            // find the element by snapshot_id in stream
            if let Some(snapshot_id) = snapshot_id {
                // prepare the stream of snapshot
                let snapshot_version = tbl.snapshot_format_version(None).await?;
                let snapshot_location = tbl
                    .meta_location_generator
                    .snapshot_location_from_uuid(&snapshot.snapshot_id, snapshot_version)?;
                let reader =
                    MetaReaders::table_snapshot_reader(tbl.get_operator(), self.ctx.txn_mgr());
                let mut snapshot_stream = reader.snapshot_history(
                    snapshot_location,
                    snapshot_version,
                    tbl.meta_location_generator().clone(),
                );
                while let Some((snapshot, _)) = snapshot_stream.try_next().await? {
                    if snapshot.snapshot_id.simple().to_string() == snapshot_id {
                        return self.to_block(&snapshot.segments).await;
                    }
                }
            } else {
                return self.to_block(&snapshot.segments).await;
            }
        }

        Ok(DataBlock::empty_with_schema(Arc::new(
            Self::schema().into(),
        )))
    }

    #[async_backtrace::framed]
    async fn to_block(&self, segment_locations: &[Location]) -> Result<DataBlock> {
        let limit = self.limit.unwrap_or(usize::MAX);
        let len = std::cmp::min(segment_locations.len(), limit);

        let mut format_versions: Vec<u64> = Vec::with_capacity(len);
        let mut block_count: Vec<u64> = Vec::with_capacity(len);
        let mut row_count: Vec<u64> = Vec::with_capacity(len);
        let mut compressed: Vec<u64> = Vec::with_capacity(len);
        let mut uncompressed: Vec<u64> = Vec::with_capacity(len);
        let mut file_location: Vec<String> = Vec::with_capacity(len);

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.table.operator.clone(),
            self.table.schema(),
        );

        let mut row_num = 0;
        let mut end_flag = false;
        let chunk_size =
            std::cmp::min(self.ctx.get_settings().get_max_threads()? as usize * 4, len).max(1);
        for chunk in segment_locations.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;

            for (idx, segment) in segments.into_iter().enumerate() {
                let segment = segment?;
                format_versions.push(segment_locations[idx].1);
                block_count.push(segment.summary.block_count);
                row_count.push(segment.summary.row_count);
                compressed.push(segment.summary.compressed_byte_size);
                uncompressed.push(segment.summary.uncompressed_byte_size);
                file_location.push(segment_locations[idx].0.clone());

                row_num += 1;
                if row_num >= limit {
                    end_flag = true;
                    break;
                }
            }

            if end_flag {
                break;
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(file_location),
            UInt64Type::from_data(format_versions),
            UInt64Type::from_data(block_count),
            UInt64Type::from_data(row_count),
            UInt64Type::from_data(uncompressed),
            UInt64Type::from_data(compressed),
        ]))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("file_location", TableDataType::String),
            TableField::new(
                "format_version",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("block_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "bytes_uncompressed",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "bytes_compressed",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ])
    }
}
