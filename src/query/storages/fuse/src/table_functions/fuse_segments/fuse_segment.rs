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

use common_catalog::table::Table;
use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::NumberColumnBuilder;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use futures_util::TryStreamExt;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;

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
        let maybe_snapshot = tbl.read_table_snapshot().await?;
        if let Some(snapshot) = maybe_snapshot {
            // find the element by snapshot_id in stream
            if let Some(snapshot_id) = snapshot_id {
                // prepare the stream of snapshot
                let snapshot_version = tbl.snapshot_format_version(None).await?;
                let snapshot_location = tbl
                    .meta_location_generator
                    .snapshot_location_from_uuid(&snapshot.snapshot_id, snapshot_version)?;
                let reader = MetaReaders::table_snapshot_reader(tbl.get_operator());
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

        let mut format_versions = NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, len);
        let mut block_count = NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, len);
        let mut row_count = NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, len);
        let mut compressed = NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, len);
        let mut uncompressed = NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, len);
        let mut file_location = StringColumnBuilder::with_capacity(len, 0);

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.table.operator.clone(),
            self.table.schema(),
        );

        let mut row_num = 0;
        let mut end_flag = false;
        let chunk_size = std::cmp::min(
            self.ctx.get_settings().get_max_storage_io_requests()? as usize,
            len,
        );
        for chunk in segment_locations.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<Arc<SegmentInfo>>(chunk, true)
                .await?;

            let take_num = if row_num + chunk_size >= len {
                end_flag = true;
                len - row_num
            } else {
                row_num += chunk_size;
                chunk_size
            };
            for (idx, segment) in segments.into_iter().take(take_num).enumerate() {
                let segment = segment?;
                format_versions.push(NumberScalar::UInt64(segment_locations[idx].1));
                block_count.push(NumberScalar::UInt64(segment.summary.block_count));
                row_count.push(NumberScalar::UInt64(segment.summary.row_count));
                compressed.push(NumberScalar::UInt64(segment.summary.compressed_byte_size));
                uncompressed.push(NumberScalar::UInt64(segment.summary.uncompressed_byte_size));
                file_location.put_slice(segment_locations[idx].0.as_bytes());
                file_location.commit_row();
            }

            if end_flag {
                break;
            }
        }

        Ok(DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Column(Column::String(file_location.build())),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(Column::Number(format_versions.build())),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(Column::Number(block_count.build())),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(Column::Number(row_count.build())),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(Column::Number(uncompressed.build())),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(Column::Number(compressed.build())),
                ),
            ],
            len,
        ))
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
