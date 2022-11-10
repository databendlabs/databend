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

use common_exception::Result;
use common_expression::Chunk;
use common_expression::DataSchema;
use common_expression::DataSchemaRefExt;
use common_expression::DataType;
use common_expression::NumberColumnBuilder;
use common_expression::NumberDataType;
use common_expression::Scalar;
use common_expression::StringColumnBuilder;
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

    pub async fn get_blocks(&self) -> Result<Chunk> {
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

        Ok(Chunk::empty())
    }

    async fn to_block(&self, snapshot: Arc<TableSnapshot>) -> Result<Chunk> {
        let len = snapshot.summary.block_count as usize;
        let snapshot_id = snapshot.snapshot_id.simple().to_string().into_bytes();
        let timestamp = snapshot.timestamp.map(|dt| (dt.timestamp_micros()));
        let mut block_location = StringColumnBuilder::with_capacity(len, len);
        let mut block_size = NumberColumnBuilder::with_capacity(NumberDataType::UInt64, len);
        let mut file_size = NumberColumnBuilder::with_capacity(NumberDataType::UInt64, len);
        let mut row_count = NumberColumnBuilder::with_capacity(NumberDataType::UInt64, len);
        let mut bloom_filter_location = StringColumnBuilder::with_capacity(len, len);
        let mut bloom_filter_size = NumberColumnBuilder::with_capacity(NumberDataType::UInt64, len);

        let segments_io = SegmentsIO::create(self.ctx.clone(), self.table.operator.clone());
        let segments = segments_io.read_segments(&snapshot.segments).await?;
        for segment in segments {
            let segment = segment?;
            segment.blocks.iter().for_each(|block| {
                let block = block.as_ref();
                block_location.put_slice(block.location.0.clone().into_bytes());
                block_location.commit_row();
                block_size.push(NumberScalar::UInt64(block.block_size));
                file_size.push(NumberScalar::UInt64(block.file_size));
                row_count.push(NumberScalar::UInt64(block.row_count));
                bloom_filter_location.put_slice(
                    block
                        .bloom_filter_index_location
                        .as_ref()
                        .map(|(s, _)| s.to_owned().into_bytes()),
                );
                bloom_filter_location.commit_row();
                bloom_filter_size.push(NumberScalar::UInt64(block.bloom_filter_index_size));
            });
        }

        Ok(Chunk::new(
            vec![
                (
                    Value::Scalar(Scalar::String(snapshot_id.to_vec())),
                    DataType::String,
                ),
                (
                    Value::Scalar(Scalar::Timestamp(timestamp)),
                    DataType::Nullable(Box::new(DataType::Timestamp)),
                ),
                (
                    Value::Column(Column::String(block_location.build())),
                    DataType::String,
                ),
                (
                    Value::Column(Column::Number(block_size.build())),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::Number(file_size.build())),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::Number(row_count.build())),
                    DataType::Number(NumberDataType::UInt64),
                ),
                (
                    Value::Column(Column::String(bloom_filter_location.build())),
                    DataType::String,
                ),
                (
                    Value::Column(Column::Number(bloom_filter_size.build())),
                    DataType::Number(NumberDataType::UInt64),
                ),
            ],
            len,
        ))
    }

    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("snapshot_id", SchemaDataType::String),
            DataField::new(
                "timestamp",
                SchemaDataType::Nullable(Box::new(SchemaDataType::Timestamp)),
            ),
            DataField::new("block_location", SchemaDataType::String),
            DataField::new("block_size", SchemaDataType::Number(NumberDataType::UInt64)),
            DataField::new("file_size", SchemaDataType::Number(NumberDataType::UInt64)),
            DataField::new("row_count", SchemaDataType::Number(NumberDataType::UInt64)),
            DataField::new(
                "bloom_filter_location",
                SchemaDataType::Nullable(Box::new(SchemaDataType::String)),
            ),
            DataField::new(
                "bloom_filter_size",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
        ])
    }
}
