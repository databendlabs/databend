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
use common_expression::types::number::NumberColumnBuilder;
use common_expression::types::number::NumberScalar;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::FromOptData;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use futures_util::TryStreamExt;
use storages_common_table_meta::meta::TableSnapshot;

use crate::io::MetaReaders;
use crate::io::SegmentsIO;
use crate::io::SnapshotHistoryReader;
use crate::sessions::TableContext;
use crate::FuseTable;

pub struct FuseBlock<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
    pub snapshot_id: Option<String>,
    pub limit: Option<usize>,
}

impl<'a> FuseBlock<'a> {
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
    pub async fn get_blocks(&self) -> Result<DataBlock> {
        let tbl = self.table;
        let maybe_snapshot = tbl.read_table_snapshot().await?;
        if let Some(snapshot) = maybe_snapshot {
            if self.snapshot_id.is_none() {
                return self.to_block(snapshot).await;
            }

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

            // find the element by snapshot_id in stream
            while let Some((snapshot, _)) = snapshot_stream.try_next().await? {
                if snapshot.snapshot_id.simple().to_string() == self.snapshot_id.clone().unwrap() {
                    return self.to_block(snapshot).await;
                }
            }
        }

        Ok(DataBlock::empty_with_schema(Arc::new(
            Self::schema().into(),
        )))
    }

    #[async_backtrace::framed]
    async fn to_block(&self, snapshot: Arc<TableSnapshot>) -> Result<DataBlock> {
        let limit = self.limit.unwrap_or(usize::MAX);
        let len = std::cmp::min(snapshot.segments.len(), limit);

        let snapshot_id = snapshot.snapshot_id.simple().to_string().into_bytes();
        let timestamp = snapshot.timestamp.unwrap_or_default().timestamp_micros();
        let mut block_location = StringColumnBuilder::with_capacity(len, len);
        let mut block_size = NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, len);
        let mut file_size = NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, len);
        let mut row_count = NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, len);
        let mut bloom_filter_location = vec![];
        let mut bloom_filter_size =
            NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, len);

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.table.operator.clone(),
            self.table.schema(),
        );
        let segments = segments_io
            .read_segments(&snapshot.segments[..len], true)
            .await?;
        for segment in segments {
            let segment = segment?;
            segment.blocks.iter().for_each(|block| {
                let block = block.as_ref();
                block_location.put_slice(block.location.0.as_bytes());
                block_location.commit_row();
                block_size.push(NumberScalar::UInt64(block.block_size));
                file_size.push(NumberScalar::UInt64(block.file_size));
                row_count.push(NumberScalar::UInt64(block.row_count));
                bloom_filter_location.push(
                    block
                        .bloom_filter_index_location
                        .as_ref()
                        .map(|s| s.0.as_bytes().to_vec()),
                );
                bloom_filter_size.push(NumberScalar::UInt64(block.bloom_filter_index_size));
            });
        }

        Ok(DataBlock::new(
            vec![
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(snapshot_id.to_vec())),
                },
                BlockEntry {
                    data_type: DataType::Nullable(Box::new(DataType::Timestamp)),
                    value: Value::Scalar(Scalar::Timestamp(timestamp)),
                },
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Column(Column::String(block_location.build())),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::UInt64),
                    value: Value::Column(Column::Number(block_size.build())),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::UInt64),
                    value: Value::Column(Column::Number(file_size.build())),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::UInt64),
                    value: Value::Column(Column::Number(row_count.build())),
                },
                BlockEntry {
                    data_type: DataType::String.wrap_nullable(),
                    value: Value::Column(StringType::from_opt_data(bloom_filter_location)),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::UInt64),
                    value: Value::Column(Column::Number(bloom_filter_size.build())),
                },
            ],
            len,
        ))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp.wrap_nullable()),
            TableField::new("block_location", TableDataType::String),
            TableField::new("block_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("file_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "bloom_filter_location",
                TableDataType::String.wrap_nullable(),
            ),
            TableField::new(
                "bloom_filter_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ])
    }
}
