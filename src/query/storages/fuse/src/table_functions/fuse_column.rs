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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractBlockMeta;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::io::read::ColumnOrientedSegmentReader;
use crate::io::read::RowOrientedSegmentReader;
use crate::io::read::SegmentReader;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::function_template::TableMetaFunc;
use crate::table_functions::TableMetaFuncTemplate;
use crate::FuseTable;

pub struct FuseColumn;
pub type FuseColumnFunc = TableMetaFuncTemplate<FuseColumn>;

#[async_trait::async_trait]
impl TableMetaFunc for FuseColumn {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("block_location", TableDataType::String),
            TableField::new("block_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("file_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("column_name", TableDataType::String),
            TableField::new("column_type", TableDataType::String),
            TableField::new("column_id", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new(
                "block_offset",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "bytes_compressed",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        match tbl.is_column_oriented() {
            true => {
                Self::apply_generic::<ColumnOrientedSegmentReader>(ctx, tbl, snapshot, limit).await
            }
            false => {
                Self::apply_generic::<RowOrientedSegmentReader>(ctx, tbl, snapshot, limit).await
            }
        }
    }
}

impl FuseColumn {
    async fn apply_generic<R: SegmentReader>(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let limit = limit.unwrap_or(usize::MAX);
        let len = std::cmp::min(snapshot.summary.block_count as usize, limit);

        let snapshot_id = snapshot.snapshot_id.simple().to_string();
        let timestamp = snapshot.timestamp.unwrap_or_default().timestamp_micros();
        let mut block_location = StringColumnBuilder::with_capacity(len);
        let mut block_size = vec![];
        let mut file_size = vec![];
        let mut row_count = vec![];

        let mut column_name = StringColumnBuilder::with_capacity(len);
        let mut column_type = StringColumnBuilder::with_capacity(len);
        let mut column_id = vec![];
        let mut block_offset = vec![];
        let mut bytes_compressed = vec![];

        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), tbl.schema());

        let mut num_rows = 0;
        let chunk_size =
            std::cmp::min(ctx.get_settings().get_max_threads()? as usize * 4, len).max(1);

        let schema = tbl.schema();
        let leaf_fields = schema.leaf_fields();
        let col_ids = schema.to_leaf_column_id_set();
        let projection = HashSet::new();

        'FOR: for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .generic_read_compact_segments::<R>(chunk, true, &projection)
                .await?;
            for segment in segments {
                let segment = segment?;
                for block in segment.block_metas()? {
                    for (id, column) in block.col_metas(&col_ids).iter() {
                        if let Some(f) = leaf_fields.iter().find(|f| f.column_id == *id) {
                            block_location.put_and_commit(block.location_path());
                            block_size.push(block.block_size());
                            file_size.push(block.file_size());
                            row_count.push(column.total_rows() as u64);

                            column_name.put_and_commit(&f.name);

                            column_type.put_and_commit(f.data_type.to_string());

                            column_id.push(*id);

                            let (offset, length) = column.offset_length();
                            block_offset.push(offset);
                            bytes_compressed.push(length);

                            num_rows += 1;

                            if num_rows >= limit {
                                break 'FOR;
                            }
                        }
                    }
                }
            }
        }

        Ok(DataBlock::new(
            vec![
                BlockEntry::new_const_column_arg::<StringType>(snapshot_id, num_rows),
                BlockEntry::new_const_column_arg::<TimestampType>(timestamp, num_rows),
                Column::String(block_location.build()).into(),
                UInt64Type::from_data(block_size).into(),
                UInt64Type::from_data(file_size).into(),
                UInt64Type::from_data(row_count).into(),
                Column::String(column_name.build()).into(),
                Column::String(column_type.build()).into(),
                UInt32Type::from_data(column_id).into(),
                UInt64Type::from_data(block_offset).into(),
                UInt64Type::from_data(bytes_compressed).into(),
            ],
            num_rows,
        ))
    }
}
