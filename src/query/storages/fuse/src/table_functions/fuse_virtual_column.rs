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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_index::VirtualColumnNameIndex;
use databend_storages_common_index::VirtualColumnNode;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractBlockMeta;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::io::read::ColumnOrientedSegmentReader;
use crate::io::read::RowOrientedSegmentReader;
use crate::io::read::SegmentReader;
use crate::io::read::load_virtual_column_file_meta;
use crate::sessions::TableContext;
use crate::table_functions::TableMetaFuncTemplate;
use crate::table_functions::function_template::TableMetaFunc;

pub struct FuseVirtualColumn;
pub type FuseVirtualColumnFunc = TableMetaFuncTemplate<FuseVirtualColumn>;

#[async_trait::async_trait]
impl TableMetaFunc for FuseVirtualColumn {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("virtual_block_location", TableDataType::String),
            TableField::new(
                "virtual_block_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
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

impl FuseVirtualColumn {
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
        let mut virtual_block_location = StringColumnBuilder::with_capacity(len);
        let mut virtual_block_size = vec![];
        let mut row_count = vec![];

        let mut column_name = StringColumnBuilder::with_capacity(len);
        let mut column_type = StringColumnBuilder::with_capacity(len);
        let mut column_id = vec![];
        let mut block_offset = vec![];
        let mut bytes_compressed = vec![];

        let schema = tbl.schema();
        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), schema.clone());
        let source_column_names = build_source_column_name_map(schema.as_ref());

        let mut num_rows = 0;
        let chunk_size =
            std::cmp::min(ctx.get_settings().get_max_threads()? as usize * 4, len).max(1);

        let projection = HashSet::new();
        'FOR: for chunk in snapshot.segments.chunks(chunk_size) {
            let chunk_refs: Vec<&_> = chunk.iter().collect();
            let segments = segments_io
                .generic_read_compact_segments::<R>(&chunk_refs, true, &projection)
                .await?;
            for segment in segments {
                let segment = segment?;
                for block in segment.block_metas()? {
                    let Some(block_meta) = block.virtual_block_meta() else {
                        continue;
                    };
                    let location = block_meta.virtual_location.0;
                    let Ok(virtual_meta) =
                        load_virtual_column_file_meta(tbl.operator.clone(), &location).await
                    else {
                        continue;
                    };
                    let entries =
                        collect_virtual_column_entries(&virtual_meta, &source_column_names);

                    for entry in entries {
                        virtual_block_location.put_and_commit(location.clone());
                        virtual_block_size.push(block_meta.virtual_column_size);
                        row_count.push(entry.num_values);

                        column_name.put_and_commit(&entry.column_name);
                        column_type.put_and_commit(&entry.column_type);
                        column_id.push(entry.column_id);

                        block_offset.push(entry.offset);
                        bytes_compressed.push(entry.len);

                        num_rows += 1;

                        if num_rows >= limit {
                            break 'FOR;
                        }
                    }
                }
            }
        }

        Ok(DataBlock::new(
            vec![
                BlockEntry::new_const_column_arg::<StringType>(snapshot_id, num_rows),
                BlockEntry::new_const_column_arg::<TimestampType>(timestamp, num_rows),
                Column::String(virtual_block_location.build()).into(),
                UInt64Type::from_data(virtual_block_size).into(),
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

struct VirtualColumnEntry {
    column_name: String,
    column_type: String,
    column_id: u32,
    offset: u64,
    len: u64,
    num_values: u64,
}

fn collect_virtual_column_entries(
    virtual_meta: &VirtualColumnFileMeta,
    source_column_names: &HashMap<u32, String>,
) -> Vec<VirtualColumnEntry> {
    let mut entries = Vec::new();

    for (source_column_id, node) in &virtual_meta.virtual_column_nodes {
        let source_column_name = source_column_names
            .get(source_column_id)
            .cloned()
            .unwrap_or_else(|| source_column_id.to_string());
        let mut segments = Vec::new();
        collect_virtual_column_leaves(
            virtual_meta,
            &source_column_name,
            node,
            &mut segments,
            &mut entries,
        );
    }

    for (source_column_id, (key_meta, value_meta)) in &virtual_meta.shared_column_metas {
        let source_column_name = source_column_names
            .get(source_column_id)
            .cloned()
            .unwrap_or_else(|| source_column_id.to_string());
        entries.push(VirtualColumnEntry {
            column_name: format!(
                "{}.__shared_virtual_column_data__.entries.key",
                source_column_name
            ),
            column_type: key_meta.data_type.to_string(),
            column_id: key_meta.column_id,
            offset: key_meta.meta.offset,
            len: key_meta.meta.len,
            num_values: key_meta.meta.num_values,
        });
        entries.push(VirtualColumnEntry {
            column_name: format!(
                "{}.__shared_virtual_column_data__.entries.value",
                source_column_name
            ),
            column_type: value_meta.data_type.to_string(),
            column_id: value_meta.column_id,
            offset: value_meta.meta.offset,
            len: value_meta.meta.len,
            num_values: value_meta.meta.num_values,
        });
    }

    entries.sort_by_key(|entry| entry.column_id);
    entries
}

fn collect_virtual_column_leaves(
    virtual_meta: &VirtualColumnFileMeta,
    source_column_name: &str,
    node: &VirtualColumnNode,
    segments: &mut Vec<String>,
    entries: &mut Vec<VirtualColumnEntry>,
) {
    if let Some(VirtualColumnNameIndex::Column(leaf_index)) = node.leaf.as_ref() {
        if let Some(meta) = virtual_meta.column_metas.get(*leaf_index as usize) {
            let key_name = build_virtual_column_key_name(segments);
            let name = format!("{}{}", source_column_name, key_name);
            entries.push(VirtualColumnEntry {
                column_name: name,
                column_type: meta.data_type.to_string(),
                column_id: meta.column_id,
                offset: meta.meta.offset,
                len: meta.meta.len,
                num_values: meta.meta.num_values,
            });
        }
    }

    let mut children: Vec<(u32, &VirtualColumnNode)> = node
        .children
        .iter()
        .map(|(id, child)| (*id, child))
        .collect();
    children.sort_by_key(|(id, _)| *id);
    for (child_id, child_node) in children {
        let Some(segment) = virtual_meta.string_table.get(child_id as usize) else {
            continue;
        };
        segments.push(segment.clone());
        collect_virtual_column_leaves(
            virtual_meta,
            source_column_name,
            child_node,
            segments,
            entries,
        );
        segments.pop();
    }
}

fn build_source_column_name_map(schema: &TableSchema) -> HashMap<u32, String> {
    schema
        .fields()
        .iter()
        .map(|field| (field.column_id, field.name.clone()))
        .collect()
}

fn build_virtual_column_key_name(segments: &[String]) -> String {
    let mut name = String::new();
    for segment in segments {
        name.push('[');
        name.push('\'');
        name.push_str(segment);
        name.push('\'');
        name.push(']');
    }
    name
}
