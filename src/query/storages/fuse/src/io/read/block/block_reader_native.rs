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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_metrics::storage::*;
use databend_common_native::read::reader::read_meta_async;
use databend_common_native::read::reader::NativeReader;
use databend_common_native::read::NativeReadBuf;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::ColumnMeta;
use opendal::Operator;

use crate::fuse_part::FuseBlockPartInfo;
use crate::io::BlockReader;

// Native storage format

pub trait NativeReaderExt: NativeReadBuf + std::io::Seek + Send + Sync {}

impl<T: NativeReadBuf + std::io::Seek + Send + Sync> NativeReaderExt for T {}

pub type Reader = Box<dyn NativeReaderExt>;

pub type NativeSourceData = BTreeMap<usize, Vec<NativeReader<Reader>>>;

impl BlockReader {
    #[async_backtrace::framed]
    pub async fn async_read_native_columns_data(
        &self,
        part: &PartInfoPtr,
        ctx: &Arc<dyn TableContext>,
        ignore_column_ids: &Option<HashSet<ColumnId>>,
    ) -> Result<NativeSourceData> {
        // Perf
        {
            metrics_inc_remote_io_read_parts(1);
        }

        let part = FuseBlockPartInfo::from_part(part)?;
        let settings = ReadSettings::from_ctx(ctx)?;
        let read_res = self
            .read_columns_data_by_merge_io(
                &settings,
                &part.location,
                &part.columns_meta,
                ignore_column_ids,
            )
            .await?;

        let column_buffers = read_res.column_buffers()?;
        let mut results = BTreeMap::new();
        for (index, column_node) in self.project_column_nodes.iter().enumerate() {
            if let Some(ignore_column_ids) = ignore_column_ids {
                if column_node.leaf_column_ids.len() == 1
                    && ignore_column_ids.contains(&column_node.leaf_column_ids[0])
                {
                    continue;
                }
            }

            let readers = column_node
                .leaf_column_ids
                .iter()
                .map(|column_id| {
                    let native_meta = part
                        .columns_meta
                        .get(column_id)
                        .unwrap()
                        .as_native()
                        .unwrap();
                    let data = column_buffers.get(column_id).unwrap();
                    let reader: Reader = Box::new(std::io::Cursor::new(data.clone()));
                    NativeReader::new(reader, native_meta.pages.clone(), vec![])
                })
                .collect();

            results.insert(index, readers);
        }
        Ok(results)
    }

    #[async_backtrace::framed]
    pub async fn read_native_columns_data(
        op: Operator,
        path: &str,
        index: usize,
        metas: Vec<ColumnMeta>,
        range: Option<&Range<usize>>,
    ) -> Result<(usize, Vec<NativeReader<Reader>>)> {
        let mut native_readers = Vec::with_capacity(metas.len());
        for meta in metas {
            let mut native_meta = meta.as_native().unwrap().clone();
            if let Some(range) = &range {
                native_meta = native_meta.slice(range.start, range.end);
            }

            let (offset, length) = (
                native_meta.offset,
                native_meta.pages.iter().map(|p| p.length).sum::<u64>(),
            );

            let reader = op.read_with(path).range(offset..offset + length).await?;
            let reader: Reader = Box::new(std::io::Cursor::new(reader.to_bytes()));

            let native_reader = NativeReader::new(reader, native_meta.pages.clone(), vec![]);
            native_readers.push(native_reader);
        }

        Ok((index, native_readers))
    }

    #[inline(always)]
    pub fn fill_missing_native_column_values(
        &self,
        data_block: DataBlock,
        data_block_column_ids: &HashSet<ColumnId>,
    ) -> Result<DataBlock> {
        DataBlock::create_with_default_value_and_block(
            &self.projected_schema,
            &data_block,
            data_block_column_ids,
            &self.default_vals,
        )
    }

    pub fn build_block(
        &self,
        columns: &[(usize, Column)],
        default_val_indices: Option<HashSet<usize>>,
    ) -> Result<DataBlock> {
        let mut nums_rows: Option<usize> = None;
        let mut entries = Vec::with_capacity(self.project_column_nodes.len());
        for (index, _) in self.project_column_nodes.iter().enumerate() {
            if let Some(column) = columns.iter().find(|c| c.0 == index).map(|c| c.1.clone()) {
                match nums_rows {
                    Some(rows) => {
                        debug_assert_eq!(rows, column.len(), "Column lengths are not equal")
                    }
                    None => nums_rows = Some(column.len()),
                }
                entries.push(column.into());
            } else if let Some(ref default_val_indices) = default_val_indices {
                if default_val_indices.contains(&index) {
                    let data_type = self.projected_schema.field(index).data_type().into();
                    let default_val = self.default_vals[index].clone();
                    entries.push(BlockEntry::new_const_column(data_type, default_val, 0));
                }
            }
        }
        let nums_rows = nums_rows.unwrap_or_default();
        if nums_rows != 0 {
            for entry in &mut entries {
                if let Some((_, _, n)) = entry.as_const_mut() {
                    *n = nums_rows;
                }
            }
        }
        Ok(DataBlock::new(entries, nums_rows))
    }

    #[async_backtrace::framed]
    pub async fn async_read_native_schema(
        operator: &Operator,
        loc: &str,
    ) -> Option<(Vec<ColumnMeta>, ArrowSchema)> {
        let stat = operator.stat(loc).await.ok()?;
        let reader = operator.reader(loc).await.ok()?;

        let (native_metas, schema) =
            read_meta_async(reader.clone(), stat.content_length() as usize)
                .await
                .ok()?;
        let metas = native_metas
            .into_iter()
            .map(ColumnMeta::Native)
            .collect::<Vec<ColumnMeta>>();
        let schema = DataSchema::from(&schema);
        Some((metas, ArrowSchema::from(&schema)))
    }
}
