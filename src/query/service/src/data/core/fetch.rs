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
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_storage::ColumnNodes;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::BlockReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use futures::future::try_join_all;

use crate::data::core::RowRef;
use crate::data::core::TableTarget;
use crate::sessions::TableContextCachePolicy;

/// Reads columns of the rows an operation found.
pub struct RowFetcher {
    /// `None` when no column is read.
    reader: Option<Arc<BlockReader>>,
    projection: Projection,
    column_nodes: ColumnNodes,
    settings: ReadSettings,
    fields: Vec<DataField>,
}

impl RowFetcher {
    /// A fetcher of the table `columns`, in order.
    pub fn create(target: &TableTarget, columns: &[&str]) -> Result<Self> {
        let fuse_table: &FuseTable = target.fuse_table();
        let schema = fuse_table.schema();
        let indices = columns
            .iter()
            .map(|name| {
                schema.index_of(name).map_err(|_| {
                    ErrorCode::UnknownColumn(format!(
                        "table `{}` has no column `{name}`",
                        fuse_table.name()
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let fields = indices
            .iter()
            .map(|i| DataField::from(schema.field(*i)))
            .collect();
        let projection = Projection::Columns(indices);
        let ctx: Arc<dyn TableContext> = target.ctx.clone();
        let reader = (!columns.is_empty())
            .then(|| {
                fuse_table.create_block_reader(
                    ctx.clone(),
                    projection.clone(),
                    target.ctx.cache_policy(),
                )
            })
            .transpose()?;
        Ok(RowFetcher {
            reader,
            projection,
            column_nodes: ColumnNodes::new_from_schema(&schema.as_ref().into(), Some(&schema)),
            settings: ReadSettings::from_ctx(&ctx)?,
            fields,
        })
    }

    pub fn schema(&self) -> DataSchema {
        DataSchema::new(self.fields.clone())
    }

    /// The columns of `rows`, a row per `RowRef` in order. `blocks` are the blocks of the
    /// target, that `RowRef::block` refers to by position.
    pub async fn fetch(&self, blocks: &[Arc<BlockMeta>], rows: &[RowRef]) -> Result<DataBlock> {
        let Some(reader) = &self.reader else {
            return Ok(DataBlock::new(vec![], rows.len()));
        };
        if rows.is_empty() {
            return Ok(DataBlock::empty_with_schema(&self.schema()));
        }
        // The rows of each block, ascending, as the block is read.
        let mut rows_by_block: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
        for row in rows {
            rows_by_block.entry(row.block).or_default().push(row.row);
        }
        for rows in rows_by_block.values_mut() {
            rows.sort_unstable();
            rows.dedup();
        }
        let reads = rows_by_block
            .iter()
            .map(|(block, rows)| self.read_rows(reader, &blocks[*block], rows));
        let read_blocks = try_join_all(reads).await?;

        // Each row is a row of one block read, at its position among the rows read.
        let positions = rows_by_block
            .iter()
            .enumerate()
            .map(|(read, (block, read_rows))| (*block, (read, read_rows)))
            .collect::<BTreeMap<_, _>>();
        let indices = rows
            .iter()
            .map(|row| {
                let (read, read_rows) = positions[&row.block];
                let position = read_rows.binary_search(&row.row).expect("row was read");
                (read as u32, position as u32)
            })
            .collect::<Vec<_>>();
        Ok(DataBlock::take_blocks(&read_blocks, &indices))
    }

    /// The projected columns of `rows`, ascending distinct row numbers of `block`. Like
    /// the SQL row fetch, the whole columns are decoded, then the rows taken: decoded
    /// columns are cached (a row selection would bypass the cache) for the next search.
    async fn read_rows(
        &self,
        reader: &BlockReader,
        block: &BlockMeta,
        rows: &[u32],
    ) -> Result<DataBlock> {
        let num_rows = block.row_count as usize;
        if let Some(row) = rows.iter().find(|row| **row as usize >= num_rows) {
            return Err(ErrorCode::Internal(format!(
                "row {row} outside block {} of {num_rows} rows",
                block.location.0
            )));
        }
        let part =
            FuseTable::projection_part(block, &None, &self.column_nodes, None, &self.projection);
        let part = FuseBlockPartInfo::from_part(&part)?;
        let chunks = reader
            .read_columns_data_by_merge_io(
                &self.settings,
                &part.location,
                &part.columns_meta,
                &None,
            )
            .await?;
        let block = reader.deserialize_parquet_chunks(
            part.nums_rows,
            &part.columns_meta,
            chunks.columns_chunks()?,
            &part.compression,
            &part.location,
            None,
        )?;
        block.take(rows)
    }
}
