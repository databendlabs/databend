//  Copyright 2023 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_arrow::parquet::metadata::ColumnDescriptor;
use common_catalog::plan::split_row_id;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_pipeline_transforms::processors::transforms::AsyncTransform;
use opendal::Operator;

use super::fuse_row_fetcher::build_partitions_map;
use super::native_data_source::DataChunks;
use super::native_data_source_deserializer::NativeDeserializeDataTransform;
use crate::io::BlockReader;

pub struct TransformNativeRowsFetcher<const BLOCKING_IO: bool> {
    ctx: Arc<dyn TableContext>,

    table_schema: TableSchemaRef,
    projection: Projection,
    column_leaves: Vec<Vec<ColumnDescriptor>>,

    row_id_column_idx: usize,

    operator: Operator,
    reader: Arc<BlockReader>,
}

#[async_trait::async_trait]
impl<const BLOCKING_IO: bool> AsyncTransform for TransformNativeRowsFetcher<BLOCKING_IO> {
    const NAME: &'static str = "TransformNativeRowsFetcher";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let num_rows = data.num_rows();
        if num_rows == 0 {
            return Ok(data);
        }

        let row_id_column = data.columns()[self.row_id_column_idx]
            .value
            .convert_to_full_column(&DataType::Number(NumberDataType::UInt64), num_rows)
            .into_number()
            .unwrap()
            .into_u_int64()
            .unwrap();

        let mut part_set = HashSet::new();
        let mut row_set = Vec::with_capacity(num_rows);
        for row_id in row_id_column.iter() {
            let (part_id, idx) = split_row_id(*row_id);
            part_set.insert(part_id);
            row_set.push((part_id, idx));
        }

        let (snapshot_loc, ver) = self
            .ctx
            .get_snapshot()
            .ok_or_else(|| ErrorCode::Internal("Snapshot location is not found"))?;
        let part_map = build_partitions_map(
            snapshot_loc,
            ver,
            self.operator.clone(),
            self.table_schema.clone(),
            &self.projection,
        )
        .await?;

        let mut chunks = Vec::with_capacity(part_set.len());
        if BLOCKING_IO {
            for part_id in part_set.into_iter() {
                let part = part_map[&part_id].clone();
                let reader = self.reader.clone();
                let chunk = reader.sync_read_native_columns_data(part)?;
                chunks.push((part_id, chunk));
            }
        } else {
            for part_id in part_set.into_iter() {
                let part = part_map[&part_id].clone();
                let reader = self.reader.clone();
                let chunk = reader.async_read_native_columns_data(part).await?;
                chunks.push((part_id, chunk));
            }
        }
        let mut part_idx_map = HashMap::with_capacity(chunks.len());
        let fetched_blocks = chunks
            .into_iter()
            .enumerate()
            .map(|(idx, (part, chunk))| {
                part_idx_map.insert(part, idx);
                self.build_block(chunk)
            })
            .collect::<Result<Vec<_>>>()?;

        let indices = row_set
            .iter()
            .map(|(part_id, row_idx)| {
                let block_idx = part_idx_map[part_id];
                (block_idx, *row_idx as usize, 1_usize)
            })
            .collect::<Vec<_>>();

        let needed_block = DataBlock::take_blocks(&fetched_blocks, &indices, num_rows);
        for col in needed_block.columns().iter() {
            data.add_column(col.clone());
        }

        Ok(data)
    }
}

impl<const BLOCKING_IO: bool> TransformNativeRowsFetcher<BLOCKING_IO> {
    fn build_block(&self, mut chunks: DataChunks) -> Result<DataBlock> {
        let mut array_iters = BTreeMap::new();

        for (index, column_node) in self.reader.project_column_nodes.iter().enumerate() {
            let readers = chunks.remove(&index).unwrap();
            if !readers.is_empty() {
                let leaves = self.column_leaves.get(index).unwrap().clone();
                let array_iter =
                    NativeDeserializeDataTransform::build_array_iter(column_node, leaves, readers)?;
                array_iters.insert(index, array_iter);
            }
        }

        let mut arrays = Vec::with_capacity(array_iters.len());
        for (index, array_iter) in array_iters.iter_mut() {
            let array = array_iter.next().unwrap()?;
            arrays.push((*index, array));
        }

        self.reader.build_block(arrays, None)
    }
}
