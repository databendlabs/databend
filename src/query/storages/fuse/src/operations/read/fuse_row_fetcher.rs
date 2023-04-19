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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_arrow::parquet::metadata::ColumnDescriptor;
use common_catalog::plan::compute_row_id_prefix;
use common_catalog::plan::split_row_id;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::AsyncTransform;
use common_pipeline_transforms::processors::transforms::AsyncTransformer;
use common_storage::ColumnNodes;
use opendal::Operator;
use storages_common_cache::LoadParams;

use super::native_rows_fetcher::NativeRowFetcher;
use super::parquet_rows_fetcher::ParquetRowFetcher;
use crate::io::BlockReader;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::FuseStorageFormat;
use crate::FuseTable;

#[async_backtrace::framed]
pub(super) async fn build_partitions_map(
    snapshot_loc: String,
    ver: u64,
    operator: Operator,
    table_schema: TableSchemaRef,
    projection: &Projection,
) -> Result<HashMap<u64, PartInfoPtr>> {
    let arrow_schema = table_schema.to_arrow();
    let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&table_schema));

    let snapshot = FuseTable::read_table_snapshot_impl(snapshot_loc, ver, operator.clone())
        .await?
        .ok_or_else(|| ErrorCode::Internal("No snapshot found"))?;
    let segment_id_map = snapshot.build_segment_id_map();
    let segment_reader = MetaReaders::segment_info_reader(operator, table_schema);
    let mut map = HashMap::new();

    for (location, seg_id) in segment_id_map.iter() {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.clone(),
                len_hint: None,
                ver,
                put_cache: true,
            })
            .await?;
        for (block_idx, block_meta) in segment_info.blocks.iter().enumerate() {
            let part_info =
                FuseTable::projection_part(block_meta, &None, &column_nodes, None, projection);
            let part_id = compute_row_id_prefix(*seg_id as u64, block_idx as u64);
            map.insert(part_id, part_info);
        }
    }

    Ok(map)
}

pub fn build_row_fetcher_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    row_id_col_offset: usize,
    source: &DataSourcePlan,
    projection: Projection,
) -> Result<()> {
    let table = ctx.build_table_from_source_plan(source)?;
    let fuse_table = table
        .as_any()
        .downcast_ref::<FuseTable>()
        .ok_or_else(|| ErrorCode::Internal("Row fetcher is only supported by Fuse engine"))?;
    let block_reader = fuse_table.create_block_reader(projection.clone(), false, ctx.clone())?;
    let table_schema = table.schema();

    pipeline.add_transform(|input, output| {
        Ok(match &fuse_table.storage_format {
            FuseStorageFormat::Native => {
                let mut column_leaves = Vec::with_capacity(block_reader.project_column_nodes.len());
                for column_node in &block_reader.project_column_nodes {
                    let leaves: Vec<ColumnDescriptor> = column_node
                        .leaf_indices
                        .iter()
                        .map(|i| block_reader.parquet_schema_descriptor.columns()[*i].clone())
                        .collect::<Vec<_>>();
                    column_leaves.push(leaves);
                }
                if block_reader.support_blocking_api() {
                    TransformRowsFetcher::create(
                        ctx.clone(),
                        input,
                        output,
                        row_id_col_offset,
                        table_schema.clone(),
                        projection.clone(),
                        NativeRowFetcher::<true>::create(block_reader.clone(), column_leaves),
                    )
                } else {
                    TransformRowsFetcher::create(
                        ctx.clone(),
                        input,
                        output,
                        row_id_col_offset,
                        table_schema.clone(),
                        projection.clone(),
                        NativeRowFetcher::<false>::create(block_reader.clone(), column_leaves),
                    )
                }
            }
            FuseStorageFormat::Parquet => {
                let buffer_size =
                    ctx.get_settings().get_parquet_uncompressed_buffer_size()? as usize;
                let read_settings = ReadSettings::from_ctx(&ctx)?;
                if block_reader.support_blocking_api() {
                    TransformRowsFetcher::create(
                        ctx.clone(),
                        input,
                        output,
                        row_id_col_offset,
                        table_schema.clone(),
                        projection.clone(),
                        ParquetRowFetcher::<true>::create(
                            block_reader.clone(),
                            read_settings,
                            buffer_size,
                        ),
                    )
                } else {
                    TransformRowsFetcher::create(
                        ctx.clone(),
                        input,
                        output,
                        row_id_col_offset,
                        table_schema.clone(),
                        projection.clone(),
                        ParquetRowFetcher::<false>::create(
                            block_reader.clone(),
                            read_settings,
                            buffer_size,
                        ),
                    )
                }
            }
        })
    })
}

#[async_trait::async_trait]
pub trait RowFetcher {
    async fn fetch(
        &self,
        part_map: &HashMap<u64, PartInfoPtr>,
        part_set: HashSet<u64>,
    ) -> Result<(Vec<DataBlock>, HashMap<u64, usize>)>;

    fn reader(&self) -> &BlockReader;
}

pub struct TransformRowsFetcher<F: RowFetcher> {
    ctx: Arc<dyn TableContext>,

    table_schema: TableSchemaRef,
    projection: Projection,

    row_id_col_offset: usize,

    part_map: HashMap<u64, PartInfoPtr>,

    fetcher: F,
}

#[async_trait::async_trait]
impl<F: RowFetcher + Send + Sync> AsyncTransform for TransformRowsFetcher<F> {
    const NAME: &'static str = "TransformNativeRowsFetcher";

    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        let (snapshot_loc, ver) = self
            .ctx
            .get_snapshot()
            .ok_or_else(|| ErrorCode::Internal("Snapshot location is not found"))?;
        self.part_map = build_partitions_map(
            snapshot_loc,
            ver,
            self.fetcher.reader().operator.clone(),
            self.table_schema.clone(),
            &self.projection,
        )
        .await?;
        Ok(())
    }

    // #[async_backtrace::framed]
    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let num_rows = data.num_rows();
        if num_rows == 0 {
            return Ok(data);
        }

        let row_id_column = data.columns()[self.row_id_col_offset]
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

        let (fetched_blocks, part_idx_map) = self.fetcher.fetch(&self.part_map, part_set).await?;

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

impl<F> TransformRowsFetcher<F>
where F: RowFetcher + Send + Sync + 'static
{
    fn create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        row_id_col_offset: usize,
        table_schema: TableSchemaRef,
        projection: Projection,
        fetcher: F,
    ) -> ProcessorPtr {
        ProcessorPtr::create(AsyncTransformer::create(input, output, Self {
            ctx,
            table_schema,
            projection,
            row_id_col_offset,
            part_map: HashMap::new(),
            fetcher,
        }))
    }
}
