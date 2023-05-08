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

use common_arrow::parquet::metadata::ColumnDescriptor;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Projection;
use common_catalog::table::Table;
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
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::SegmentInfo;

use super::native_rows_fetcher::NativeRowsFetcher;
use super::parquet_rows_fetcher::ParquetRowsFetcher;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::FuseStorageFormat;
use crate::FuseTable;

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
        .ok_or_else(|| ErrorCode::Internal("Row fetcher is only supported by Fuse engine"))?
        .to_owned();
    let fuse_table = Arc::new(fuse_table);

    let block_reader = fuse_table.create_block_reader(projection.clone(), false, ctx.clone())?;

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
                        input,
                        output,
                        fuse_table.clone(),
                        row_id_col_offset,
                        projection.clone(),
                        NativeRowsFetcher::<true>::create(block_reader.clone(), column_leaves),
                    )
                } else {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        fuse_table.clone(),
                        row_id_col_offset,
                        projection.clone(),
                        NativeRowsFetcher::<false>::create(block_reader.clone(), column_leaves),
                    )
                }
            }
            FuseStorageFormat::Parquet => {
                let buffer_size =
                    ctx.get_settings().get_parquet_uncompressed_buffer_size()? as usize;
                let read_settings = ReadSettings::from_ctx(&ctx)?;
                if block_reader.support_blocking_api() {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        fuse_table.clone(),
                        row_id_col_offset,
                        projection.clone(),
                        ParquetRowsFetcher::<true>::create(
                            block_reader.clone(),
                            read_settings,
                            buffer_size,
                        ),
                    )
                } else {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        fuse_table.clone(),
                        row_id_col_offset,
                        projection.clone(),
                        ParquetRowsFetcher::<false>::create(
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
pub trait RowsFetcher {
    async fn fetch(&self, row_ids: &[u64]) -> Result<DataBlock>;

    /// `segments` contains seg_id and segment info
    fn set_metas(
        &mut self,
        segments: Vec<(u64, Arc<SegmentInfo>)>,
        table_schema: TableSchemaRef,
        projection: &Projection,
    );
}

pub struct TransformRowsFetcher<F: RowsFetcher> {
    table: Arc<FuseTable>,
    projection: Projection,

    row_id_col_offset: usize,
    fetcher: F,
}

#[async_trait::async_trait]
impl<F> AsyncTransform for TransformRowsFetcher<F>
where F: RowsFetcher + Send + Sync + 'static
{
    const NAME: &'static str = "TransformRowsFetcher";

    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        let table_schema = self.table.schema();

        let snapshot = self
            .table
            .read_table_snapshot()
            .await?
            .ok_or_else(|| ErrorCode::Internal("No snapshot found"))?;
        let segment_id_map = snapshot.build_segment_id_map();
        let compact_segment_reader =
            MetaReaders::segment_info_reader(self.table.operator.clone(), table_schema.clone());
        let mut segments: Vec<(u64, Arc<SegmentInfo>)> =
            Vec::with_capacity(snapshot.segments.len());
        for ((location, ver), seg_id) in segment_id_map.into_iter() {
            let compact_segment_info = compact_segment_reader
                .read(&LoadParams {
                    location,
                    len_hint: None,
                    ver,
                    put_cache: false,
                })
                .await?;
            segments.push((
                seg_id as u64,
                Arc::new(compact_segment_info.as_ref().try_into()?),
            ));
        }

        self.fetcher
            .set_metas(segments, table_schema, &self.projection);
        Ok(())
    }

    #[async_backtrace::framed]
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

        let fetched_block = self.fetcher.fetch(&row_id_column).await?;

        for col in fetched_block.columns().iter() {
            data.add_column(col.clone());
        }

        Ok(data)
    }
}

impl<F> TransformRowsFetcher<F>
where F: RowsFetcher + Send + Sync + 'static
{
    fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: Arc<FuseTable>,
        row_id_col_offset: usize,
        projection: Projection,
        fetcher: F,
    ) -> ProcessorPtr {
        ProcessorPtr::create(AsyncTransformer::create(input, output, Self {
            table,
            projection,
            row_id_col_offset,
            fetcher,
        }))
    }
}
